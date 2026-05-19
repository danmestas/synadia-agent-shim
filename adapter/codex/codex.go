// Package codex bridges the orch-agent-shim to the codex CLI.
// It does three things:
//
//  1. Tails the session's rollout JSONL — codex writes these under
//     ~/.codex/sessions/<YYYY>/<MM>/<DD>/rollout-<ts>-<uuid>.jsonl — and
//     emits a Synadia response chunk per assistant message line.
//  2. Watches the orch stop marker file the existing codex-hooks script writes
//     (~/.cache/orch-stop/<pane>.event) so the shim terminates active streams
//     when codex finishes a turn.
//  3. Closes the codex Notification gap: codex exposes no mid-turn attention
//     event, so the adapter emits synthetic §7 query chunks whenever the pane
//     buffer has been idle for ~5s AND its visible content matches a TUI prompt
//     pattern.
//  4. Injects inbound prompts into the pane via `tmux send-keys`.
//
// Watcher lifetime is decoupled from prompt lifetime — callers SHOULD invoke
// Start(shimCtx) once at boot. The shim's event pump reads Events() for the
// lifetime of the process.
package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/danmestas/synadia-agent-shim/shim"
)

// Adapter is the codex implementation of shim.Adapter. It is constructed once
// per pane and re-used across prompts.
type Adapter struct {
	// Pane is the raw tmux pane id (e.g. "%37"). Used to derive the stop
	// marker file path and as the send-keys target.
	Pane string

	// StopMarkerDir overrides ~/.cache/orch-stop/. Tests use this to point at
	// a tempdir.
	StopMarkerDir string

	// CodexSessionsDir overrides ~/.codex/sessions/. Tests use this.
	CodexSessionsDir string

	// SessionID, when non-empty, pins the transcript tailer to the
	// rollout JSONL whose basename matches `rollout-*-<SessionID>.jsonl`.
	// Codex names rollouts `rollout-<ts>-<uuid>.jsonl` under
	// `<sessionsDir>/<YYYY>/<MM>/<DD>/`; only the trailing UUID is
	// operator-knowable ahead of time, so we walk the tree once at
	// startup to recover the date bucket. Pinning disables the
	// latest-mtime scan (issue #11): codex's date-bucketed discovery is
	// not cwd-scoped, so ANY codex session on the host with a more
	// recent mtime would otherwise win the race. When empty, the
	// legacy findLatestRollout behaviour is preserved for backwards
	// compatibility.
	SessionID string

	// SendKeys is the function invoked to deliver a prompt to the pane.
	// Default is realSendKeys. Tests substitute a recorder.
	SendKeys SendKeysFunc

	// CapturePaneFn is the function invoked to read the pane's visible buffer.
	// Default is realCapturePane. Tests substitute a stub.
	CapturePaneFn CapturePaneFunc

	// idleThreshold is the pane-quiescence window before emitting a synthetic
	// query chunk. Defaults to 5s; tests override to a much smaller value.
	idleThreshold time.Duration

	// events is the channel the shim drains.
	events chan shim.Chunk

	// Start binds watchers to a shim-lifetime context once. Idempotent.
	startOnce sync.Once
	startErr  error
	started   atomic.Bool

	// Close shuts down watchers and closes the events channel exactly once.
	closeOnce sync.Once
	stopCh    chan struct{}
}

// SendKeysFunc is the seam that lets tests replace the tmux invocation.
type SendKeysFunc func(pane, text string) error

// CapturePaneFunc captures the visible content of a tmux pane.
type CapturePaneFunc func(pane string) (string, error)

// New constructs an Adapter with reasonable defaults.
func New(pane string) *Adapter {
	return &Adapter{
		Pane:          pane,
		SendKeys:      realSendKeys,
		CapturePaneFn: realCapturePane,
		idleThreshold: 5 * time.Second,
		events:        make(chan shim.Chunk, 64),
		stopCh:        make(chan struct{}),
	}
}

// Events returns the shim-facing chunk channel. The channel is closed when
// Close() is called.
func (a *Adapter) Events() <-chan shim.Chunk { return a.events }

// Start binds the background watchers (transcript tail, stop-marker watch,
// idle-query loop) to ctx. Idempotent — repeated calls return the original
// error without restarting anything.
//
// Callers SHOULD pass the shim's lifetime context here, NOT a per-prompt
// context. The watchers run until either ctx is cancelled or Close() is
// invoked.
func (a *Adapter) Start(ctx context.Context) error {
	a.startOnce.Do(func() {
		a.startErr = a.startWatchers(ctx)
		if a.startErr == nil {
			a.started.Store(true)
		}
	})
	return a.startErr
}

// Close shuts down background watchers (via stopCh) and closes the events
// channel. Idempotent — guarded by closeOnce so it's safe to call from
// multiple goroutines.
func (a *Adapter) Close() error {
	a.closeOnce.Do(func() {
		close(a.stopCh)
		close(a.events)
	})
	return nil
}

// OnPrompt delivers text to the codex TUI via tmux send-keys. If Start
// hasn't been called yet, OnPrompt calls it with ctx as a safety net —
// but production callers should call Start(shimCtx) at boot.
//
// OnPrompt does not block on the agent's response; chunks flow back through
// Events().
func (a *Adapter) OnPrompt(ctx context.Context, text string) error {
	if !a.started.Load() {
		if err := a.Start(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.SendKeys(a.Pane, text)
}

// startWatchers kicks off the transcript tailer, stop-marker watcher, and the
// idle-query loop. Errors are surfaced via startErr.
func (a *Adapter) startWatchers(ctx context.Context) error {
	if err := a.ensureStopDir(); err != nil {
		return err
	}

	// fsnotify watcher for the stop marker directory.
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("codex: fsnotify: %w", err)
	}
	if err := w.Add(a.stopDir()); err != nil {
		_ = w.Close()
		return fmt.Errorf("codex: watch stop dir: %w", err)
	}

	go a.stopMarkerLoop(ctx, w)
	go a.transcriptLoop(ctx)
	go a.idleQueryLoop(ctx)
	return nil
}

func (a *Adapter) ensureStopDir() error {
	if err := os.MkdirAll(a.stopDir(), 0o755); err != nil {
		return fmt.Errorf("codex: mkdir %s: %w", a.stopDir(), err)
	}
	return nil
}

func (a *Adapter) stopDir() string {
	if a.StopMarkerDir != "" {
		return a.StopMarkerDir
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".cache", "orch-stop")
}

func (a *Adapter) stopMarker() string {
	return filepath.Join(a.stopDir(), a.Pane+".event")
}

func (a *Adapter) sessionsDir() string {
	if a.CodexSessionsDir != "" {
		return a.CodexSessionsDir
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".codex", "sessions")
}

// stopMarkerLoop reacts to fsnotify CREATE / WRITE events on the stop marker
// directory. The codex Stop hook atomically writes the marker via
// tmpfile-then-rename; a CREATE event on the destination path is what we see.
//
// Stop marker → emit Terminator chunk to close the active stream.
//
// Note: there is intentionally NO notify-marker path here (compare cc.go's
// markerLoop, which watches both stop and notify dirs). Codex exposes no
// native Notification event — that gap is closed by idleQueryLoop, which
// emits synthetic §7 query chunks when the pane buffer is idle with a
// prompt visible. Don't add a notify-marker watch here without first
// reconciling with idleQueryLoop or you'll get duplicate query chunks.
func (a *Adapter) stopMarkerLoop(ctx context.Context, w *fsnotify.Watcher) {
	defer func() {
		if err := w.Close(); err != nil {
			log.Printf("codex: stop-marker watcher close: %v", err)
		}
	}()
	stopPath := a.stopMarker()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			if ev.Op&(fsnotify.Create|fsnotify.Write) == 0 {
				continue
			}
			if ev.Name == stopPath {
				a.emit(shim.NewTerminatorChunk())
			}
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
			// transient fsnotify errors — drop and continue.
		}
	}
}

// transcriptLoop hunts for the codex rollout JSONL and tails it.
// Codex writes rollouts at:
//
//	~/.codex/sessions/<YYYY>/<MM>/<DD>/rollout-<ts>-<uuid>.jsonl
//
// We poll for the newest file across all date buckets every 250ms.
// Once a candidate file is found we tail it line-by-line, parse JSON,
// and emit response chunks for assistant message lines.
//
// Cost discipline: findLatestRollout walks 3 levels of date-bucketed
// directories, which is cheap but not free. We only rescan for a newer
// session when the currently-watched file has been quiet for a while
// (rolloverIdleScan) — an actively producing rollout is by definition
// the latest one, so re-scanning during an active turn is wasted work.
func (a *Adapter) transcriptLoop(ctx context.Context) {
	const rolloverIdleScan = 10 * time.Second
	// discover picks the JSONL to tail. When SessionID is set we walk
	// the date-bucketed tree once per poll for a basename matching
	// `rollout-*-<SessionID>.jsonl`. When empty, fall back to the
	// legacy newest-mtime scan.
	discover := func() string {
		if a.SessionID == "" {
			return findLatestRollout(a.sessionsDir())
		}
		return findRolloutBySessionID(a.sessionsDir(), a.SessionID)
	}
	var (
		watchedFile *os.File
		watchedPath string
		offset      int64
		lastActive  time.Time
	)
	defer func() {
		if watchedFile != nil {
			_ = watchedFile.Close()
		}
	}()
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		case <-t.C:
		}

		if watchedFile == nil {
			candidate := discover()
			if candidate == "" {
				continue
			}
			f, err := os.Open(candidate)
			if err != nil {
				continue
			}
			watchedFile = f
			watchedPath = candidate
			offset = 0
			lastActive = time.Now()
		}

		if _, err := watchedFile.Seek(offset, 0); err != nil {
			_ = watchedFile.Close()
			watchedFile = nil
			continue
		}
		sc := bufio.NewScanner(watchedFile)
		// codex rollout lines can be large; bump the token cap to 8MB.
		sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		// scanCompleteLines refuses to tokenize a trailing partial at
		// EOF and offset advances only by complete-line bytes (#13).
		// The default ScanLines splitter would emit the partial as a
		// final token, advancing the OS file pointer past it; the next
		// poll would seek past the partial and silently drop the line
		// when codex finishes flushing it.
		sc.Split(scanCompleteLines)
		var consumed int64
		sawBytes := false
		for sc.Scan() {
			line := sc.Bytes()
			consumed += int64(len(line)) + 1
			if len(line) == 0 {
				continue
			}
			sawBytes = true
			a.emitFromRolloutLine(line)
		}
		offset += consumed
		if sawBytes {
			lastActive = time.Now()
		}

		// Session-pinned mode: never rescan for a different file. Legacy
		// mode: rescan for a newer session ONLY when the current file has
		// been idle for rolloverIdleScan. An actively-producing rollout
		// is the latest by definition, so scanning during a turn is
		// wasted work.
		if a.SessionID == "" && time.Since(lastActive) >= rolloverIdleScan {
			newest := findLatestRollout(a.sessionsDir())
			if newest != "" && newest != watchedPath {
				_ = watchedFile.Close()
				watchedFile = nil
			} else {
				// Reset the clock so we don't rescan every tick once idle.
				lastActive = time.Now()
			}
		}
	}
}

// idleQueryLoop polls the pane's visible buffer at idleThreshold intervals.
// When the buffer has been stable for idleThreshold AND contains a TUI prompt
// pattern, it emits a synthetic §7 query chunk to close the Notification gap.
//
// The loop is bounded: it only fires when both conditions hold. A pane that
// is actively generating output will never trigger this path.
func (a *Adapter) idleQueryLoop(ctx context.Context) {
	threshold := a.idleThreshold
	if threshold <= 0 {
		threshold = 5 * time.Second
	}
	// Poll at half the threshold so we don't miss the idle window.
	pollInterval := threshold / 2
	if pollInterval < 100*time.Millisecond {
		pollInterval = 100 * time.Millisecond
	}

	var (
		prevHash  uint64
		hasPrev   bool
		idleSince time.Time
		emitted   bool // guard: only one query chunk per idle window
	)

	// Reuse a single hasher across ticks to avoid per-tick allocations.
	hasher := fnv.New64a()

	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		case <-t.C:
		}

		content, err := a.CapturePaneFn(a.Pane)
		if err != nil {
			// pane gone or tmux not available — reset and wait.
			prevHash = 0
			hasPrev = false
			idleSince = time.Time{}
			emitted = false
			continue
		}

		h := hashTail(hasher, content, idleHashTailBytes)
		if !hasPrev || h != prevHash {
			// buffer changed (or first sample): reset idle clock.
			prevHash = h
			hasPrev = true
			idleSince = time.Now()
			emitted = false
			continue
		}

		// Buffer is stable.
		if idleSince.IsZero() {
			idleSince = time.Now()
			continue
		}
		if time.Since(idleSince) < threshold {
			continue
		}
		if emitted {
			// Already fired for this idle window; wait for next change.
			continue
		}
		if !hasPromptPattern(content) {
			continue
		}

		// Pane is idle + shows a prompt → synthetic query chunk.
		id := fmt.Sprintf("codex-idle-%d", time.Now().UnixNano())
		text := extractPromptText(content)
		a.emit(shim.NewQueryChunk(id, "", text))
		emitted = true
	}
}

// idleHashTailBytes is how many trailing bytes of the pane buffer we hash
// to detect change. Prompt patterns only appear near the bottom of the
// visible buffer, so hashing the full pane (often 20–80KB) per tick wasted
// arena pressure that drifted RSS upward over long runs. 4KB comfortably
// covers a wrapped multi-line prompt and is roughly free to feed FNV.
const idleHashTailBytes = 4096

// hashTail returns the FNV-64a hash of the last n bytes of s. The hasher is
// reset and reused across calls so no per-call allocation occurs (FNV-64a
// state is fixed-size). io.WriteString avoids the []byte(s) copy that
// crypto/sha256 required, which was the dominant per-tick allocation.
func hashTail(h io.Writer, s string, n int) uint64 {
	if r, ok := h.(interface{ Reset() }); ok {
		r.Reset()
	}
	if len(s) > n {
		s = s[len(s)-n:]
	}
	_, _ = io.WriteString(h, s)
	if sum, ok := h.(interface{ Sum64() uint64 }); ok {
		return sum.Sum64()
	}
	return 0
}

// promptPatterns are simple string markers that appear in the codex TUI
// when it is waiting for user input. Checked with strings.Contains so
// partial matches across wrapped lines work correctly.
var promptPatterns = []string{
	"❯",       // codex primary prompt glyph (U+276F heavy right ornament)
	"›",       // codex compact prompt glyph (U+203A single right-pointing angle)
	"> ",      // fallback generic shell-style prompt
	"[y/n]",   // codex approval prompt
	"(y/n)",   // variant approval prompt
	"You: ",   // codex conversation prompt label
	"[Enter]", // codex "press enter" indicator
}

// hasPromptPattern reports whether any known prompt marker appears in the
// pane content.
func hasPromptPattern(content string) bool {
	for _, p := range promptPatterns {
		if strings.Contains(content, p) {
			return true
		}
	}
	return false
}

// extractPromptText returns the last non-empty line of the pane content as a
// best-effort prompt text. This is what the caller will see in the §7 query
// chunk's prompt field.
//
// Best-effort: the returned text may include trailing agent output, not just
// the prompt glyph itself. Chunk consumers should treat this as a HINT that
// the agent is waiting for input, not as a verbatim prompt string.
func extractPromptText(content string) string {
	lines := strings.Split(strings.TrimRight(content, "\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if t := strings.TrimSpace(lines[i]); t != "" {
			return t
		}
	}
	return content
}

// emit publishes a chunk to the shim, or returns silently if Close has fired.
// Non-blocking guard on stopCh prevents send to a closed channel.
func (a *Adapter) emit(c shim.Chunk) {
	select {
	case <-a.stopCh:
		return
	default:
	}
	select {
	case a.events <- c:
	case <-a.stopCh:
	}
}

// ---------------------------------------------------------------------------
// Rollout JSONL parsing
// ---------------------------------------------------------------------------

// rolloutLine is the subset of the codex rollout JSONL shape we care about.
// Codex writes lines with varying schemas; we only act on lines where `role`
// is "assistant" and the content contains text. Unknown fields are silently
// dropped by encoding/json.
//
// Codex rollout schema (empirically verified):
//
//	{"type":"message","role":"assistant","content":[{"type":"output_text","text":"..."}]}
//	{"type":"message","role":"user","content":[{"type":"input_text","text":"..."}]}
//	{"type":"function_call","name":"...","arguments":"..."}
//	{"type":"function_call_output","output":"..."}
//	{"type":"reasoning","summary":[{"type":"summary_text","text":"..."}]}
type rolloutLine struct {
	Type    string         `json:"type"`
	Role    string         `json:"role"`
	Content []rolloutBlock `json:"content"`
	// reasoning lines
	Summary []rolloutBlock `json:"summary"`
	// function_call lines
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type rolloutBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// emitFromRolloutLine parses one JSONL line and emits 0+ chunks.
// We surface:
//   - assistant message text → response chunk
//   - reasoning summary text → thinking chunk
//   - function_call lines → tool_use chunk
func (a *Adapter) emitFromRolloutLine(line []byte) {
	var e rolloutLine
	if err := json.Unmarshal(line, &e); err != nil {
		// Defensive: malformed line — drop silently.
		return
	}

	switch e.Type {
	case "message":
		if e.Role != "assistant" {
			return
		}
		for _, b := range e.Content {
			if b.Text == "" {
				continue
			}
			// codex uses "output_text" for assistant text blocks.
			if b.Type == "output_text" || b.Type == "text" {
				a.emit(shim.NewResponseChunk(b.Text))
			}
		}

	case "reasoning":
		for _, b := range e.Summary {
			if b.Text == "" {
				continue
			}
			if b.Type == "summary_text" || b.Type == "text" {
				a.emit(shim.Chunk{Type: shim.ChunkThinking, Data: b.Text})
			}
		}

	case "function_call":
		if e.Name == "" {
			return
		}
		a.emit(shim.Chunk{Type: shim.ChunkToolUse, Data: map[string]any{
			"name":      e.Name,
			"arguments": e.Arguments,
		}})
	}
}

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

// scanCompleteLines is a bufio.SplitFunc that emits only complete
// newline-terminated lines. Unlike bufio.ScanLines, it does NOT return
// a trailing partial line at EOF — transcriptLoop relies on this to
// keep its byte-offset counter anchored to the last complete-line
// boundary across polls (see #13).
func scanCompleteLines(data []byte, _ bool) (advance int, token []byte, err error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i], nil
	}
	return 0, nil, nil
}

// findRolloutBySessionID walks the codex date-bucketed sessions tree for
// a rollout JSONL whose basename matches `rollout-*-<sessionID>.jsonl`.
// Returns "" when no match exists yet (the codex TUI may not have
// written its first rollout line at shim startup). When multiple files
// match — which would be a codex-side bug — the most recently modified
// one wins to favour the live session.
func findRolloutBySessionID(sessionsDir, sessionID string) string {
	if sessionID == "" {
		return ""
	}
	suffix := "-" + sessionID + ".jsonl"
	var bestPath string
	var bestMTime time.Time
	years, err := os.ReadDir(sessionsDir)
	if err != nil {
		return ""
	}
	for _, year := range years {
		if !year.IsDir() {
			continue
		}
		yearPath := filepath.Join(sessionsDir, year.Name())
		months, err := os.ReadDir(yearPath)
		if err != nil {
			continue
		}
		for _, month := range months {
			if !month.IsDir() {
				continue
			}
			monthPath := filepath.Join(yearPath, month.Name())
			days, err := os.ReadDir(monthPath)
			if err != nil {
				continue
			}
			for _, day := range days {
				if !day.IsDir() {
					continue
				}
				dayPath := filepath.Join(monthPath, day.Name())
				entries, err := os.ReadDir(dayPath)
				if err != nil {
					continue
				}
				for _, e := range entries {
					if e.IsDir() {
						continue
					}
					name := e.Name()
					if !strings.HasPrefix(name, "rollout-") || !strings.HasSuffix(name, suffix) {
						continue
					}
					info, err := e.Info()
					if err != nil {
						continue
					}
					if info.ModTime().After(bestMTime) {
						bestMTime = info.ModTime()
						bestPath = filepath.Join(dayPath, name)
					}
				}
			}
		}
	}
	return bestPath
}

// findLatestRollout returns the path of the most-recently-modified rollout
// JSONL across all date-bucketed directories under sessionsDir, or "" if none
// is found.
//
// Codex date-buckets sessions under <YYYY>/<MM>/<DD>/ which means we need to
// walk two levels deep (year → month → day → files). ReadDir + mtime
// comparison is cheap here because the tree is shallow and date-bucketed.
func findLatestRollout(sessionsDir string) string {
	var bestPath string
	var bestMTime time.Time

	// Walk year/month/day sub-directories.
	years, err := os.ReadDir(sessionsDir)
	if err != nil {
		return ""
	}
	for _, year := range years {
		if !year.IsDir() {
			continue
		}
		yearPath := filepath.Join(sessionsDir, year.Name())
		months, err := os.ReadDir(yearPath)
		if err != nil {
			continue
		}
		for _, month := range months {
			if !month.IsDir() {
				continue
			}
			monthPath := filepath.Join(yearPath, month.Name())
			days, err := os.ReadDir(monthPath)
			if err != nil {
				continue
			}
			for _, day := range days {
				if !day.IsDir() {
					continue
				}
				dayPath := filepath.Join(monthPath, day.Name())
				entries, err := os.ReadDir(dayPath)
				if err != nil {
					continue
				}
				for _, e := range entries {
					if e.IsDir() {
						continue
					}
					if !strings.HasPrefix(e.Name(), "rollout-") || !strings.HasSuffix(e.Name(), ".jsonl") {
						continue
					}
					info, err := e.Info()
					if err != nil {
						continue
					}
					if info.ModTime().After(bestMTime) {
						bestMTime = info.ModTime()
						bestPath = filepath.Join(dayPath, e.Name())
					}
				}
			}
		}
	}
	return bestPath
}

// ---------------------------------------------------------------------------
// tmux helpers
// ---------------------------------------------------------------------------

// realSendKeys shells out to tmux send-keys. -l (literal) prevents key-spec
// interpretation; the trailing Enter is a separate invocation.
func realSendKeys(pane, text string) error {
	if pane == "" {
		return errors.New("codex: empty pane id")
	}
	if err := exec.Command("tmux", "send-keys", "-l", "-t", pane, text).Run(); err != nil {
		return fmt.Errorf("codex: send-keys text: %w", err)
	}
	if err := exec.Command("tmux", "send-keys", "-t", pane, "Enter").Run(); err != nil {
		return fmt.Errorf("codex: send-keys enter: %w", err)
	}
	return nil
}

// realCapturePane shells out to tmux capture-pane and returns the visible
// buffer content as a string.
func realCapturePane(pane string) (string, error) {
	if pane == "" {
		return "", errors.New("codex: empty pane id for capture-pane")
	}
	out, err := exec.Command("tmux", "capture-pane", "-p", "-t", pane).Output()
	if err != nil {
		return "", fmt.Errorf("codex: capture-pane: %w", err)
	}
	return string(out), nil
}

// Abort delivers the orch.signal.interrupt verb (see docs/orch-signals.md)
// to the bound tmux pane by sending Ctrl-C. The in-pane codex REPL
// interprets that as "stop the current generation"; the next prompt is
// unaffected (Close is the teardown path).
//
// No-op when Pane is empty (test harnesses occasionally construct the
// adapter without a pane to exercise the marker loops).
func (a *Adapter) Abort(_ context.Context) error {
	if a.Pane == "" {
		return nil
	}
	return exec.Command("tmux", "send-keys", "-t", a.Pane, "C-c").Run()
}

// Compile-time check that Adapter satisfies shim.Adapter and shim.Aborter.
var (
	_ shim.Adapter = (*Adapter)(nil)
	_ shim.Aborter = (*Adapter)(nil)
)
