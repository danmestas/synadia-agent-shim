// Package claudecode bridges the orch-agent-shim to the claude-code
// CLI. It does three things:
//
//  1. Tails the agent's transcript JSONL — the only source of truth for
//     "what the assistant said" — and emits a Synadia response chunk
//     per content-delta line.
//  2. Watches ~/.cache/orch-stop/<pane>.event and ~/.cache/orch-notify/<pane>.notify
//     marker files. NOTE (orch#94): the production hook writers were
//     retired; turn-end is now signalled by the bus terminator chunk and
//     the test suite drives this loop via a tempdir. Live turn-end
//     detection in this adapter is currently a follow-up (track in a
//     future cleanup issue).
//  3. Injects inbound prompts back into the pane via `tmux send-keys`,
//     so the claude TUI's input box gets the text.
//
// Why this layer exists. claude-code has no JSON-RPC mode and no stable
// stdin/stdout protocol — it's a TUI. The transcript file is what
// hooks (and now this shim) cooperate around. The shim's protocol
// duties (chunking, ack, terminator) belong in the shim core; the
// per-harness file-tail-and-send-keys discipline belongs here.
package claudecode

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// Adapter is the claude-code implementation of shim.Adapter. It is
// constructed once per pane and re-used across prompts; the shim's
// event pump reads Events() for the lifetime of the process.
//
// Watcher lifetime is decoupled from prompt lifetime. Callers SHOULD
// invoke Start(shimCtx) once at boot — that wires the transcript tail
// and marker watchers to the shim's lifetime context. OnPrompt's ctx
// (per the Adapter contract: "cancellation means stop the current
// turn") is intentionally NOT propagated to the watchers — tearing
// them down on the first prompt's cancel would dismantle the adapter.
// As a safety net, the first OnPrompt also calls Start with its own
// ctx if Start hasn't been called yet, but production callers should
// always call Start explicitly.
type Adapter struct {
	// Pane is the raw tmux pane id (e.g. "%37"). Used to derive the
	// marker file paths and as the send-keys target.
	Pane string
	// CWD is the pane's working directory. Used to derive the
	// claude transcript path: ~/.claude/projects/<encoded-cwd>/<sess>.jsonl
	// where encoded-cwd replaces both `/` and `.` with `-`.
	CWD string
	// StopMarkerDir / NotifyMarkerDir override the marker directories
	// (default ~/.cache/orch-stop/ + ~/.cache/orch-notify/). Tests use
	// this to point at a tempdir.
	StopMarkerDir   string
	NotifyMarkerDir string
	// ClaudeProjectsDir overrides ~/.claude/projects/. Tests use this.
	ClaudeProjectsDir string
	// SessionID, when non-empty, pins the transcript tailer to exactly
	// `<ClaudeProjectsDir>/<encoded-cwd>/<SessionID>.jsonl` and disables
	// the latest-mtime scan. This is the issue #11 fix: when multiple
	// claude-code processes share a cwd (a developer's own session plus
	// a shim-spawned TUI), mtime-discovery races and the shim ends up
	// tailing the wrong JSONL. Callers (orch-spawn, sesh-tooling) snapshot
	// `~/.claude/projects/<encoded>/` before spawning the wrapped claude
	// and pass the new entry as `-session-id`. When empty, the legacy
	// findLatestJSONL behaviour is preserved for backwards compatibility.
	SessionID string
	// SendKeys is the function invoked to deliver a prompt to the pane.
	// Default is realSendKeys (which shells out to tmux). Tests
	// substitute a recorder.
	SendKeys SendKeysFunc

	// events is the channel the shim drains.
	events chan shim.Chunk

	// Start binds watchers to a shim-lifetime context once. Idempotent;
	// repeated calls are no-ops returning the original start error (if
	// any). `started` is read on OnPrompt's hot path so we use atomic
	// rather than holding startOnce open.
	startOnce sync.Once
	startErr  error
	started   atomic.Bool

	// Close shuts down watchers and closes the events channel exactly
	// once. closeOnce makes Close safe to call from multiple goroutines
	// (e.g. the shim's deferred stop() + a test's t.Cleanup).
	closeOnce sync.Once
	stopCh    chan struct{}
}

// SendKeysFunc is the seam that lets tests replace the tmux invocation
// with a recorder. The default implementation shells out to:
//
//	tmux send-keys -l -t <pane> <text>
//	tmux send-keys -t <pane> Enter
//
// (-l is literal mode so prompt text never gets interpreted as a tmux
// key spec — critical for prompts containing C-c, Up, etc.)
type SendKeysFunc func(pane, text string) error

// New constructs an Adapter with reasonable defaults applied.
func New(pane, cwd string) *Adapter {
	return &Adapter{
		Pane:     pane,
		CWD:      cwd,
		SendKeys: realSendKeys,
		events:   make(chan shim.Chunk, 64),
		stopCh:   make(chan struct{}),
	}
}

// Events returns the shim-facing chunk channel. The channel is closed
// when Close() is called.
func (a *Adapter) Events() <-chan shim.Chunk { return a.events }

// Start binds the background watchers (transcript tail, marker watch)
// to `ctx`. Idempotent — repeated calls return the original error
// without restarting anything.
//
// Callers SHOULD pass the shim's lifetime context here, NOT a
// per-prompt context. The watchers run until either ctx is cancelled
// or Close() is invoked.
func (a *Adapter) Start(ctx context.Context) error {
	a.startOnce.Do(func() {
		a.startErr = a.startWatchers(ctx)
		if a.startErr == nil {
			a.started.Store(true)
		}
	})
	return a.startErr
}

// Close shuts down background watchers (via stopCh) and closes the
// events channel. Idempotent — guarded by closeOnce so it's safe to
// call from multiple goroutines.
//
// Closing the events channel is what tells the shim's eventPump to
// exit: an unclosed channel would leave the pump blocked forever on
// the receive even after watcher shutdown.
func (a *Adapter) Close() error {
	a.closeOnce.Do(func() {
		close(a.stopCh)
		close(a.events)
	})
	return nil
}

// OnPrompt delivers `text` to the claude TUI via tmux send-keys. If
// Start hasn't been called yet, OnPrompt calls it with ctx as a
// safety net — but production callers should call Start(shimCtx) at
// boot to keep watcher lifetime independent of per-prompt cancellation.
//
// OnPrompt does not block on the agent's response. The transcript
// tailer will emit response chunks as the assistant turn progresses,
// and the stop-marker watcher will emit a terminator when claude
// finishes the turn.
//
// ctx cancellation is respected only for send-keys delivery; ongoing
// turns continue (claude has no "interrupt this turn" API beyond
// Ctrl-C, which isn't safe to drive from here).
func (a *Adapter) OnPrompt(ctx context.Context, text string) error {
	if !a.started.Load() {
		if err := a.Start(ctx); err != nil {
			return err
		}
	}
	if a.SendKeys == nil {
		a.SendKeys = realSendKeys
	}
	// Per-call ctx check: if the caller cancelled before we delivered,
	// don't bother shelling out.
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.SendKeys(a.Pane, text)
}

// startWatchers kicks off the transcript tailer and the marker watcher.
// Errors are surfaced via startErr so the shim's OnPrompt path returns
// a fail-fast error rather than running with a half-broken adapter.
func (a *Adapter) startWatchers(ctx context.Context) error {
	if err := a.ensureMarkerDirs(); err != nil {
		return err
	}

	// fsnotify watcher for marker files. We watch the DIRECTORIES, not
	// individual files — the file may not exist yet, but fsnotify only
	// supports watching existing paths, so directory-level watching
	// catches the CREATE event on first write.
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("claudecode: fsnotify: %w", err)
	}
	if err := w.Add(a.stopDir()); err != nil {
		_ = w.Close()
		return fmt.Errorf("claudecode: watch stop dir: %w", err)
	}
	if err := w.Add(a.notifyDir()); err != nil {
		_ = w.Close()
		return fmt.Errorf("claudecode: watch notify dir: %w", err)
	}

	go a.markerLoop(ctx, w)
	go a.transcriptLoop(ctx)
	return nil
}

func (a *Adapter) ensureMarkerDirs() error {
	for _, d := range []string{a.stopDir(), a.notifyDir()} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("claudecode: mkdir %s: %w", d, err)
		}
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

func (a *Adapter) notifyDir() string {
	if a.NotifyMarkerDir != "" {
		return a.NotifyMarkerDir
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".cache", "orch-notify")
}

func (a *Adapter) stopMarker() string {
	return filepath.Join(a.stopDir(), a.Pane+".event")
}

func (a *Adapter) notifyMarker() string {
	return filepath.Join(a.notifyDir(), a.Pane+".notify")
}

// markerLoop reacts to fsnotify CREATE / WRITE events on the marker
// directories. NOTE (orch#94): the production marker-writer hooks
// (orch-stop-marker.sh, orch-notify-marker.sh) were retired; in live
// operation the markers are no longer written. This loop remains so the
// adapter's test suite can drive it directly (cc_test.go writes markers
// into a tempdir), and as a substrate for a future turn-end detector
// rewritten on top of bus signals. Atomic tmpfile-then-rename writes
// produce one CREATE event per turn, which is what the loop expects.
//
// Stop marker → emit Terminator chunk to close the active stream.
// Notify marker → emit Query chunk so the caller can answer the
// "Claude is waiting for input" prompt.
func (a *Adapter) markerLoop(ctx context.Context, w *fsnotify.Watcher) {
	defer w.Close()
	stopPath := a.stopMarker()
	notifyPath := a.notifyMarker()
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
			// We only act on CREATE / WRITE on our specific pane's
			// markers. Renames-into-place from the hook scripts surface
			// as CREATE on the destination path on macOS/Linux fsnotify.
			if ev.Op&(fsnotify.Create|fsnotify.Write) == 0 {
				continue
			}
			switch ev.Name {
			case stopPath:
				a.emit(shim.NewTerminatorChunk())
			case notifyPath:
				text := readFileTrim(notifyPath)
				if text == "" {
					continue
				}
				// Mid-stream query: tag with the marker mtime so the
				// caller's reply correlates with this specific prompt.
				// reply_subject is reserved for future plumbing — v1
				// emits the query chunk but doesn't route the reply
				// back via send-keys (Plan 9 territory).
				id := fmt.Sprintf("notify-%d", time.Now().UnixNano())
				a.emit(shim.NewQueryChunk(id, "", text))
			}
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
			// fsnotify errors are transient — log path would require a
			// logger we don't carry. Drop and continue; the next event
			// will succeed or the directory has truly disappeared.
		}
	}
}

// emit publishes a chunk to the shim, or returns silently if Close has
// fired. Selecting on stopCh first guarantees we never send to a closed
// events channel (Close closes stopCh BEFORE closing events).
func (a *Adapter) emit(c shim.Chunk) {
	// Fast path: if Close has fired, stopCh is closed and the receive
	// returns immediately. Selecting BEFORE attempting the send avoids
	// the race where stopCh is closed but a send is selected first.
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

// transcriptLoop hunts for the claude transcript JSONL and tails it.
// TODO(plan-future): promote from 250ms polling to fsnotify on the
// projects dir once the cold-start race (claude creates the file AFTER
// the first prompt) is handled. The poll is cheap enough for v1
// (one stat per pane per 250ms) and avoids a fsnotify-Add-before-mkdir
// dance.
// The file path encodes the cwd by replacing both `/` and `.` with `-`
// (claude convention — see ~/.claude/projects/ on any active install).
//
// We poll for the file's appearance because:
//
//   - The pane may not have started claude yet on shim startup.
//   - The session_id is part of the filename and is only known after
//     claude writes its first JSONL line.
//
// Once we find a candidate file we tail it line-by-line, parse JSON,
// and emit `response` chunks for assistant content blocks.
func (a *Adapter) transcriptLoop(ctx context.Context) {
	dir := a.projectsDir()
	encoded := encodeProjectPath(a.CWD)
	target := filepath.Join(dir, encoded)
	// pinned is the absolute path the SessionID branch opens, or "" when
	// the legacy latest-mtime discovery is in effect. The discover
	// closure isolates the choice so the tail loop below stays single-
	// shape regardless of mode.
	var pinned string
	if a.SessionID != "" {
		pinned = filepath.Join(target, a.SessionID+".jsonl")
	}
	discover := func() string {
		if pinned != "" {
			// Only open the pinned file once it exists; the wrapped
			// claude-code TUI may not have written its first JSONL line
			// yet at shim startup. Return "" until os.Stat succeeds so
			// the loop keeps polling without falling back to mtime scan.
			if _, err := os.Stat(pinned); err != nil {
				return ""
			}
			return pinned
		}
		return findLatestJSONL(target)
	}
	var (
		watchedFile *os.File
		watchedPath string
		offset      int64
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
		}
		// Read whatever has been appended since last poll.
		if _, err := watchedFile.Seek(offset, 0); err != nil {
			_ = watchedFile.Close()
			watchedFile = nil
			continue
		}
		sc := bufio.NewScanner(watchedFile)
		// Some transcript lines (with embedded tool results) can be
		// large — bump the default 64KB token cap to 8MB so we don't
		// silently truncate mid-line.
		sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			a.emitFromTranscriptLine(line)
		}
		// Update offset to the file's current position.
		pos, err := watchedFile.Seek(0, 1)
		if err == nil {
			offset = pos
		}
		// Session-pinned mode: never switch files. Legacy mode: switch to
		// the newest .jsonl if one appeared (new claude session).
		if pinned == "" {
			newest := findLatestJSONL(target)
			if newest != "" && newest != watchedPath {
				_ = watchedFile.Close()
				watchedFile = nil
			}
		}
	}
}

func (a *Adapter) projectsDir() string {
	if a.ClaudeProjectsDir != "" {
		return a.ClaudeProjectsDir
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".claude", "projects")
}

// transcriptEntry is the subset of the claude-code JSONL line shape we
// care about. Lines we ignore have `type` set to `user`, `summary`,
// `system`, etc. Assistant content arrives as `type:"assistant"` with
// a `message.content[]` array of blocks. Each block has its own type
// (`text`, `tool_use`, `thinking`).
//
// We MUST tolerate unknown fields — claude evolves the schema between
// releases. encoding/json drops unknown keys silently, which is the
// behavior we want.
type transcriptEntry struct {
	Type    string `json:"type"`
	Message struct {
		Content []transcriptBlock `json:"content"`
	} `json:"message"`
}

type transcriptBlock struct {
	Type string `json:"type"`
	// `text` is set when Type=="text".
	Text string `json:"text"`
	// Tool-use blocks carry name + input; we surface the name and
	// pass-through the full block as the chunk data so callers
	// see the same shape claude emits.
	Name  string         `json:"name"`
	Input map[string]any `json:"input"`
}

// emitFromTranscriptLine parses one JSONL line and emits 0+ chunks.
// Granularity choice: one Synadia chunk per content BLOCK (text /
// tool_use / thinking) — this preserves block boundaries the operator
// UX cares about (e.g. tool_use chunks can be rendered differently).
func (a *Adapter) emitFromTranscriptLine(line []byte) {
	var e transcriptEntry
	if err := json.Unmarshal(line, &e); err != nil {
		return
	}
	if e.Type != "assistant" {
		return
	}
	for _, b := range e.Message.Content {
		switch b.Type {
		case "text":
			if b.Text == "" {
				continue
			}
			a.emit(shim.NewResponseChunk(b.Text))
		case "thinking":
			if b.Text == "" {
				continue
			}
			a.emit(shim.Chunk{Type: shim.ChunkThinking, Data: b.Text})
		case "tool_use":
			a.emit(shim.Chunk{Type: shim.ChunkToolUse, Data: map[string]any{
				"name":  b.Name,
				"input": b.Input,
			}})
		}
	}
}

// encodeProjectPath replicates claude-code's project-directory encoding:
// both `/` AND `.` become `-`. Verified against ~/.claude/projects/
// on any active install.
//
// NOTE: This encoding is LOSSY — "/foo.bar" and "/foo/bar" both collapse
// to "-foo-bar". We mirror it faithfully because callers look up the
// transcript by reproducing claude's own naming; "fixing" the collision
// would diverge from the directory layout on disk and break the lookup.
// In practice the collision is harmless because each pane has exactly
// one CWD value, so two panes only collide if they're at distinct paths
// that happen to encode identically — and even then they get separate
// session-id-keyed JSONL files inside the shared directory.
func encodeProjectPath(p string) string {
	p = strings.ReplaceAll(p, "/", "-")
	p = strings.ReplaceAll(p, ".", "-")
	return p
}

// findLatestJSONL returns the most-recently-modified .jsonl file in
// `dir`, or "" if the directory doesn't exist or contains none.
func findLatestJSONL(dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	var bestPath string
	var bestMTime time.Time
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(bestMTime) {
			bestMTime = info.ModTime()
			bestPath = filepath.Join(dir, e.Name())
		}
	}
	return bestPath
}

func readFileTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// realSendKeys is the default SendKeys implementation: shells out to
// tmux. -l (literal) prevents key-spec interpretation; the trailing
// Enter is a separate invocation because -l also suppresses the
// special-key parsing we WANT for the trailing Enter.
func realSendKeys(pane, text string) error {
	if pane == "" {
		return errors.New("claudecode: empty pane id")
	}
	if err := exec.Command("tmux", "send-keys", "-l", "-t", pane, text).Run(); err != nil {
		return fmt.Errorf("claudecode: send-keys text: %w", err)
	}
	if err := exec.Command("tmux", "send-keys", "-t", pane, "Enter").Run(); err != nil {
		return fmt.Errorf("claudecode: send-keys enter: %w", err)
	}
	return nil
}

// Abort delivers the orch.signal.interrupt verb (see docs/orch-signals.md)
// to the bound tmux pane by sending Ctrl-C. The in-pane claude-code REPL
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
