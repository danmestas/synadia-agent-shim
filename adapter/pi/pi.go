// Package pi bridges the orch-agent-shim to the pi coding agent CLI.
// It does three things:
//
//  1. Tails the agent's transcript JSONL under ~/.pi/agent/sessions/ and
//     emits a Synadia response chunk per content line.
//  2. Watches ~/.cache/orch-stop/<pane>.event to terminate the active
//     stream. NOTE (orch#94): the production marker-writer (the legacy
//     pi-extension orch-stop-marker.ts) was retired; this watch survives
//     for the test suite and as substrate for a future bus-native
//     turn-end detector.
//  3. Synthesizes §7 query chunks via the idle-with-prompt heuristic
//     (Plan 11): pi has no native Notification event, so we emit a
//     synthetic Query at the same moment as the Terminator.
//
// Inbound prompts are delivered via tmux send-keys, identical to the
// claude-code adapter.
//
// Transcript path: ~/.pi/agent/sessions/<encoded-cwd>/<ts>_<sid>.jsonl
// where <encoded-cwd> replaces both `/` and `.` with `-` — e.g.
// `/Users/d/p/proj` → `-Users-d-p-proj` (pi convention).
//
// This adapter follows the backpressure patterns established in the
// claude-code adapter (cc.go): Start binds watchers to shim lifetime ctx;
// emit uses a non-blocking stopCh guard; Close uses sync.Once.
package pi

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

// Adapter is the pi implementation of shim.Adapter. Constructed once per
// pane and reused across prompts; the shim's event pump reads Events()
// for the lifetime of the process.
//
// Watcher lifetime is decoupled from prompt lifetime. Callers SHOULD
// invoke Start(shimCtx) once at boot — that wires the transcript tail
// and marker watcher to the shim's lifetime context. OnPrompt's ctx is
// intentionally NOT propagated to watchers.
type Adapter struct {
	// Pane is the raw tmux pane id (e.g. "%37"). Used to derive the
	// stop-marker file path and as the send-keys target.
	Pane string
	// CWD is the pane's working directory. Used to derive the pi
	// transcript path: ~/.pi/agent/sessions/<encoded-cwd>/<ts>_<sid>.jsonl
	// where encoded-cwd replaces both `/` and `.` with `-`.
	CWD string
	// StopMarkerDir overrides ~/.cache/orch-stop/. Tests use this.
	StopMarkerDir string
	// PiSessionsDir overrides ~/.pi/agent/sessions/. Tests use this.
	PiSessionsDir string
	// SessionID, when non-empty, pins the transcript tailer to the JSONL
	// matching `<encoded-cwd>/*_<SessionID>.jsonl` and disables the
	// latest-mtime scan. This is the issue #11 fix: when multiple pi
	// processes share a cwd, mtime discovery races. Pi names its
	// transcripts `<timestamp>_<session-uuid>.jsonl`, so we accept just
	// the session-uuid suffix and resolve the timestamp prefix at
	// startup by globbing the encoded-cwd dir. When empty, the legacy
	// findLatestJSONL behaviour is preserved for backwards compatibility.
	SessionID string
	// SendKeys is the function invoked to deliver a prompt to the pane.
	// Default is realSendKeys (which shells out to tmux). Tests substitute
	// a recorder.
	SendKeys SendKeysFunc

	// events is the channel the shim drains.
	events chan shim.Chunk

	// Start binds watchers to a shim-lifetime context once. Idempotent;
	// repeated calls are no-ops returning the original start error (if any).
	startOnce sync.Once
	startErr  error
	started   atomic.Bool

	// Close shuts down watchers and closes the events channel exactly once.
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

// Start binds the background watchers (transcript tail, stop-marker watch)
// to `ctx`. Idempotent — repeated calls return the original error without
// restarting anything.
//
// Callers SHOULD pass the shim's lifetime context here, NOT a per-prompt
// context. The watchers run until either ctx is cancelled or Close() fires.
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
// multiple goroutines (e.g. the shim's deferred stop() + a test's t.Cleanup).
//
// Closing the events channel tells the shim's eventPump to exit.
func (a *Adapter) Close() error {
	a.closeOnce.Do(func() {
		close(a.stopCh)
		close(a.events)
	})
	return nil
}

// OnPrompt delivers `text` to the pi TUI via tmux send-keys. If Start
// hasn't been called yet, OnPrompt calls it with ctx as a safety net —
// but production callers should call Start(shimCtx) at boot to keep
// watcher lifetime independent of per-prompt cancellation.
//
// OnPrompt does not block on the agent's response. The transcript tailer
// will emit response chunks as the assistant turn progresses, and the
// stop-marker watcher will emit a terminator when pi finishes the turn.
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

// startWatchers kicks off the transcript tailer and the stop-marker watcher.
func (a *Adapter) startWatchers(ctx context.Context) error {
	if err := a.ensureStopDir(); err != nil {
		return err
	}

	// fsnotify watcher for the stop-marker directory. We watch the
	// DIRECTORY, not the individual file — the file may not exist yet,
	// but fsnotify only supports watching existing paths, so directory-
	// level watching catches the CREATE event on first write.
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("pi: fsnotify: %w", err)
	}
	if err := w.Add(a.stopDir()); err != nil {
		_ = w.Close()
		return fmt.Errorf("pi: watch stop dir: %w", err)
	}

	go a.markerLoop(ctx, w)
	go a.transcriptLoop(ctx)
	return nil
}

func (a *Adapter) ensureStopDir() error {
	if err := os.MkdirAll(a.stopDir(), 0o755); err != nil {
		return fmt.Errorf("pi: mkdir %s: %w", a.stopDir(), err)
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
	if a.PiSessionsDir != "" {
		return a.PiSessionsDir
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".pi", "agent", "sessions")
}

// markerLoop reacts to fsnotify CREATE / WRITE events on the stop-marker
// directory. The historical producer (the legacy orch-stop-marker.ts
// pi-extension, retired in #94) used an atomic tmpfile-rename, which
// surfaces as a single CREATE event on the destination path. Live writers
// are gone; the loop survives for the test suite.
//
// Stop marker → emit Terminator chunk to close the active stream.
// Synthetic query (Plan 11 idle-with-prompt heuristic): pi has no native
// Notification event, so we also emit a Query chunk at turn-end so the
// caller can detect "pi is idle, next prompt accepted". The query is
// emitted BEFORE the terminator so the caller sees it while the stream
// is still active.
func (a *Adapter) markerLoop(ctx context.Context, w *fsnotify.Watcher) {
	defer w.Close()
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
			// We only act on CREATE / WRITE on our specific pane's marker.
			// Renames-into-place from orch-stop-marker.ts surface as CREATE
			// on the destination path on macOS/Linux.
			if ev.Op&(fsnotify.Create|fsnotify.Write) == 0 {
				continue
			}
			if ev.Name != stopPath {
				continue
			}
			// Synthetic query chunk (Plan 11): pi has no Notification event.
			// Emitting before the terminator lets the caller detect idle state
			// while the stream is still open and can carry the query payload.
			//
			// Divergence from codex: pi has no native Notification event AND
			// no separate idle state — turn-end IS the operator-attention
			// moment, so we emit the synthetic query at stop-marker time
			// rather than via time-based idle detection (cf. codex adapter's
			// idleQueryLoop). Simpler and arguably more correct for pi: the
			// stop-marker IS the idle signal.
			id := fmt.Sprintf("pi-idle-%d", time.Now().UnixNano())
			a.emit(shim.NewQueryChunk(id, "", "pi agent turn ended — ready for next prompt"))
			a.emit(shim.NewTerminatorChunk())
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
			// fsnotify errors are transient — drop and continue.
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

// transcriptLoop hunts for the pi transcript JSONL and tails it.
// Pi writes transcripts at:
//
//	~/.pi/agent/sessions/<encoded-cwd>/<timestamp>_<session_uuid>.jsonl
//
// where <encoded-cwd> replaces both `/` and `.` with `-` (same rule as
// claude's encoding).
//
// We poll every 250ms for the file's appearance because:
//   - The pane may not have started pi yet on shim startup.
//   - The session_id is embedded in the filename and is only known after
//     pi writes its first JSONL line.
//
// Once we find a candidate file we tail it line-by-line, parse JSON,
// and emit response chunks for assistant content.
func (a *Adapter) transcriptLoop(ctx context.Context) {
	encoded := encodePiPath(a.CWD)
	sessionDir := filepath.Join(a.sessionsDir(), encoded)

	// discover picks the JSONL to tail. When SessionID is set we glob
	// `<sessionDir>/*_<SessionID>.jsonl` — pi writes `<ts>_<sid>.jsonl`
	// and only the suffix is operator-knowable ahead of time. When
	// SessionID is empty we fall back to mtime discovery.
	discover := func() string {
		if a.SessionID == "" {
			return findLatestJSONL(sessionDir)
		}
		matches, err := filepath.Glob(filepath.Join(sessionDir, "*_"+a.SessionID+".jsonl"))
		if err != nil || len(matches) == 0 {
			return ""
		}
		// Multiple matches with the same session-id suffix would be a
		// pi-side bug; first match is good enough.
		return matches[0]
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
		// Pi tool-result lines can be large — bump to 8MB to avoid silent truncation.
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
		// the newest .jsonl if one appeared (new pi session).
		if a.SessionID == "" {
			newest := findLatestJSONL(sessionDir)
			if newest != "" && newest != watchedPath {
				_ = watchedFile.Close()
				watchedFile = nil
			}
		}
	}
}

// piTranscriptEntry is the subset of the pi JSONL line shape we care about.
// Pi's transcript format mirrors claude-code's (type + message.content[]).
// Unknown fields are dropped silently by encoding/json (forward-compat).
type piTranscriptEntry struct {
	Type    string `json:"type"`
	Message struct {
		Content []piTranscriptBlock `json:"content"`
	} `json:"message"`
}

type piTranscriptBlock struct {
	Type  string         `json:"type"`
	Text  string         `json:"text"`
	Name  string         `json:"name"`
	Input map[string]any `json:"input"`
}

// emitFromTranscriptLine parses one JSONL line and emits 0+ chunks.
// We emit one Synadia chunk per content block (text / tool_use / thinking).
func (a *Adapter) emitFromTranscriptLine(line []byte) {
	var e piTranscriptEntry
	if err := json.Unmarshal(line, &e); err != nil {
		// Malformed line — drop defensively.
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

// encodePiPath encodes the cwd for the pi sessions directory. Pi uses the
// same encoding as claude-code: both `/` AND `.` become `-`. Verified from
// orch-nats-publish-jsonl.ts: `cwd.replace(/[/.]/g, "-")`.
//
// Example: `/Users/d/p/proj` → `-Users-d-p-proj`
//
// NOTE: This encoding is LOSSY (same caveat as the claude adapter) —
// "/foo.bar" and "/foo/bar" both collapse to "-foo-bar". We mirror it
// faithfully because we're reproducing pi's own naming convention.
func encodePiPath(p string) string {
	p = strings.ReplaceAll(p, "/", "-")
	p = strings.ReplaceAll(p, ".", "-")
	return p
}

// findLatestJSONL returns the most-recently-modified .jsonl file in `dir`,
// or "" if the directory doesn't exist or contains none.
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

// realSendKeys is the default SendKeys implementation: shells out to tmux.
// -l (literal) prevents key-spec interpretation; the trailing Enter is a
// separate invocation because -l also suppresses the special-key parsing
// we WANT for the trailing Enter.
func realSendKeys(pane, text string) error {
	if pane == "" {
		return errors.New("pi: empty pane id")
	}
	if err := exec.Command("tmux", "send-keys", "-l", "-t", pane, text).Run(); err != nil {
		return fmt.Errorf("pi: send-keys text: %w", err)
	}
	if err := exec.Command("tmux", "send-keys", "-t", pane, "Enter").Run(); err != nil {
		return fmt.Errorf("pi: send-keys enter: %w", err)
	}
	return nil
}

// Abort delivers the orch.signal.interrupt verb (see docs/orch-signals.md)
// to the bound tmux pane by sending Ctrl-C. The in-pane pi REPL
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

// Compile-time assertion: Adapter must satisfy shim.Adapter and shim.Aborter.
var (
	_ shim.Adapter = (*Adapter)(nil)
	_ shim.Aborter = (*Adapter)(nil)
)
