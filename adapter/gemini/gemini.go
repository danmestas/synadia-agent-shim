// Package gemini bridges the orch-agent-shim to the gemini-cli CLI. It
// does three things:
//
//  1. Tails gemini-cli's chat JSONL under ~/.gemini/tmp/<scope>/chats/
//     and emits a Synadia response chunk per model-role line.
//
//  2. Watches ~/.cache/orch-stop/<pane>.event and
//     ~/.cache/orch-notify/<pane>.notify, emitting typed Synadia chunks:
//     stop → Terminator, notify → Query.
//
//     NOTE (orch#94): the production marker-writer hooks were retired.
//     This loop remains for the test suite and as the substrate for a
//     future bus-native turn-end detector (follow-up).
//
//  3. Injects inbound prompts back into the pane via `tmux send-keys`.
//
// AfterAgent quirk. gemini-cli's turn-end event is named "AfterAgent",
// NOT "Stop". When per-harness hook writers are reintroduced (if ever),
// the gemini hook must wire under AfterAgent, not Stop — gemini-cli
// silently rejects unknown event names.
//
// Transcript shape. Each JSONL line is a SDK-style Content record:
//
//	{"role":"model","parts":[{"text":"..."}]}
//	{"role":"user","parts":[{"text":"..."}]}
//	{"role":"model","parts":[{"functionCall":{"name":"...","args":{...}}}]}
//
// We act only on `role:"model"` lines: text parts become response chunks,
// functionCall parts become tool_use chunks. Unknown fields drop silently
// (encoding/json forward-compat).
//
// Scope path. gemini-cli buckets chats under an opaque `<scope>` directory
// derived from project context. We walk GeminiChatsDir (default
// ~/.gemini/tmp/) recursively for the newest `session-*.jsonl` regardless
// of the intermediate scope, since the scope-encoding algorithm is not
// stable enough to reproduce here.
package gemini

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// Adapter is the gemini-cli implementation of shim.Adapter. It is
// constructed once per pane and re-used across prompts.
//
// Watcher lifetime is decoupled from prompt lifetime — exactly the same
// invariant as the claudecode adapter. Callers SHOULD invoke
// Start(shimCtx) at boot; OnPrompt's per-turn ctx MUST NOT be
// propagated to the watchers.
type Adapter struct {
	// Pane is the raw tmux pane id (e.g. "%37"). On cmux/zmx workers,
	// Pane carries the engine-native identifier so marker-keyed paths
	// remain stable per worker.
	Pane string

	// Locator is the engine-aware target for realSendKeys / Abort
	// (shim.LocatorSendKeys). Defaults to tmux:<Pane> via New; cmux/zmx
	// callers use NewForLocator.
	Locator shim.Locator

	// StopMarkerDir / NotifyMarkerDir override the marker directories
	// (default ~/.cache/orch-stop/ + ~/.cache/orch-notify/). Tests use
	// this to point at a tempdir.
	StopMarkerDir   string
	NotifyMarkerDir string

	// GeminiChatsDir overrides ~/.gemini/tmp/. Tests use this. The tailer
	// walks the directory tree recursively for the newest
	// `session-*.jsonl` file.
	GeminiChatsDir string

	// SendKeys is the function invoked to deliver a prompt to the pane.
	// Default is realSendKeys (which shells out to tmux). Tests
	// substitute a recorder.
	SendKeys SendKeysFunc

	// events is the channel the shim drains.
	events chan shim.Chunk

	// startOnce makes Start idempotent — repeated calls are no-ops.
	startOnce sync.Once
	startErr  error
	started   atomic.Bool

	// closeOnce makes Close safe to call from multiple goroutines.
	closeOnce sync.Once
	stopCh    chan struct{}
}

// SendKeysFunc is the seam that lets tests replace the tmux invocation
// with a recorder. The default implementation shells out to:
//
//	tmux send-keys -l -t <pane> <text>
//	tmux send-keys -t <pane> Enter
//
// (-l is literal mode so prompt text is never interpreted as a tmux
// key spec — critical for prompts containing C-c, Up, etc.)
type SendKeysFunc func(pane, text string) error

// New constructs an Adapter with reasonable defaults applied. pane is
// interpreted as a tmux pane id for back-compat; cmux/zmx callers use
// NewForLocator.
func New(pane string) *Adapter {
	return &Adapter{
		Pane:     pane,
		Locator:  shim.Locator{Engine: shim.EngineTmux, Value: pane},
		SendKeys: realSendKeys,
		events:   make(chan shim.Chunk, 64),
		stopCh:   make(chan struct{}),
	}
}

// NewForLocator constructs an Adapter for an engine-aware Locator.
func NewForLocator(loc shim.Locator) *Adapter {
	return &Adapter{
		Pane:     loc.Value,
		Locator:  loc,
		SendKeys: realSendKeys,
		events:   make(chan shim.Chunk, 64),
		stopCh:   make(chan struct{}),
	}
}

// Events returns the shim-facing chunk channel. The channel is closed
// when Close() is called.
func (a *Adapter) Events() <-chan shim.Chunk { return a.events }

// Start binds the background marker watcher to `ctx`. Idempotent —
// repeated calls return the original error without restarting anything.
//
// Callers SHOULD pass the shim's lifetime context here, NOT a
// per-prompt context. The watcher runs until either ctx is cancelled
// or Close() is invoked.
func (a *Adapter) Start(ctx context.Context) error {
	a.startOnce.Do(func() {
		a.startErr = a.startWatcher(ctx)
		if a.startErr == nil {
			a.started.Store(true)
		}
	})
	return a.startErr
}

// Close shuts down the background watcher (via stopCh) and closes the
// events channel. Idempotent — guarded by closeOnce so it is safe to
// call from multiple goroutines.
//
// Closing the events channel is what tells the shim's eventPump to
// exit.
func (a *Adapter) Close() error {
	a.closeOnce.Do(func() {
		close(a.stopCh)
		close(a.events)
	})
	return nil
}

// OnPrompt delivers `text` to the gemini TUI via tmux send-keys.
//
// If you reach the !started branch below, the caller failed to invoke
// Start(shimCtx) before OnPrompt — that is a lifecycle bug. We fall
// through anyway so a misuse does not crash, but the watchers will
// bind to the prompt ctx and tear down when this prompt completes,
// effectively disabling the adapter for subsequent turns. Production
// callers (cmd/orch-agent-shim) call Start(shimCtx) at boot and never
// hit this path; the safety net exists only for tests and exotic
// embeddings.
func (a *Adapter) OnPrompt(ctx context.Context, text string) error {
	if !a.started.Load() {
		if err := a.Start(ctx); err != nil {
			return err
		}
	}
	if a.SendKeys == nil {
		a.SendKeys = realSendKeys
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Engine-aware dispatch (shim/engine.go). See cc.go for the longer
	// comment on why non-tmux engines bypass the legacy SendKeys
	// closure.
	if a.Locator.IsValid() && a.Locator.Engine != shim.EngineTmux {
		return shim.LocatorSendKeys(a.Locator, text)
	}
	return a.SendKeys(a.Pane, text)
}

// startWatcher initialises the fsnotify watcher over the stop and
// notify marker directories, then launches markerLoop.
func (a *Adapter) startWatcher(ctx context.Context) error {
	if err := a.ensureMarkerDirs(); err != nil {
		return err
	}

	// Watch directories, not individual files — the marker file may not
	// exist yet, and fsnotify only supports watching existing paths.
	// Directory-level watching captures the CREATE event on first write.
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("gemini: fsnotify: %w", err)
	}
	if err := w.Add(a.stopDir()); err != nil {
		_ = w.Close()
		return fmt.Errorf("gemini: watch stop dir: %w", err)
	}
	if err := w.Add(a.notifyDir()); err != nil {
		_ = w.Close()
		return fmt.Errorf("gemini: watch notify dir: %w", err)
	}

	go a.markerLoop(ctx, w)
	go a.transcriptLoop(ctx)
	return nil
}

func (a *Adapter) ensureMarkerDirs() error {
	for _, d := range []string{a.stopDir(), a.notifyDir()} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("gemini: mkdir %s: %w", d, err)
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

func (a *Adapter) chatsDir() string {
	if a.GeminiChatsDir != "" {
		return a.GeminiChatsDir
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".gemini", "tmp")
}

// transcriptLoop hunts for gemini-cli's chat JSONL and tails it. The
// scope subdirectory is opaque so we walk the tree under chatsDir() for
// the newest `session-*.jsonl` by mtime. Same cost-discipline trick as
// codex's transcriptLoop: rescan for a newer session only when the
// current file has been quiet for rolloverIdleScan — an actively-writing
// session is by definition the latest.
func (a *Adapter) transcriptLoop(ctx context.Context) {
	const rolloverIdleScan = 10 * time.Second
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
			latest := findLatestGeminiSession(a.chatsDir())
			if latest == "" {
				continue
			}
			f, err := os.Open(latest)
			if err != nil {
				continue
			}
			watchedFile = f
			watchedPath = latest
			offset = 0
			lastActive = time.Now()
		}

		if _, err := watchedFile.Seek(offset, 0); err != nil {
			_ = watchedFile.Close()
			watchedFile = nil
			continue
		}
		sc := bufio.NewScanner(watchedFile)
		// Gemini chat lines can carry large function-call args; bump to 8MB.
		sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		sawBytes := false
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			sawBytes = true
			a.emitFromChatLine(line)
		}
		if pos, err := watchedFile.Seek(0, 1); err == nil {
			offset = pos
		}
		if sawBytes {
			lastActive = time.Now()
		}

		if time.Since(lastActive) >= rolloverIdleScan {
			newest := findLatestGeminiSession(a.chatsDir())
			if newest != "" && newest != watchedPath {
				_ = watchedFile.Close()
				watchedFile = nil
			} else {
				lastActive = time.Now()
			}
		}
	}
}

// geminiChatEntry is the subset of gemini-cli's Content shape we act on.
// Unknown fields drop silently via encoding/json (forward-compat).
type geminiChatEntry struct {
	Role  string           `json:"role"`
	Parts []geminiChatPart `json:"parts"`
}

type geminiChatPart struct {
	Text         string              `json:"text"`
	FunctionCall *geminiFunctionCall `json:"functionCall,omitempty"`
}

type geminiFunctionCall struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

// emitFromChatLine parses one JSONL line and emits 0+ chunks. Only
// `role:"model"` lines produce chunks; user-side entries are ignored.
func (a *Adapter) emitFromChatLine(line []byte) {
	var e geminiChatEntry
	if err := json.Unmarshal(line, &e); err != nil {
		return
	}
	if e.Role != "model" {
		return
	}
	for _, p := range e.Parts {
		switch {
		case p.FunctionCall != nil:
			a.emit(shim.Chunk{Type: shim.ChunkToolUse, Data: map[string]any{
				"name":  p.FunctionCall.Name,
				"input": p.FunctionCall.Args,
			}})
		case p.Text != "":
			a.emit(shim.NewResponseChunk(p.Text))
		}
	}
}

// findLatestGeminiSession walks `root` recursively for the most-recently-
// modified `session-*.jsonl` file. Returns "" when none exists.
func findLatestGeminiSession(root string) string {
	var bestPath string
	var bestMTime time.Time
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if !strings.HasPrefix(name, "session-") || !strings.HasSuffix(name, ".jsonl") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if info.ModTime().After(bestMTime) {
			bestMTime = info.ModTime()
			bestPath = path
		}
		return nil
	})
	return bestPath
}

// markerLoop reacts to fsnotify CREATE / WRITE events on the marker
// directories. NOTE (orch#94): the production hook writers were retired;
// in live operation this loop never fires. The loop is preserved for the
// test suite (which writes markers into a tempdir) and as substrate for
// a future bus-native turn-end detector.
//
// Stop marker → Terminator chunk; Notify marker → Query chunk.
// Atomic tmpfile-then-rename writes produce one CREATE event per turn.
func (a *Adapter) markerLoop(ctx context.Context, w *fsnotify.Watcher) {
	defer func() {
		if err := w.Close(); err != nil {
			log.Printf("gemini: marker watcher close: %v", err)
		}
	}()
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
			// Act only on CREATE / WRITE for our specific pane's markers.
			// Renames-into-place surface as CREATE on the destination path
			// on both macOS and Linux fsnotify.
			if ev.Op&(fsnotify.Create|fsnotify.Write) == 0 {
				continue
			}
			switch ev.Name {
			case stopPath:
				// AfterAgent hook wrote the stop marker — signal turn-end.
				// NOTE: this is "AfterAgent" on the gemini-cli side, NOT
				// "Stop". The hook is wired under AfterAgent; we only see
				// the file it creates here.
				a.emit(shim.NewTerminatorChunk())
			case notifyPath:
				// Native Notification event — no synthetic detection needed.
				text := readFileTrim(notifyPath)
				if text == "" {
					continue
				}
				id := fmt.Sprintf("notify-%d", time.Now().UnixNano())
				a.emit(shim.NewQueryChunk(id, "", text))
			}
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
			// fsnotify errors are transient — drop and continue. Carrying a
			// logger here would add a dependency we don't need for v1.
		}
	}
}

// emit publishes a chunk to the shim, or returns silently if Close has
// fired. The non-blocking check on stopCh prevents sends to a closed
// events channel (Close closes stopCh BEFORE closing events).
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

func readFileTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	s := string(b)
	// Trim leading/trailing whitespace — hook scripts may append newline.
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\r' || s[0] == '\n') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t' || s[len(s)-1] == '\r' || s[len(s)-1] == '\n') {
		s = s[:len(s)-1]
	}
	return s
}

// realSendKeys is the default SendKeys implementation: shells out to
// tmux. -l (literal) prevents key-spec interpretation; the trailing
// Enter is a separate invocation because -l also suppresses the
// special-key parsing we want for Enter.
func realSendKeys(pane, text string) error {
	if pane == "" {
		return errors.New("gemini: empty pane id")
	}
	if err := exec.Command("tmux", "send-keys", "-l", "-t", pane, text).Run(); err != nil {
		return fmt.Errorf("gemini: send-keys text: %w", err)
	}
	if err := exec.Command("tmux", "send-keys", "-t", pane, "Enter").Run(); err != nil {
		return fmt.Errorf("gemini: send-keys enter: %w", err)
	}
	return nil
}

// Abort delivers the orch.signal.interrupt verb (see docs/orch-signals.md)
// to the bound surface via shim.LocatorInterrupt — engine-aware
// (tmux C-c / cmux ctrl+c / zmx 0x03).
//
// No-op when Pane is empty (test harnesses occasionally construct the
// adapter without a pane to exercise the marker loops).
func (a *Adapter) Abort(_ context.Context) error {
	if a.Pane == "" {
		return nil
	}
	loc := a.Locator
	if !loc.IsValid() {
		loc = shim.Locator{Engine: shim.EngineTmux, Value: a.Pane}
	}
	return shim.LocatorInterrupt(loc)
}

// Compile-time check that the Adapter satisfies shim.Adapter and shim.Aborter.
var (
	_ shim.Adapter = (*Adapter)(nil)
	_ shim.Aborter = (*Adapter)(nil)
)
