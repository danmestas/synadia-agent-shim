package gemini

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/synadia-agent-shim/shim"
)

// -----------------------------------------------------------------------------
// Test helpers: send-keys recorder + adapter factory.
// -----------------------------------------------------------------------------

// sendKeysRecorder is the test seam — captures every "send" call so
// assertions can inspect what would have been delivered to tmux.
type sendKeysRecorder struct {
	mu    sync.Mutex
	calls []sendKeysCall
}

type sendKeysCall struct{ Pane, Text string }

func (r *sendKeysRecorder) fn(pane, text string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, sendKeysCall{Pane: pane, Text: text})
	return nil
}

func (r *sendKeysRecorder) snapshot() []sendKeysCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]sendKeysCall, len(r.calls))
	copy(out, r.calls)
	return out
}

func newTestAdapter(t *testing.T) (*Adapter, *sendKeysRecorder) {
	t.Helper()
	rec := &sendKeysRecorder{}
	a := New("%42")
	a.SendKeys = rec.fn
	a.StopMarkerDir = t.TempDir()
	a.NotifyMarkerDir = t.TempDir()
	a.GeminiChatsDir = t.TempDir()
	return a, rec
}

// drain reads up to `max` chunks until either max is reached or `timeout`
// elapses. Returns whatever was collected.
func drain(ch <-chan shim.Chunk, max int, timeout time.Duration) []shim.Chunk {
	out := make([]shim.Chunk, 0, max)
	deadline := time.After(timeout)
	for len(out) < max {
		select {
		case c, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, c)
		case <-deadline:
			return out
		}
	}
	return out
}

// appendLine atomically appends one JSONL line to `path`, creating the
// parent directory if needed.
func appendLine(t *testing.T, path, line string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(line + "\n"); err != nil {
		t.Fatal(err)
	}
}

// receiveChunk blocks until a chunk arrives or the timeout fires.
func receiveChunk(t *testing.T, ch <-chan shim.Chunk, timeout time.Duration) shim.Chunk {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(timeout):
		t.Fatal("timeout waiting for chunk")
	}
	return shim.Chunk{}
}

// atomicWrite simulates the hook scripts' tmpfile-then-rename pattern,
// which surfaces as a single CREATE event on the destination path.
func atomicWrite(t *testing.T, path, content string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// -----------------------------------------------------------------------------
// Tests.
// -----------------------------------------------------------------------------

// TestAdapter_OnPrompt_SendsViaRecorder verifies the send-keys path.
func TestAdapter_OnPrompt_SendsViaRecorder(t *testing.T) {
	a, rec := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	promptCtx, promptCancel := context.WithCancel(context.Background())
	defer promptCancel()
	if err := a.OnPrompt(promptCtx, "hello gemini"); err != nil {
		t.Fatalf("OnPrompt: %v", err)
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 send-keys call, got %d", len(calls))
	}
	if calls[0].Pane != "%42" || calls[0].Text != "hello gemini" {
		t.Errorf("call mismatch: %+v", calls[0])
	}
}

// TestAdapter_AfterAgent_EmitsTerminator asserts that the stop marker
// (written by the AfterAgent hook) produces a Terminator chunk.
// IMPORTANT: gemini-cli uses "AfterAgent" NOT "Stop" — this test is the
// canonical regression guard for that quirk.
func TestAdapter_AfterAgent_EmitsTerminator(t *testing.T) {
	a, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Settle so fsnotify registers the directory watch.
	time.Sleep(50 * time.Millisecond)

	// Simulate the AfterAgent hook writing the stop marker.
	marker := filepath.Join(a.stopDir(), "%42.event")
	atomicWrite(t, marker, `{"event":"AfterAgent","harness":"gemini"}`)

	c := receiveChunk(t, a.Events(), 1*time.Second)
	if !c.Terminator {
		t.Errorf("AfterAgent: expected Terminator chunk, got %+v", c)
	}
}

// TestAdapter_Notification_EmitsQueryChunk verifies that the native
// Notification marker produces a Query chunk. Unlike codex/pi, gemini
// has a first-class Notification event, so no synthetic detection is
// needed — the hook writes the marker directly and the adapter emits
// the query chunk unconditionally.
func TestAdapter_Notification_EmitsQueryChunk(t *testing.T) {
	a, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Simulate the Notification hook writing the notify marker.
	marker := filepath.Join(a.notifyDir(), "%42.notify")
	atomicWrite(t, marker, "Gemini is waiting for your input")

	c := receiveChunk(t, a.Events(), 1*time.Second)
	if c.Type != shim.ChunkQuery {
		t.Fatalf("Notification: expected ChunkQuery, got type=%v", c.Type)
	}
	q, ok := c.Data.(shim.QueryData)
	if !ok {
		t.Fatalf("Notification: expected QueryData payload, got %T", c.Data)
	}
	if q.Prompt != "Gemini is waiting for your input" {
		t.Errorf("query prompt: got %q", q.Prompt)
	}
	if q.ID == "" {
		t.Error("query id should be non-empty")
	}
}

// TestAdapter_WatchersSurvivePromptCtxCancel verifies that cancelling
// the per-prompt context does NOT tear down the marker watcher — the
// watcher is bound to the shim's lifetime context, not the prompt's.
// This catches the regression where startWatcher was called with
// OnPrompt's ctx, dismantling the adapter after the first turn.
func TestAdapter_WatchersSurvivePromptCtxCancel(t *testing.T) {
	a, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	promptCtx, promptCancel := context.WithCancel(context.Background())
	if err := a.OnPrompt(promptCtx, "first"); err != nil {
		t.Fatalf("OnPrompt: %v", err)
	}
	promptCancel() // simulate end-of-turn cleanup
	// Allow any (incorrect) goroutine teardown to complete.
	time.Sleep(60 * time.Millisecond)

	// The marker watcher must still be live after the prompt ctx cancel.
	marker := filepath.Join(a.stopDir(), "%42.event")
	atomicWrite(t, marker, `{"event":"AfterAgent"}`)

	c := receiveChunk(t, a.Events(), 1*time.Second)
	if !c.Terminator {
		t.Errorf("watcher torn down after prompt ctx cancel: got %+v", c)
	}
}

// TestAdapter_Close_Idempotent verifies that calling Close twice does
// not panic and that the events channel is closed afterward.
func TestAdapter_Close_Idempotent(t *testing.T) {
	a, _ := newTestAdapter(t)
	if err := a.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// Events channel MUST be closed so the shim's eventPump exits.
	select {
	case _, ok := <-a.Events():
		if ok {
			t.Error("expected closed events channel after Close")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Events() did not close after Close()")
	}
}

// TestAdapter_Transcript_EmitsResponsePerModelLine asserts that lines
// with role:"model" produce one ChunkResponse per text part, and that
// role:"user" lines are ignored.
func TestAdapter_Transcript_EmitsResponsePerModelLine(t *testing.T) {
	a, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Gemini buckets chats under an opaque <scope>/chats/ subdirectory;
	// the tailer walks recursively, so any depth under chatsDir works.
	session := filepath.Join(a.GeminiChatsDir, "scope-x", "chats", "session-1.jsonl")
	lines := []string{
		`{"role":"model","parts":[{"text":"first reply"}]}`,
		`{"role":"user","parts":[{"text":"should be ignored"}]}`,
		`{"role":"model","parts":[{"text":"second"},{"text":"third"}]}`,
	}
	for _, line := range lines {
		appendLine(t, session, line)
	}

	// 3 chunks total: first reply, second, third.
	got := drain(a.Events(), 3, 2*time.Second)
	if len(got) != 3 {
		t.Fatalf("expected exactly 3 chunks, got %d: %+v", len(got), got)
	}
	want := []string{"first reply", "second", "third"}
	for i, w := range want {
		if got[i].Type != shim.ChunkResponse {
			t.Errorf("chunk %d: type got %q want response", i, got[i].Type)
		}
		if s, ok := got[i].Data.(string); !ok || s != w {
			t.Errorf("chunk %d: data got %v want %q", i, got[i].Data, w)
		}
	}
}

// TestAdapter_Transcript_FunctionCallEmitsToolUse covers the
// functionCall part shape — gemini-cli emits these for tool invocations.
func TestAdapter_Transcript_FunctionCallEmitsToolUse(t *testing.T) {
	a, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	session := filepath.Join(a.GeminiChatsDir, "s", "chats", "session-2.jsonl")
	appendLine(t, session,
		`{"role":"model","parts":[{"functionCall":{"name":"ls","args":{"path":"/tmp"}}}]}`)

	c := receiveChunk(t, a.Events(), 1*time.Second)
	if c.Type != shim.ChunkToolUse {
		t.Fatalf("expected ChunkToolUse, got %v", c.Type)
	}
	m, ok := c.Data.(map[string]any)
	if !ok || m["name"] != "ls" {
		t.Errorf("tool_use payload: got %+v", c.Data)
	}
}

// TestAdapter_Transcript_ToleratesUnknownFields verifies that unknown
// JSON fields are silently dropped (forward-compat per §6.6 / §12).
func TestAdapter_Transcript_ToleratesUnknownFields(t *testing.T) {
	a, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	session := filepath.Join(a.GeminiChatsDir, "scope", "chats", "session-3.jsonl")
	appendLine(t, session,
		`{"role":"model","unknown":"x","parts":[{"text":"ok","extra":true}]}`)

	c := receiveChunk(t, a.Events(), 1*time.Second)
	if c.Type != shim.ChunkResponse || c.Data.(string) != "ok" {
		t.Errorf("expected response 'ok', got %+v", c)
	}
}

// TestFindLatestGeminiSession_RecursiveByMTime verifies the recursive
// directory walk picks the newest session-*.jsonl across nested dirs.
func TestFindLatestGeminiSession_RecursiveByMTime(t *testing.T) {
	root := t.TempDir()
	older := filepath.Join(root, "a", "chats", "session-100.jsonl")
	newer := filepath.Join(root, "b", "chats", "session-200.jsonl")
	noise := filepath.Join(root, "c", "chats", "other.jsonl") // wrong prefix
	for _, p := range []string{older, newer, noise} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("{}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Force `newer` to have a later mtime than `older`.
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(older, past, past); err != nil {
		t.Fatal(err)
	}
	if got := findLatestGeminiSession(root); got != newer {
		t.Errorf("findLatestGeminiSession: got %q want %q", got, newer)
	}
}

// TestFindLatestGeminiSession_EmptyDirReturnsEmpty guards the
// no-chats-yet startup window.
func TestFindLatestGeminiSession_EmptyDirReturnsEmpty(t *testing.T) {
	if got := findLatestGeminiSession(t.TempDir()); got != "" {
		t.Errorf("expected empty for empty chats dir, got %q", got)
	}
}

// TestAdapter_EmptyNotifyMarker_Skipped verifies that an empty notify
// marker file does not produce a chunk (defensive: hook write failure).
func TestAdapter_EmptyNotifyMarker_Skipped(t *testing.T) {
	a, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	marker := filepath.Join(a.notifyDir(), "%42.notify")
	atomicWrite(t, marker, "")

	// No chunk should arrive within a short window.
	select {
	case c := <-a.Events():
		t.Errorf("expected no chunk for empty notify marker, got %+v", c)
	case <-time.After(200 * time.Millisecond):
		// Correct: nothing emitted.
	}
}
