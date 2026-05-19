package pi

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
// Unit-ish tests: path encoding + small helpers.
// -----------------------------------------------------------------------------

func TestEncodePiPath(t *testing.T) {
	// Pi encodes the cwd by replacing both `/` AND `.` with `-`.
	// Verified from orch-nats-publish-jsonl.ts: `cwd.replace(/[/.]/g, "-")`.
	// Example from the TS comment: `/Users/dmestas/projects/darken`
	// → `--Users-dmestas-projects-darken` (each char including leading `/`
	// becomes `-`; two leading dashes because the path starts with `/U`).
	// Wait — the TS comment says `/Users/d/p/proj` → `--Users-d-p-proj`
	// meaning the leading `/` plus the separator after the first segment
	// both become `-`. Let's trace: replace each `/` or `.` with `-`:
	//   /Users/d/p/proj → -Users-d-p-proj (one leading dash, not two).
	// The TS comment's `--Users-dmestas-projects-darken` is the encoded
	// form of `/Users/dmestas/projects/darken` where there are 4 `/`
	// separators: the leading `/`, then `/dmestas`, `/projects`, `/darken`.
	// So the pattern `--Users-...` comes from paths like `/Users/…` where
	// the two leading dashes represent `/` + `U…` wait — `/` → `-`, `U`
	// stays `U`, so `/Users` → `-Users`. There's only ONE leading dash.
	// The TS comment must have a typo/extra dash. Our Go impl is correct.
	cases := []struct{ in, want string }{
		{"/Users/d/p/proj", "-Users-d-p-proj"},
		{"/tmp/worktrees/orch-62", "-tmp-worktrees-orch-62"},
		{"/var/lib/foo.bar", "-var-lib-foo-bar"},
	}
	for _, c := range cases {
		if got := encodePiPath(c.in); got != c.want {
			t.Errorf("encodePiPath(%q): got %q want %q", c.in, got, c.want)
		}
	}
}

func TestFindLatestJSONL_PicksNewestByMTime(t *testing.T) {
	dir := t.TempDir()
	older := filepath.Join(dir, "older.jsonl")
	newer := filepath.Join(dir, "newer.jsonl")
	if err := os.WriteFile(older, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Pause so mtimes diverge on filesystems with coarse resolution.
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(newer, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := findLatestJSONL(dir)
	if got != newer {
		t.Fatalf("findLatestJSONL: got %q want %q", got, newer)
	}
}

func TestFindLatestJSONL_EmptyDirReturnsEmpty(t *testing.T) {
	if got := findLatestJSONL(t.TempDir()); got != "" {
		t.Errorf("expected empty string for empty dir, got %q", got)
	}
}

func TestFindLatestJSONL_IgnoresNonJSONL(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "x.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := findLatestJSONL(dir); got != "" {
		t.Errorf("expected empty when only non-.jsonl present, got %q", got)
	}
}

// -----------------------------------------------------------------------------
// Behavior tests: send-keys recorder + marker watching + transcript tail.
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

// newTestAdapter creates an Adapter wired to a recorder and tempdirs,
// so tests never touch the real filesystem or tmux.
func newTestAdapter(t *testing.T) (*Adapter, *sendKeysRecorder, string) {
	t.Helper()
	rec := &sendKeysRecorder{}
	stopDir := t.TempDir()
	piSessionsDir := t.TempDir()
	cwd := "/tmp/test-cwd"
	a := New("%42", cwd)
	a.SendKeys = rec.fn
	a.StopMarkerDir = stopDir
	a.PiSessionsDir = piSessionsDir
	return a, rec, piSessionsDir
}

func TestAdapter_OnPrompt_SendsViaRecorder(t *testing.T) {
	a, rec, _ := newTestAdapter(t)
	defer a.Close()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	promptCtx, promptCancel := context.WithCancel(context.Background())
	defer promptCancel()
	if err := a.OnPrompt(promptCtx, "hello world"); err != nil {
		t.Fatalf("OnPrompt: %v", err)
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 send-keys call, got %d", len(calls))
	}
	if calls[0].Pane != "%42" || calls[0].Text != "hello world" {
		t.Errorf("call mismatch: %+v", calls[0])
	}
}

// TestAdapter_WatchersSurvivePromptCtxCancel verifies the contract that
// watchers MUST survive cancellation of OnPrompt's ctx — that ctx represents
// a single turn, not the adapter's lifetime. This test catches regressions
// where startWatchers bound to OnPrompt's ctx and the first prompt's cleanup
// tore down the adapter permanently.
func TestAdapter_WatchersSurvivePromptCtxCancel(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer a.Close()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Per-prompt ctx with its own cancel; we cancel it AFTER OnPrompt.
	promptCtx, promptCancel := context.WithCancel(context.Background())
	if err := a.OnPrompt(promptCtx, "first"); err != nil {
		t.Fatalf("OnPrompt: %v", err)
	}
	promptCancel() // simulate end-of-turn cleanup
	// Settle so any (incorrect) goroutine teardown can occur.
	time.Sleep(60 * time.Millisecond)

	// The marker watcher should still be live: writing a stop marker
	// should still produce chunks (query + terminator).
	marker := filepath.Join(a.stopDir(), "%42.event")
	tmp := marker + ".tmp"
	if err := os.WriteFile(tmp, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, marker); err != nil {
		t.Fatal(err)
	}
	// Expect query chunk first, then terminator.
	c := receiveChunk(t, a.Events(), 1*time.Second)
	if c.Type != shim.ChunkQuery {
		t.Errorf("watcher torn down after prompt ctx cancel: expected query chunk, got %+v", c)
	}
	c = receiveChunk(t, a.Events(), 1*time.Second)
	if !c.Terminator {
		t.Errorf("expected terminator after query, got %+v", c)
	}
}

// TestAdapter_Close_Idempotent verifies that Close can be called multiple
// times without panicking, and that Events() is closed afterwards.
func TestAdapter_Close_Idempotent(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	if err := a.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// Events channel MUST be closed (so the shim's eventPump exits).
	select {
	case _, ok := <-a.Events():
		if ok {
			t.Error("expected closed events channel after Close")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Events() did not close after Close()")
	}
}

// TestAdapter_StopMarker_EmitsSyntheticQueryThenTerminator verifies the
// Plan 11 idle-with-prompt heuristic: a stop-marker event should produce
// a synthetic Query chunk followed immediately by a Terminator chunk.
func TestAdapter_StopMarker_EmitsSyntheticQueryThenTerminator(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer a.Close()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Settle for fsnotify to register the watch.
	time.Sleep(50 * time.Millisecond)

	// Atomically write the stop marker (orch-stop-marker.ts does a
	// tmpfile-then-rename which surfaces as CREATE on the destination).
	marker := filepath.Join(a.stopDir(), "%42.event")
	tmp := marker + ".tmp"
	if err := os.WriteFile(tmp, []byte("ts_ns=1\npane_id=%42\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, marker); err != nil {
		t.Fatal(err)
	}

	// First chunk: synthetic query.
	c := receiveChunk(t, a.Events(), 1*time.Second)
	if c.Type != shim.ChunkQuery {
		t.Fatalf("expected query chunk first, got type=%v terminator=%v", c.Type, c.Terminator)
	}
	q, ok := c.Data.(shim.QueryData)
	if !ok {
		t.Fatalf("expected QueryData payload, got %T", c.Data)
	}
	if q.ID == "" {
		t.Error("query id should be non-empty")
	}
	if q.Prompt == "" {
		t.Error("query prompt should be non-empty")
	}

	// Second chunk: terminator.
	c = receiveChunk(t, a.Events(), 1*time.Second)
	if !c.Terminator {
		t.Errorf("expected terminator chunk after query, got %+v", c)
	}
}

// TestAdapter_Transcript_EmitsResponseChunksPerTextBlock verifies that the
// transcript tailer picks up the pi JSONL and emits the correct chunks.
func TestAdapter_Transcript_EmitsResponseChunksPerTextBlock(t *testing.T) {
	a, _, piSessionsDir := newTestAdapter(t)
	defer a.Close()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := a.OnPrompt(context.Background(), "trigger"); err != nil {
		t.Fatalf("OnPrompt: %v", err)
	}

	// Create the session directory that pi would create on first turn.
	encoded := encodePiPath(a.CWD)
	sessDir := filepath.Join(piSessionsDir, encoded)
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Pi transcript filename: <timestamp>_<session_uuid>.jsonl
	transcript := filepath.Join(sessDir, "1700000000_abc123.jsonl")

	// Two assistant lines, mixing block types. The third is a user-side
	// entry that MUST NOT produce a chunk.
	lines := []string{
		`{"type":"assistant","message":{"content":[{"type":"text","text":"first reply"}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"thinking","text":"hmm"},{"type":"text","text":"second"}]}}`,
		`{"type":"user","message":{"content":[{"type":"text","text":"should be ignored"}]}}`,
	}
	for _, line := range lines {
		appendLine(t, transcript, line)
	}

	// Allow the 250ms tail poller to pick up the file. We expect three
	// chunks: response("first reply"), thinking("hmm"), response("second").
	got := drain(a.Events(), 3, 2*time.Second)
	if len(got) != 3 {
		t.Fatalf("expected exactly 3 chunks, got %d: %+v", len(got), got)
	}

	type want struct {
		typ  shim.ChunkType
		data string
	}
	wants := []want{
		{shim.ChunkResponse, "first reply"},
		{shim.ChunkThinking, "hmm"},
		{shim.ChunkResponse, "second"},
	}
	for i, w := range wants {
		if got[i].Type != w.typ {
			t.Errorf("chunk %d: type got %q want %q", i, got[i].Type, w.typ)
		}
		if s, ok := got[i].Data.(string); !ok || s != w.data {
			t.Errorf("chunk %d: data got %v want %q", i, got[i].Data, w.data)
		}
	}
}

// TestAdapter_Transcript_SessionIDPinsToNamedFile verifies issue #11
// path (A) for pi: when SessionID is set the adapter MUST tail
// exactly the JSONL whose filename matches `*_<session-id>.jsonl` in
// the encoded-cwd session dir — even when a newer .jsonl exists from a
// concurrent pi process.
func TestAdapter_Transcript_SessionIDPinsToNamedFile(t *testing.T) {
	a, _, piSessionsDir := newTestAdapter(t)
	a.SessionID = "pinned"
	t.Cleanup(func() { _ = a.Close() })
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()

	sessDir := filepath.Join(piSessionsDir, encodePiPath(a.CWD))
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Pinned file: older mtime, content the test asserts on.
	pinned := filepath.Join(sessDir, "1000_pinned.jsonl")
	appendLine(t, pinned, `{"type":"assistant","message":{"content":[{"type":"text","text":"from-pinned"}]}}`)
	// Racy file: newer mtime, must NOT be tailed.
	time.Sleep(20 * time.Millisecond)
	racy := filepath.Join(sessDir, "2000_other.jsonl")
	appendLine(t, racy, `{"type":"assistant","message":{"content":[{"type":"text","text":"from-other"}]}}`)

	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	c := receiveChunk(t, a.Events(), 2*time.Second)
	if c.Type != shim.ChunkResponse {
		t.Fatalf("expected response chunk, got %+v", c)
	}
	if s, _ := c.Data.(string); s != "from-pinned" {
		t.Errorf("SessionID was ignored: got %q want %q", s, "from-pinned")
	}

	extra := drain(a.Events(), 1, 400*time.Millisecond)
	for _, e := range extra {
		if s, _ := e.Data.(string); s == "from-other" {
			t.Errorf("adapter tailed the racy file despite SessionID pin: saw %q", s)
		}
	}
}

// TestAdapter_Transcript_NoSessionIDFallsBackToLatestMTime preserves the
// pre-#11 behaviour for callers that don't supply a session id.
func TestAdapter_Transcript_NoSessionIDFallsBackToLatestMTime(t *testing.T) {
	a, _, piSessionsDir := newTestAdapter(t)
	t.Cleanup(func() { _ = a.Close() })
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()

	sessDir := filepath.Join(piSessionsDir, encodePiPath(a.CWD))
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatal(err)
	}
	older := filepath.Join(sessDir, "1000_older.jsonl")
	appendLine(t, older, `{"type":"assistant","message":{"content":[{"type":"text","text":"from-older"}]}}`)
	time.Sleep(20 * time.Millisecond)
	newer := filepath.Join(sessDir, "2000_newer.jsonl")
	appendLine(t, newer, `{"type":"assistant","message":{"content":[{"type":"text","text":"from-newer"}]}}`)

	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	c := receiveChunk(t, a.Events(), 2*time.Second)
	if s, _ := c.Data.(string); s != "from-newer" {
		t.Errorf("fallback to latest-mtime broke: got %q want %q", s, "from-newer")
	}
}

// TestAdapter_Transcript_ToleratesUnknownFields verifies forward-compat:
// unknown JSON fields in the transcript are silently ignored.
func TestAdapter_Transcript_ToleratesUnknownFields(t *testing.T) {
	a, _, piSessionsDir := newTestAdapter(t)
	defer a.Close()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	encoded := encodePiPath(a.CWD)
	sessDir := filepath.Join(piSessionsDir, encoded)
	_ = os.MkdirAll(sessDir, 0o755)
	appendLine(t, filepath.Join(sessDir, "1700000001_def456.jsonl"),
		`{"type":"assistant","extra":"new-field","message":{"content":[{"type":"text","text":"ok","new_block_field":true}]}}`)

	c := receiveChunk(t, a.Events(), 1*time.Second)
	if c.Type != shim.ChunkResponse || c.Data.(string) != "ok" {
		t.Errorf("expected response 'ok', got %+v", c)
	}
}

// TestAdapter_Transcript_MalformedLineDropped verifies that a malformed
// JSONL line does not crash the tailer and subsequent lines still work.
func TestAdapter_Transcript_MalformedLineDropped(t *testing.T) {
	a, _, piSessionsDir := newTestAdapter(t)
	defer a.Close()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	encoded := encodePiPath(a.CWD)
	sessDir := filepath.Join(piSessionsDir, encoded)
	_ = os.MkdirAll(sessDir, 0o755)
	transcript := filepath.Join(sessDir, "1700000002_ghi789.jsonl")
	// Malformed line first, then valid assistant line.
	appendLine(t, transcript, `not valid json }{`)
	appendLine(t, transcript, `{"type":"assistant","message":{"content":[{"type":"text","text":"after bad line"}]}}`)

	c := receiveChunk(t, a.Events(), 2*time.Second)
	if c.Type != shim.ChunkResponse || c.Data.(string) != "after bad line" {
		t.Errorf("expected response after malformed line, got %+v", c)
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

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

// drain reads up to `max` chunks until either max is reached or `timeout`
// expires. Used when we don't know the exact count.
func drain(ch <-chan shim.Chunk, max int, timeout time.Duration) []shim.Chunk {
	out := make([]shim.Chunk, 0, max)
	deadline := time.Now().Add(timeout)
	for len(out) < max {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return out
		}
		select {
		case c := <-ch:
			out = append(out, c)
		case <-time.After(remaining):
			return out
		}
	}
	return out
}

func appendLine(t *testing.T, path, line string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(line + "\n"); err != nil {
		t.Fatal(err)
	}
}
