package claudecode

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
// Unit-ish tests: encoding + small helpers.
// -----------------------------------------------------------------------------

func TestEncodeProjectPath(t *testing.T) {
	// claude-code encodes the cwd by replacing both `/` AND `.` with `-`.
	// This is verified against ~/.claude/projects on a real install.
	cases := []struct{ in, want string }{
		{"/Users/dan/projects/orch", "-Users-dan-projects-orch"},
		{"/tmp/worktrees/orch-56", "-tmp-worktrees-orch-56"},
		{"/var/lib/foo.bar", "-var-lib-foo-bar"},
	}
	for _, c := range cases {
		if got := encodeProjectPath(c.in); got != c.want {
			t.Errorf("encodeProjectPath(%q): got %q want %q", c.in, got, c.want)
		}
	}
}

// TestResolveProjectDir_ResolvesSymlinks reproduces issue #15: when the
// pane's cwd traverses a symlink (e.g. on macOS, /tmp → /private/tmp),
// the adapter must encode the symlink-resolved path — otherwise it
// watches a directory claude-code never writes to.
//
// We construct a real symlink inside the test tempdir, point cwd at it,
// and assert resolveProjectDir returns the realpath (which differs from
// the symlinked input).
func TestResolveProjectDir_ResolvesSymlinks(t *testing.T) {
	tmp := t.TempDir()
	realDir := filepath.Join(tmp, "real")
	if err := os.MkdirAll(realDir, 0o755); err != nil {
		t.Fatal(err)
	}
	linkDir := filepath.Join(tmp, "link")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Skipf("symlink not supported on this platform: %v", err)
	}

	// On macOS the TempDir itself may live under /var, which is a symlink
	// to /private/var. EvalSymlinks resolves the entire chain — compare
	// against the canonical form of the target rather than against
	// realDir's literal value.
	wantCanonical, err := filepath.EvalSymlinks(realDir)
	if err != nil {
		t.Fatalf("EvalSymlinks(realDir): %v", err)
	}

	got := resolveProjectDir(linkDir)
	if got != wantCanonical {
		t.Errorf("resolveProjectDir(symlink): got %q want %q", got, wantCanonical)
	}

	// Sanity-check the regression we're guarding against: feeding the
	// symlinked path straight into encodeProjectPath produces a DIFFERENT
	// encoded result than feeding the resolved path. If these two ever
	// became equal the bug-shape wouldn't reproduce and the regression
	// test would silently pass.
	if encodeProjectPath(linkDir) == encodeProjectPath(wantCanonical) {
		t.Skip("symlink resolves to identical path; cannot exercise mismatch on this platform")
	}
	if encodeProjectPath(resolveProjectDir(linkDir)) != encodeProjectPath(wantCanonical) {
		t.Errorf("resolveProjectDir+encodeProjectPath did not match canonical encoding")
	}
}

// TestResolveProjectDir_FallsBackOnMissingPath confirms the documented
// fallback: if EvalSymlinks errors (path doesn't exist yet), the literal
// cwd is returned so the tail poller can reconcile once the directory
// appears.
func TestResolveProjectDir_FallsBackOnMissingPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist-yet")
	got := resolveProjectDir(missing)
	if got != missing {
		t.Errorf("resolveProjectDir on missing path: got %q want %q (literal fallback)", got, missing)
	}
}

// TestEncodeProjectPath_StraightPath confirms no regression on the
// no-symlink case — paths that are already canonical encode identically
// before and after the resolveProjectDir hop.
func TestEncodeProjectPath_StraightPath(t *testing.T) {
	dir := t.TempDir()
	// On platforms where t.TempDir() returns a symlinked path (notably
	// macOS where /tmp/* aliases /private/tmp/*), normalise once so
	// the round-trip below is exercised against a canonical input.
	canonical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	if encodeProjectPath(canonical) != encodeProjectPath(resolveProjectDir(canonical)) {
		t.Errorf("encodeProjectPath round-trip diverged on canonical input %q", canonical)
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

func newTestAdapter(t *testing.T) (*Adapter, *sendKeysRecorder, string) {
	t.Helper()
	rec := &sendKeysRecorder{}
	stopDir := t.TempDir()
	notifyDir := t.TempDir()
	projectsDir := t.TempDir()
	cwd := "/tmp/test-cwd"
	a := New("%42", cwd)
	a.SendKeys = rec.fn
	a.StopMarkerDir = stopDir
	a.NotifyMarkerDir = notifyDir
	a.ClaudeProjectsDir = projectsDir
	return a, rec, projectsDir
}

func TestAdapter_OnPrompt_SendsViaRecorder(t *testing.T) {
	a, rec, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
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

// Watchers MUST survive cancellation of OnPrompt's ctx — that ctx
// represents a single turn, not the adapter's lifetime. This test
// catches the regression where startWatchers bound to OnPrompt's ctx
// and the first prompt's cleanup tore down the adapter permanently.
func TestAdapter_WatchersSurvivePromptCtxCancel(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
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
	// should still produce a Terminator chunk.
	marker := filepath.Join(a.stopDir(), "%42.event")
	tmp := marker + ".tmp"
	if err := os.WriteFile(tmp, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, marker); err != nil {
		t.Fatal(err)
	}
	c := receiveChunk(t, a.Events(), 1*time.Second)
	if !c.Terminator {
		t.Errorf("watcher torn down after prompt ctx cancel: got %+v", c)
	}
}

// Close MUST be idempotent — called twice should not panic.
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

func TestAdapter_StopMarker_EmitsTerminator(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Settle for fsnotify to register the watch.
	time.Sleep(50 * time.Millisecond)

	// Atomically write the stop marker (the orch-stop-marker.sh hook
	// does a tmpfile-then-rename which surfaces as CREATE on the
	// destination — we simulate the rename path).
	marker := filepath.Join(a.stopDir(), "%42.event")
	tmp := marker + ".tmp"
	if err := os.WriteFile(tmp, []byte(`{"transcript":"x"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, marker); err != nil {
		t.Fatal(err)
	}

	c := receiveChunk(t, a.Events(), 1*time.Second)
	if !c.Terminator {
		t.Errorf("expected terminator chunk, got %+v", c)
	}
}

func TestAdapter_NotifyMarker_EmitsQueryChunk(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	marker := filepath.Join(a.notifyDir(), "%42.notify")
	tmp := marker + ".tmp"
	if err := os.WriteFile(tmp, []byte("Claude is waiting for input"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, marker); err != nil {
		t.Fatal(err)
	}

	c := receiveChunk(t, a.Events(), 1*time.Second)
	if c.Type != shim.ChunkQuery {
		t.Fatalf("expected query chunk, got type=%v", c.Type)
	}
	q, ok := c.Data.(shim.QueryData)
	if !ok {
		t.Fatalf("expected QueryData payload, got %T", c.Data)
	}
	if q.Prompt != "Claude is waiting for input" {
		t.Errorf("query prompt: got %q", q.Prompt)
	}
	if q.ID == "" {
		t.Error("query id should be non-empty")
	}
}

func TestAdapter_Transcript_EmitsResponseChunksPerTextBlock(t *testing.T) {
	a, _, projectsDir := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := a.OnPrompt(context.Background(), "trigger"); err != nil {
		t.Fatalf("OnPrompt: %v", err)
	}

	// Create the project directory that claude would create on first turn.
	encoded := encodeProjectPath(a.CWD)
	projDir := filepath.Join(projectsDir, encoded)
	if err := os.MkdirAll(projDir, 0o755); err != nil {
		t.Fatal(err)
	}
	transcript := filepath.Join(projDir, "session-abc.jsonl")

	// Two assistant lines, mixing block types. The third line is a
	// user-side entry that MUST NOT produce a chunk.
	lines := []string{
		`{"type":"assistant","message":{"content":[{"type":"text","text":"first reply"}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"thinking","text":"hmm"},{"type":"text","text":"second"}]}}`,
		`{"type":"user","message":{"content":[{"type":"text","text":"should be ignored"}]}}`,
	}
	for _, line := range lines {
		appendLine(t, transcript, line)
	}

	// Allow the 250ms tail poller to pick up the file. We expect three
	// chunks total: response("first reply"), thinking("hmm"), response("second").
	// User-side entries MUST NOT yield chunks.
	got := drain(a.Events(), 3, 2*time.Second)
	if len(got) != 3 {
		t.Fatalf("expected exactly 3 chunks, got %d: %+v", len(got), got)
	}

	// Assert publication order matches block order in the transcript:
	// line 1 → response, line 2 → thinking then response.
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

func TestAdapter_Transcript_ToleratesUnknownFields(t *testing.T) {
	// §6.6 / §12: forward-compat means unknown fields are silently
	// ignored. encoding/json drops them; we assert chunks still emit.
	a, _, projectsDir := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	projDir := filepath.Join(projectsDir, encodeProjectPath(a.CWD))
	_ = os.MkdirAll(projDir, 0o755)
	appendLine(t, filepath.Join(projDir, "x.jsonl"),
		`{"type":"assistant","extra":"new-field","message":{"content":[{"type":"text","text":"ok","new_block_field":true}]}}`)

	c := receiveChunk(t, a.Events(), 1*time.Second)
	if c.Type != shim.ChunkResponse || c.Data.(string) != "ok" {
		t.Errorf("expected response 'ok', got %+v", c)
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
// expires. Used when we don't know the exact count (e.g. transcript with
// block-level chunking).
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
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(line + "\n"); err != nil {
		t.Fatal(err)
	}
}

// Compile-time check that the Adapter satisfies shim.Adapter.
var _ shim.Adapter = (*Adapter)(nil)
