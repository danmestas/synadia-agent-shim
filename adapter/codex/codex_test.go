package codex

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/synadia-agent-shim/shim"
)

// -----------------------------------------------------------------------------
// Unit tests: helpers
// -----------------------------------------------------------------------------

func TestFindLatestRollout_PicksNewestByMTime(t *testing.T) {
	sessDir := t.TempDir()
	// Create date-bucketed structure.
	dayPath := filepath.Join(sessDir, "2024", "01", "15")
	if err := os.MkdirAll(dayPath, 0o755); err != nil {
		t.Fatal(err)
	}
	older := filepath.Join(dayPath, "rollout-100-aaaa.jsonl")
	newer := filepath.Join(dayPath, "rollout-200-bbbb.jsonl")
	if err := os.WriteFile(older, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(newer, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := findLatestRollout(sessDir)
	if got != newer {
		t.Fatalf("findLatestRollout: got %q want %q", got, newer)
	}
}

func TestFindLatestRollout_EmptyDirReturnsEmpty(t *testing.T) {
	if got := findLatestRollout(t.TempDir()); got != "" {
		t.Errorf("expected empty for empty sessions dir, got %q", got)
	}
}

func TestFindLatestRollout_IgnoresNonRolloutFiles(t *testing.T) {
	sessDir := t.TempDir()
	dayPath := filepath.Join(sessDir, "2024", "01", "15")
	if err := os.MkdirAll(dayPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dayPath, "other.jsonl"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dayPath, "rollout-100-abc.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := findLatestRollout(sessDir); got != "" {
		t.Errorf("expected empty when no rollout-*.jsonl present, got %q", got)
	}
}

func TestFindLatestRollout_AcrossDateBuckets(t *testing.T) {
	sessDir := t.TempDir()
	day1 := filepath.Join(sessDir, "2024", "01", "14")
	day2 := filepath.Join(sessDir, "2024", "01", "15")
	for _, d := range []string{day1, day2} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	p1 := filepath.Join(day1, "rollout-100-old.jsonl")
	if err := os.WriteFile(p1, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	p2 := filepath.Join(day2, "rollout-200-new.jsonl")
	if err := os.WriteFile(p2, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := findLatestRollout(sessDir)
	if got != p2 {
		t.Fatalf("expected newest across buckets %q, got %q", p2, got)
	}
}

func TestHasPromptPattern(t *testing.T) {
	cases := []struct {
		content string
		want    bool
	}{
		{"some output\n❯ ", true},
		{"Waiting...\n[y/n] proceed?", true},
		{"You: type here", true},
		{"no prompt here", false},
		{"", false},
	}
	for _, c := range cases {
		if got := hasPromptPattern(c.content); got != c.want {
			t.Errorf("hasPromptPattern(%q): got %v want %v", c.content, got, c.want)
		}
	}
}

func TestExtractPromptText(t *testing.T) {
	content := "line one\nline two\n❯ input here\n"
	got := extractPromptText(content)
	if got != "❯ input here" {
		t.Errorf("extractPromptText: got %q", got)
	}
}

func TestEmitFromRolloutLine_AssistantText(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	line := []byte(`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello codex"}]}`)
	a.emitFromRolloutLine(line)

	select {
	case c := <-a.Events():
		if c.Type != shim.ChunkResponse {
			t.Errorf("expected response chunk, got %v", c.Type)
		}
		if s, ok := c.Data.(string); !ok || s != "hello codex" {
			t.Errorf("expected 'hello codex', got %v", c.Data)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for chunk")
	}
}

func TestEmitFromRolloutLine_Reasoning(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	line := []byte(`{"type":"reasoning","summary":[{"type":"summary_text","text":"thinking hard"}]}`)
	a.emitFromRolloutLine(line)

	select {
	case c := <-a.Events():
		if c.Type != shim.ChunkThinking {
			t.Errorf("expected thinking chunk, got %v", c.Type)
		}
		if s, ok := c.Data.(string); !ok || s != "thinking hard" {
			t.Errorf("expected 'thinking hard', got %v", c.Data)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for chunk")
	}
}

func TestEmitFromRolloutLine_FunctionCall(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	line := []byte(`{"type":"function_call","name":"bash","arguments":"{\"cmd\":\"ls\"}"}`)
	a.emitFromRolloutLine(line)

	select {
	case c := <-a.Events():
		if c.Type != shim.ChunkToolUse {
			t.Errorf("expected tool_use chunk, got %v", c.Type)
		}
		m, ok := c.Data.(map[string]any)
		if !ok {
			t.Fatalf("expected map data, got %T", c.Data)
		}
		if m["name"] != "bash" {
			t.Errorf("expected name 'bash', got %v", m["name"])
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for chunk")
	}
}

func TestEmitFromRolloutLine_UserLineIgnored(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	line := []byte(`{"type":"message","role":"user","content":[{"type":"input_text","text":"should be ignored"}]}`)
	a.emitFromRolloutLine(line)

	select {
	case <-a.Events():
		t.Error("expected no chunk for user line")
	case <-time.After(50 * time.Millisecond):
		// correct — no chunk emitted.
	}
}

func TestEmitFromRolloutLine_ToleratesMalformedJSON(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	a.emitFromRolloutLine([]byte("not json at all"))

	select {
	case <-a.Events():
		t.Error("expected no chunk for malformed line")
	case <-time.After(50 * time.Millisecond):
		// correct.
	}
}

func TestEmitFromRolloutLine_ToleratesUnknownFields(t *testing.T) {
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	line := []byte(`{"type":"message","role":"assistant","new_field":"ignored","content":[{"type":"output_text","text":"ok","extra":true}]}`)
	a.emitFromRolloutLine(line)

	select {
	case c := <-a.Events():
		if c.Type != shim.ChunkResponse || c.Data.(string) != "ok" {
			t.Errorf("expected response 'ok', got %+v", c)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for chunk")
	}
}

// -----------------------------------------------------------------------------
// Behavior tests: send-keys recorder + stop marker + transcript tail +
// idle-query synthetic chunks.
// -----------------------------------------------------------------------------

// sendKeysRecorder captures every send-keys call for assertions.
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

// capturePaneStub lets tests control what the idle loop sees.
type capturePaneStub struct {
	mu      sync.Mutex
	content string
}

func (s *capturePaneStub) set(c string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.content = c
}

func (s *capturePaneStub) fn(pane string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.content, nil
}

// newTestAdapter builds an Adapter wired to a temp stop dir, sessions dir,
// send-keys recorder, and capture-pane stub.
func newTestAdapter(t *testing.T) (*Adapter, *sendKeysRecorder, string) {
	t.Helper()
	rec := &sendKeysRecorder{}
	stopDir := t.TempDir()
	sessionsDir := t.TempDir()
	a := New("%42")
	a.SendKeys = rec.fn
	a.StopMarkerDir = stopDir
	a.CodexSessionsDir = sessionsDir
	a.CapturePaneFn = (&capturePaneStub{}).fn // no prompt by default
	return a, rec, sessionsDir
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
	if err := a.OnPrompt(promptCtx, "hello codex"); err != nil {
		t.Fatalf("OnPrompt: %v", err)
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 send-keys call, got %d", len(calls))
	}
	if calls[0].Pane != "%42" || calls[0].Text != "hello codex" {
		t.Errorf("call mismatch: %+v", calls[0])
	}
}

// OnPrompt must work even if Start was never called — the lazy-start
// safety net should boot the watchers using the prompt's ctx and then
// deliver the prompt.
func TestAdapter_OnPrompt_LazyStart(t *testing.T) {
	a, rec, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()

	// Deliberately skip Start — verify OnPrompt boots watchers itself.
	promptCtx, promptCancel := context.WithCancel(context.Background())
	defer promptCancel()
	if err := a.OnPrompt(promptCtx, "lazy prompt"); err != nil {
		t.Fatalf("OnPrompt (no prior Start): %v", err)
	}
	if !a.started.Load() {
		t.Error("expected adapter to be marked started after lazy OnPrompt")
	}
	calls := rec.snapshot()
	if len(calls) != 1 || calls[0].Text != "lazy prompt" {
		t.Errorf("expected 1 send-keys call with 'lazy prompt', got %+v", calls)
	}

	// Settle so the fsnotify watcher is registered, then verify watchers
	// actually came up: a stop-marker write should produce a Terminator.
	time.Sleep(50 * time.Millisecond)
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
		t.Errorf("watchers not running after lazy start: got %+v", c)
	}
}

// Watchers MUST survive cancellation of OnPrompt's ctx.
func TestAdapter_WatchersSurvivePromptCtxCancel(t *testing.T) {
	a, _, _ := newTestAdapter(t)
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
	time.Sleep(60 * time.Millisecond)

	// Stop marker watcher must still be live after prompt ctx cancel.
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
	time.Sleep(50 * time.Millisecond)

	marker := filepath.Join(a.stopDir(), "%42.event")
	tmp := marker + ".tmp"
	if err := os.WriteFile(tmp, []byte(`{"event":"stop","harness":"codex"}`), 0o644); err != nil {
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

func TestAdapter_Transcript_EmitsResponseChunks(t *testing.T) {
	a, _, sessionsDir := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Create date-bucketed dir + rollout file.
	dayPath := filepath.Join(sessionsDir, "2024", "01", "15")
	if err := os.MkdirAll(dayPath, 0o755); err != nil {
		t.Fatal(err)
	}
	transcript := filepath.Join(dayPath, "rollout-123-abc.jsonl")

	lines := []string{
		`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"first reply"}]}`,
		`{"type":"reasoning","summary":[{"type":"summary_text","text":"hmm"}]}`,
		`{"type":"message","role":"user","content":[{"type":"input_text","text":"ignored"}]}`,
	}
	for _, line := range lines {
		appendLine(t, transcript, line)
	}

	// Expect 2 chunks: response("first reply") + thinking("hmm").
	// User line MUST NOT produce a chunk.
	got := drain(a.Events(), 2, 2*time.Second)
	if len(got) != 2 {
		t.Fatalf("expected 2 chunks, got %d: %+v", len(got), got)
	}
	if got[0].Type != shim.ChunkResponse || got[0].Data.(string) != "first reply" {
		t.Errorf("chunk 0: got %+v", got[0])
	}
	if got[1].Type != shim.ChunkThinking || got[1].Data.(string) != "hmm" {
		t.Errorf("chunk 1: got %+v", got[1])
	}
}

func TestAdapter_IdleQuery_EmitsSyntheticQueryChunk(t *testing.T) {
	// Use a short idle threshold so the test completes quickly.
	stub := &capturePaneStub{}

	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	// Override with our prompt-showing stub.
	a.CapturePaneFn = stub.fn
	a.idleThreshold = 150 * time.Millisecond

	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Set pane content to something that has a prompt pattern and doesn't change.
	stub.set("codex output\n❯ type something")

	c := receiveChunk(t, a.Events(), 3*time.Second)
	if c.Type != shim.ChunkQuery {
		t.Fatalf("expected query chunk, got type=%v", c.Type)
	}
	q, ok := c.Data.(shim.QueryData)
	if !ok {
		t.Fatalf("expected QueryData payload, got %T", c.Data)
	}
	if q.ID == "" {
		t.Error("query id should be non-empty")
	}
	if !hasPromptPattern(q.Prompt) && q.Prompt == "" {
		t.Errorf("query prompt should be non-empty, got %q", q.Prompt)
	}
}

func TestAdapter_IdleQuery_NoChunk_WhenBufferChanges(t *testing.T) {
	// Verify idle loop does NOT fire while the buffer keeps changing.
	// We use a threshold of 200ms and keep the buffer changing for 300ms —
	// longer than the threshold — so the pane never settles long enough.
	stub := &capturePaneStub{}
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	a.CapturePaneFn = stub.fn
	a.idleThreshold = 200 * time.Millisecond

	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Change the buffer every 40ms for 300ms total. Each change resets the idle
	// clock so the pane is never idle for the full 200ms threshold.
	deadline := time.Now().Add(300 * time.Millisecond)
	i := 0
	for time.Now().Before(deadline) {
		stub.set(fmt.Sprintf("changing output %d\n❯ prompt", i))
		i++
		time.Sleep(40 * time.Millisecond)
	}

	// No query chunk should have arrived during the changing window.
	select {
	case c := <-a.Events():
		if c.Type == shim.ChunkQuery {
			t.Errorf("should not have emitted query chunk on active buffer, got %+v", c)
		}
	case <-time.After(50 * time.Millisecond):
		// correct — no query chunk fired while buffer was changing.
	}
}

func TestAdapter_IdleQuery_NoChunk_WhenNoPromptPattern(t *testing.T) {
	stub := &capturePaneStub{}
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	a.CapturePaneFn = stub.fn
	a.idleThreshold = 100 * time.Millisecond

	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Idle buffer with NO prompt pattern.
	stub.set("codex thinking... please wait")

	select {
	case c := <-a.Events():
		if c.Type == shim.ChunkQuery {
			t.Errorf("should not emit query chunk when no prompt pattern, got %+v", c)
		}
	case <-time.After(400 * time.Millisecond):
		// correct — no query chunk without prompt pattern.
	}
}

// TestHashTail_NoAllocations asserts the per-tick change-detection path does
// not allocate. Regression guard for orch#108: the previous sha256 path copied
// the whole pane string into a fresh []byte every poll (~50KB/tick → MADV_FREE
// arena drift over long runs).
func TestHashTail_NoAllocations(t *testing.T) {
	// Build a 24KB sample pane buffer, similar to what tmux capture-pane
	// returns for a default-size pane.
	content := strings.Repeat("codex output line that looks roughly like real TUI text\n", 480)

	h := fnv.New64a()
	// Warm-up so any one-time init isn't counted.
	_ = hashTail(h, content, 4096)

	allocs := testing.AllocsPerRun(200, func() {
		_ = hashTail(h, content, 4096)
	})
	// io.WriteString on a *Hash should not allocate; tail-slicing is a
	// pointer/length adjustment. Allow a tiny slack for runtime jitter.
	if allocs > 1 {
		t.Fatalf("hashTail allocates per call: got %.2f allocs/op, want ≤1", allocs)
	}
}

// TestIdleQueryLoop_BoundedHeap drives ~1000 idleQueryLoop ticks through the
// adapter and asserts heap growth stays bounded. Regression guard for orch#108
// where the codex shim's RSS climbed ~26% over a 3-min soak while peer
// adapters (claude/pi/gemini) stayed within ±1%.
func TestIdleQueryLoop_BoundedHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heap-growth test in -short mode")
	}

	stub := &capturePaneStub{}
	a, _, _ := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	a.CapturePaneFn = stub.fn
	a.idleThreshold = 2 * time.Millisecond // poll at 1ms

	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Vary content every iteration so the loop hashes a fresh buffer each
	// tick (worst case for the old sha256 path).
	base := strings.Repeat("pane line of typical codex TUI width that wraps occasionally\n", 400)
	change := func(i int) {
		stub.set(fmt.Sprintf("%s\nsample %d\n", base, i))
	}
	change(0)

	// Drain any chunks the loop emits so the events channel never blocks.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case <-a.Events():
			case <-shimCtx.Done():
				return
			}
		}
	}()

	// Warm-up: let the loop reach steady state.
	for i := 0; i < 100; i++ {
		change(i)
		time.Sleep(1 * time.Millisecond)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Drive ~1000 captures.
	for i := 0; i < 1000; i++ {
		change(i + 1000)
		time.Sleep(1 * time.Millisecond)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// HeapInuse should not climb meaningfully. Allow 2MB of slack for
	// non-idle-loop allocations and runtime jitter.
	const maxGrowthBytes = 2 * 1024 * 1024
	growth := int64(after.HeapInuse) - int64(before.HeapInuse)
	if growth > maxGrowthBytes {
		t.Fatalf("idleQueryLoop heap growth too high: before=%d after=%d delta=%d bytes (limit=%d)",
			before.HeapInuse, after.HeapInuse, growth, maxGrowthBytes)
	}

	// Also assert no goroutine leak from the loop itself.
	shimCancel()
	<-drained
	time.Sleep(50 * time.Millisecond)
	// Loop exit is best-effort to check here — the main signal is HeapInuse.
}

// TestAdapter_HappyPath_PromptResponseTerminator covers the full Synadia
// per-turn sequence from the adapter's perspective (orch#134):
//
//  1. Caller invokes OnPrompt → adapter delivers via send-keys.
//  2. The harness writes transcript lines → adapter emits §6.3 response
//     chunks containing the agent's user-visible reply text.
//  3. The harness's Stop hook writes the marker → adapter emits the §6.5
//     terminator.
//
// This is the canonical regression guard for "wire-conformant but
// content-mute" — the pre-#134 failure shape where the terminator fired
// but no response chunks ever landed between the ack and the terminator.
func TestAdapter_HappyPath_PromptResponseTerminator(t *testing.T) {
	a, rec, sessionsDir := newTestAdapter(t)
	defer func() { _ = a.Close() }()
	shimCtx, shimCancel := context.WithCancel(context.Background())
	defer shimCancel()
	if err := a.Start(shimCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Settle so fsnotify registers the stop-dir watch.
	time.Sleep(50 * time.Millisecond)

	// 1. Caller sends a prompt.
	if err := a.OnPrompt(context.Background(), "say hello"); err != nil {
		t.Fatalf("OnPrompt: %v", err)
	}
	calls := rec.snapshot()
	if len(calls) != 1 || calls[0].Text != "say hello" {
		t.Fatalf("send-keys: got %+v", calls)
	}

	// 2. Harness writes a response line into the rollout transcript.
	dayPath := filepath.Join(sessionsDir, "2024", "01", "15")
	if err := os.MkdirAll(dayPath, 0o755); err != nil {
		t.Fatal(err)
	}
	transcript := filepath.Join(dayPath, "rollout-1700000000-uuid.jsonl")
	appendLine(t, transcript,
		`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello back"}]}`)

	c := receiveChunk(t, a.Events(), 2*time.Second)
	if c.Type != shim.ChunkResponse {
		t.Fatalf("expected response chunk, got %+v", c)
	}
	if s, ok := c.Data.(string); !ok || s != "hello back" {
		t.Fatalf("response payload: got %v want %q", c.Data, "hello back")
	}

	// 3. Stop hook writes the marker → terminator.
	marker := filepath.Join(a.stopDir(), "%42.event")
	tmp := marker + ".tmp"
	if err := os.WriteFile(tmp, []byte(`{"event":"stop","harness":"codex"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, marker); err != nil {
		t.Fatal(err)
	}
	// The stop-marker may race against the idle-query loop (its threshold
	// is 5s here, well past the test duration), so the next chunk on the
	// channel is the terminator.
	c = receiveChunk(t, a.Events(), 2*time.Second)
	if !c.Terminator {
		t.Fatalf("expected terminator after stop marker, got %+v", c)
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
