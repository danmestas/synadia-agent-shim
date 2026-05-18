package echo_test

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/synadia-agent-shim/adapter/echo"
	"github.com/danmestas/synadia-agent-shim/shim"
	"github.com/danmestas/synadia-agent-shim/shimtest"
)

// TestAdapterConformance runs the full §12 conformance suite against
// the echo reference adapter. Because echo has zero external
// dependencies, this always runs in CI and confirms the shim protocol
// framework is correct before any per-harness adapter is tested.
func TestAdapterConformance(t *testing.T) {
	shimtest.RunAdapterConformance(
		t,
		func() shim.Adapter { return echo.New() },
		shimtest.ConformanceConfig{
			Agent: "echo",
			Pane:  "%1",
			Owner: "conformance-runner",
		},
		false, // echo emits response chunks — include all cases
	)
}

// drainN reads exactly n chunks from ch within timeout.
func drainN(t *testing.T, ch <-chan shim.Chunk, n int, timeout time.Duration) []shim.Chunk {
	t.Helper()
	chunks := make([]shim.Chunk, 0, n)
	deadline := time.After(timeout)
	for len(chunks) < n {
		select {
		case c, ok := <-ch:
			if !ok {
				t.Fatalf("drainN: channel closed after %d chunks (want %d)", len(chunks), n)
			}
			chunks = append(chunks, c)
		case <-deadline:
			t.Fatalf("drainN: timeout after %d chunks (want %d)", len(chunks), n)
		}
	}
	return chunks
}

func TestEcho_LifecycleHappyPath(t *testing.T) {
	a := echo.New()
	ctx := context.Background()

	if err := a.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := a.OnPrompt(ctx, "hello"); err != nil {
		t.Fatalf("OnPrompt: %v", err)
	}

	chunks := drainN(t, a.Events(), 2, time.Second)

	// First chunk: response with "echo: hello".
	if chunks[0].Type != shim.ChunkResponse {
		t.Errorf("chunk[0].Type: got %q want %q", chunks[0].Type, shim.ChunkResponse)
	}
	if got, ok := chunks[0].Data.(string); !ok || got != "echo: hello" {
		t.Errorf("chunk[0].Data: got %v want %q", chunks[0].Data, "echo: hello")
	}
	if chunks[0].Terminator {
		t.Error("chunk[0] should not be a terminator")
	}

	// Second chunk: terminator.
	if !chunks[1].Terminator {
		t.Error("chunk[1] should be a terminator")
	}
}

func TestEcho_MultiplePrompts(t *testing.T) {
	a := echo.New()
	ctx := context.Background()

	if err := a.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	prompts := []string{"one", "two", "three"}
	for _, p := range prompts {
		if err := a.OnPrompt(ctx, p); err != nil {
			t.Fatalf("OnPrompt(%q): %v", p, err)
		}
	}

	// Each prompt produces response + terminator.
	chunks := drainN(t, a.Events(), len(prompts)*2, 2*time.Second)
	for i, p := range prompts {
		base := i * 2
		want := "echo: " + p
		if got, ok := chunks[base].Data.(string); !ok || got != want {
			t.Errorf("prompt %q response: got %v want %q", p, chunks[base].Data, want)
		}
		if !chunks[base+1].Terminator {
			t.Errorf("prompt %q: chunk[1] should be terminator", p)
		}
	}
}

func TestEcho_OnPromptBeforeStart_ReturnsError(t *testing.T) {
	a := echo.New()
	if err := a.OnPrompt(context.Background(), "hi"); err == nil {
		t.Fatal("expected error from OnPrompt before Start")
	}
}

func TestEcho_CloseIsIdempotent(t *testing.T) {
	a := echo.New()
	_ = a.Start(context.Background())

	if err := a.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close #2 (idempotent): %v", err)
	}
}

func TestEcho_ChannelClosedAfterClose(t *testing.T) {
	a := echo.New()
	_ = a.Start(context.Background())
	_ = a.Close()

	ch := a.Events()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel")
		}
	default:
		t.Fatal("expected channel to be closed (readable)")
	}
}

func TestEcho_CustomPrefixFn(t *testing.T) {
	a := echo.New()
	a.PrefixFn = func(text string) string { return "CUSTOM:" + text }
	_ = a.Start(context.Background())
	_ = a.OnPrompt(context.Background(), "x")

	chunks := drainN(t, a.Events(), 2, time.Second)
	if got, ok := chunks[0].Data.(string); !ok || got != "CUSTOM:x" {
		t.Errorf("PrefixFn: got %v want %q", chunks[0].Data, "CUSTOM:x")
	}
}

func TestEcho_ImplementsAdapter(t *testing.T) {
	// Compile-time check that *Adapter satisfies shim.Adapter. Also
	// verified by var _ in echo.go, but an explicit test catches regressions.
	var _ shim.Adapter = echo.New()
}
