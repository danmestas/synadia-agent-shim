// Package echo provides a minimal reference implementation of the
// shim.Adapter interface. It echoes every prompt back as a single
// response chunk followed by a terminator — no external processes,
// no file I/O, no tmux.
//
// echo is used as:
//
//   - a smoke baseline in the §12 conformance test suite (zero
//     infrastructure dependencies, deterministic output),
//   - a starting template for new harness adapters,
//   - a floor that demonstrates the minimum correct adapter shape.
//
// Callers do not need to set any environment variables or run any
// external tooling to use echo; pass "--agent echo" to orch-agent-shim.
package echo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/danmestas/synadia-agent-shim/shim"
)

// Adapter is the echo reference implementation.
//
// Lifecycle (mirrors shim.Adapter contract):
//
//  1. New() creates the adapter.
//  2. Start(shimCtx) is called once after NATS registration. Binds
//     the events channel to the shim's lifetime.
//  3. OnPrompt(ctx, text) sends one response chunk ("echo: <text>")
//     and one terminator into the channel. Returns immediately.
//  4. Close() closes the channel exactly once. Idempotent.
type Adapter struct {
	// PrefixFn, when non-nil, overrides the default "echo: " prefix.
	// Useful for tests that need deterministic, custom output without
	// constructing a new adapter.
	PrefixFn func(text string) string

	events    chan shim.Chunk
	startOnce sync.Once
	closeOnce sync.Once
	started   atomic.Bool
}

var _ shim.Adapter = (*Adapter)(nil)

// New returns a ready-to-use echo adapter with default settings.
func New() *Adapter {
	return &Adapter{
		events: make(chan shim.Chunk, 8),
	}
}

// Start implements shim.Adapter. Marks the adapter live; subsequent
// OnPrompt calls will emit chunks. shimCtx is stored for future use
// but the echo adapter performs no background work.
func (a *Adapter) Start(_ context.Context) error {
	a.startOnce.Do(func() {
		a.started.Store(true)
	})
	return nil
}

// OnPrompt implements shim.Adapter. Sends one response chunk and one
// terminator. Returns immediately; the shim's event pump drains them.
//
// If Start has not been called, OnPrompt returns an error so misuse
// by callers that skip the lifecycle is caught early.
func (a *Adapter) OnPrompt(_ context.Context, text string) error {
	if !a.started.Load() {
		return fmt.Errorf("echo: OnPrompt called before Start")
	}
	response := a.buildResponse(text)
	a.events <- shim.NewResponseChunk(response)
	a.events <- shim.NewTerminatorChunk()
	return nil
}

// Events implements shim.Adapter. Returns the chunk channel.
func (a *Adapter) Events() <-chan shim.Chunk {
	return a.events
}

// Close implements shim.Adapter. Closes the events channel exactly once.
func (a *Adapter) Close() error {
	a.closeOnce.Do(func() {
		close(a.events)
	})
	return nil
}

func (a *Adapter) buildResponse(text string) string {
	if a.PrefixFn != nil {
		return a.PrefixFn(text)
	}
	return "echo: " + text
}
