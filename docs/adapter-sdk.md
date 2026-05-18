# Adapter SDK

This document describes how to implement a new harness adapter for
`orch-agent-shim`. An adapter is a Go struct that implements the
`shim.Adapter` interface (`internal/shim/adapter.go`) and bridges
one specific agent CLI to the Synadia Agent Protocol v0.3 bus.

The canonical adapter is `internal/adapter/claudecode/cc.go`. The
minimal reference is `internal/adapter/echo/echo.go`.

---

## Why adapters

`orch-agent-shim` handles all NATS protocol mechanics — service
registration, chunk encoding, heartbeats, error headers, queue groups —
so an adapter author writes only the harness-specific plumbing:

- how to deliver a prompt to the CLI (e.g. `tmux send-keys`),
- how to observe the CLI's output (e.g. tail a JSONL transcript),
- when to signal end-of-turn (e.g. watching a marker file).

The adapter never sees NATS. It emits `shim.Chunk` values into a
buffered Go channel and the shim's event pump forwards them to the
correct NATS reply subject.

---

## Interface

```go
// internal/shim/adapter.go
type Adapter interface {
    Start(ctx context.Context) error
    OnPrompt(ctx context.Context, text string) error
    Events() <-chan Chunk
    Close() error
}
```

### Lifecycle

```
New()  →  Start(shimCtx)  →  OnPrompt(turnCtx, text)*  →  Close()
```

1. `New()` — constructor called by `buildAdapter` in `main.go`. Should
   accept the parameters your harness needs (pane id, cwd, etc.) and
   return a zero-started adapter. No I/O, no background goroutines.

2. `Start(shimCtx)` — called once by the shim **after** NATS
   registration succeeds. Bind all long-lived background work (file
   tailers, fsnotify watchers, marker-file pollers) to `shimCtx`.
   When `shimCtx` is cancelled the adapter's watchers must stop.

   > **Key constraint.** Do **not** bind background work to `OnPrompt`'s
   > context — that context represents a single turn and will be
   > cancelled at turn end, which must not dismantle the adapter.

3. `OnPrompt(turnCtx, text)` — called once per accepted prompt. Must
   return quickly (non-blocking). Kick off the agent turn (e.g.
   `tmux send-keys`) and return. Chunks flow back through `Events()`.

   If the adapter cannot start the turn (e.g. CLI not found), return a
   non-nil error. The shim will emit a `500` error chunk + terminator.

4. `Close()` — called on shim shutdown. Stop background goroutines,
   close the `Events()` channel exactly once. **Must be idempotent**
   (the shim may defer it; tests often defer it too). Use `sync.Once`.

---

## Chunk types

`shim.Chunk` is the single wire type. Its `Type` field is the §6.2
discriminator. Use the constructor helpers — do not build `Chunk`
structs by hand.

| Constructor | §6 section | Wire shape | When to emit |
|---|---|---|---|
| `shim.NewResponseChunk(data)` | §6.3 | `{"type":"response","data":…}` | Each unit of agent output |
| `shim.NewStatusChunk(value)` | §6.4 | `{"type":"status","data":"…"}` | Lifecycle signals (adapters rarely emit these; shim emits `ack`) |
| `shim.NewQueryChunk(id, replySubject, prompt)` | §7.1 | `{"type":"query","data":{…}}` | Mid-stream question to the caller |
| `shim.NewTerminatorChunk()` | §6.5 | zero-byte body (no JSON) | End of turn — one per `OnPrompt` |
| `shim.NewErrorChunk(code, msg, body)` | §9.3 | error headers + body | Failure mid-stream |

`data` in `NewResponseChunk` may be a `string` (Appendix B.4) or any
`json.Marshal`-able value (Appendix B.5). The shim marshals it into the
`{"type":"response","data":…}` envelope.

### Terminator rule

Every `OnPrompt` call **must** eventually cause exactly one `NewTerminatorChunk()`
to appear in the channel. The shim stops draining the channel for the
current turn after it sees the terminator. Sending more than one
terminator per turn is safe (the shim ignores the duplicate), but
omitting it leaves the caller blocked until the §6.6 inactivity timeout.

### Unknown chunk types

The spec (§6.6) says callers silently ignore unknown `type` values, so
emitting non-standard types (`"thinking"`, `"tool_use"`) is
forward-compatible.

---

## Synadia §6 chunk mapping

The table below maps each `ChunkType` constant to its §6 definition and
shows the exact wire shape the shim produces.

| `ChunkType` | Spec ref | Wire `{"type":…,"data":…}` example |
|---|---|---|
| `ChunkResponse` | §6.3 | `{"type":"response","data":"Hello."}` or `{"type":"response","data":{"text":"Hello."}}` |
| `ChunkStatus` | §6.4 | `{"type":"status","data":"ack"}` |
| `ChunkQuery` | §7.1 | `{"type":"query","data":{"id":"…","reply_subject":"…","prompt":"…"}}` |
| `ChunkThinking` | (non-standard) | `{"type":"thinking","data":"…"}` |
| `ChunkToolUse` | (non-standard) | `{"type":"tool_use","data":{…}}` |
| terminator | §6.5 | zero-byte NATS message, no headers |
| error | §9.3 | `Nats-Service-Error-Code` + `Nats-Service-Error` headers, optional JSON body |

---

## Metadata

`shim.AdapterMetadata` (defined in `internal/shim/adapter.go`) is the
stable place for an adapter to declare fixed metadata. Today the shim
builds service metadata from `Config` fields. Future adapters can
populate `AdapterMetadata` in their `New()` constructor to override
defaults (e.g. a custom `MaxPayload`).

```go
type AdapterMetadata struct {
    HarnessName   string // canonical Synadia §C name, overrides Config.Agent if set
    MaxPayload    string // defaults to "1MB"
    AttachmentsOK bool   // false for all v1 adapters
}
```

---

## How to add a new adapter

### 1. Create the package

```
internal/adapter/<name>/
    <name>.go        # Adapter struct + New() + lifecycle methods
    <name>_test.go   # Unit tests
```

Use the echo adapter as a starting template:

```
cp internal/adapter/echo/echo.go   internal/adapter/<name>/<name>.go
cp internal/adapter/echo/echo_test.go internal/adapter/<name>/<name>_test.go
```

### 2. Implement the interface

```go
package <name>

import (
    "context"
    "sync"

    "github.com/danmestas/orch/internal/shim"
)

type Adapter struct {
    Pane      string
    events    chan shim.Chunk
    closeOnce sync.Once
    // ... harness-specific fields
}

var _ shim.Adapter = (*Adapter)(nil) // compile-time check

func New(pane string) *Adapter {
    return &Adapter{Pane: pane, events: make(chan shim.Chunk, 64)}
}

func (a *Adapter) Start(ctx context.Context) error {
    // start background watchers bound to ctx
    return nil
}

func (a *Adapter) OnPrompt(_ context.Context, text string) error {
    // kick off agent turn — non-blocking
    return nil
}

func (a *Adapter) Events() <-chan shim.Chunk { return a.events }

func (a *Adapter) Close() error {
    a.closeOnce.Do(func() { close(a.events) })
    return nil
}
```

The `var _ shim.Adapter = (*Adapter)(nil)` line produces a compile error
if you miss a method — put it directly after the struct declaration.

### 3. Register in main.go

Open `cmd/orch-agent-shim/main.go` and add:

```go
import "github.com/danmestas/orch/internal/adapter/<name>"

// in buildAdapter:
case "<name>":
    return <name>.New(pane), nil
```

Update the default error message to include the new adapter name.

### 4. Run the conformance test suite

The generic §12 conformance framework lives in
`internal/shim/conformance_test.go`. Call `RunAdapterConformance` from
your adapter's test file to verify protocol correctness:

```go
// internal/adapter/<name>/<name>_test.go
package <name>_test

import (
    "testing"
    "github.com/danmestas/orch/internal/shim"
    "<name>" "github.com/danmestas/orch/internal/adapter/<name>"
)

func TestConformance(t *testing.T) {
    shim.RunAdapterConformance(t, func() shim.Adapter {
        return <name>.New("%1")
    }, shim.Config{
        Agent: "<name>",
        Pane:  "%1",
        Owner: "conformance-runner",
    })
}
```

### 5. Verify

```bash
go test -race ./internal/adapter/<name>/...
go test -race ./internal/shim/...
gofmt -l ./...
go vet ./...
```

---

## File layout summary

```
cmd/orch-agent-shim/
    main.go                  ← buildAdapter switch (add new case here)

internal/shim/
    adapter.go               ← Adapter interface + Chunk types + AdapterMetadata
    shim.go                  ← NATS mechanics (do not modify for a new adapter)
    conformance_test.go      ← §12 conformance framework + RunAdapterConformance

internal/adapter/
    echo/                    ← reference adapter (minimal, zero dependencies)
    claudecode/              ← claude-code v1 adapter
    codex/                   ← codex adapter
    gemini/                  ← gemini-cli adapter
    pi/                      ← pi adapter
    <name>/                  ← your new adapter
```

---

## §12 agent checklist (conformance reference)

The `RunAdapterConformance` function exercises these requirements. An
adapter passes when all sub-tests are green.

| # | Requirement | Exercised by |
|---|---|---|
| §3.1 | Service registered as `agents` | `$SRV.INFO.agents` name check |
| §3.2 | `metadata.agent` / `owner` / `protocol_version` present | INFO metadata check |
| §6.4 | First reply chunk is `{"type":"status","data":"ack"}` | prompt stream sub-test |
| §6.2 | Response chunks have `{"type":"response","data":…}` shape | prompt stream sub-test |
| §6.5 | Stream ends with zero-byte terminator (no headers) | prompt stream terminator check |
| §8.7 | `status` endpoint replies with §8.3 heartbeat payload | status request sub-test |
| §8.1/§8.2 | Heartbeat published on `agents.hb.<token>.<owner>.<enc>` | heartbeat sub-test |
| §9.2 | Empty prompt rejected with 400 | error path sub-test |
| §9.3 | Error stream: error header + terminator | error path sub-test |
