# synadia-agent-shim

A Go shim that wraps an agent CLI (claude-code, codex, pi, gemini, or
any custom adapter) and exposes it on a NATS bus using the
[Synadia Agent Protocol v0.3](https://github.com/synadia-io/synadia-spec).

Operators publish a prompt on `agents.prompt.<token>.<owner>.<pane>`;
the shim translates it into the CLI's native invocation, streams chunks
back, and emits heartbeats + status per spec. Control-plane verbs
(`interrupt`, `redirect`) are routed via `orch.signal.>`.

Extracted from [danmestas/orch](https://github.com/danmestas/orch);
see [`docs/proposals/0001-extract-synadia-agent-shim.md`](https://github.com/danmestas/orch/blob/main/docs/proposals/0001-extract-synadia-agent-shim.md)
for the rationale.

## Install

### As a binary (npm)

```sh
npm install -g @agent-ops/synadia-agent-shim
```

The postinstall script fetches the matching release binary for your
OS/arch and places it at `vendor/synadia-agent-shim`. Two wrapper
scripts are exposed on `$PATH`:

- `synadia-agent-shim` — canonical
- `orch-agent-shim` — backwards-compat alias for one orch major release

### As a Go library

```sh
go get github.com/danmestas/synadia-agent-shim/shim
go get github.com/danmestas/synadia-agent-shim/adapter/echo
```

## Usage (CLI)

```sh
synadia-agent-shim --agent claude-code --pane %37
```

Resolution order (most explicit wins):

| Setting | Source |
| --- | --- |
| NATS URL | `--nats` → `$NATS_URL` → `~/.sesh/hub.url` → `nats://127.0.0.1:4222` |
| Owner | `--owner` → `$ORCH_OWNER` → `$USER` → `/etc/passwd` lookup |
| Session | `--session` → `$SESH_SESSION` → omitted from metadata |
| CWD | `--cwd` → `tmux display-message -p '#{pane_current_path}'` |

The shim exits when the bound pane dies (SIGCHLD from the parent
shell). `orch-spawn` backstops this by `wait`-ing on a sentinel pid.

## Usage (Go SDK)

```go
package main

import (
    "context"
    "log"

    "github.com/danmestas/synadia-agent-shim/adapter/echo"
    "github.com/danmestas/synadia-agent-shim/shim"
)

func main() {
    cfg := shim.Config{
        Agent:   "echo",
        Pane:    "%1",
        Owner:   "you",
        NATSURL: shim.ReadNATSURL(""),
        Adapter: echo.New(),
        // SubjectPrefix defaults to "agents".
        // SignalPrefix defaults to "orch.signal".
    }
    if err := shim.Run(context.Background(), cfg); err != nil {
        log.Fatal(err)
    }
}
```

Non-orch consumers can retarget the namespace:

```go
cfg.SubjectPrefix = "dagnats"      // dagnats.prompt.*, dagnats.status.*, ...
cfg.SignalPrefix  = "dagnats.signal"
```

## Writing a custom adapter

See [`docs/adapter-sdk.md`](docs/adapter-sdk.md). Minimal contract:

```go
type Adapter interface {
    Start(ctx context.Context) error
    OnPrompt(ctx context.Context, prompt string) error
    Events() <-chan Chunk
    Close() error
}
```

Adapters that need imperative interrupt (TUI harnesses that don't
honour `ctx.Done()`) implement the optional `Aborter` interface — the
shim type-asserts and calls `Abort` on `orch.signal.interrupt` arrival.

## Built-in adapters

- [`adapter/claudecode`](adapter/claudecode) — Anthropic claude-code CLI
- [`adapter/codex`](adapter/codex) — OpenAI codex
- [`adapter/pi`](adapter/pi) — Inflection pi
- [`adapter/gemini`](adapter/gemini) — Google gemini-cli
- [`adapter/echo`](adapter/echo) — reference adapter (no external deps)

## Versioning

- shim v1.0.0 = current behavior at extraction time from orch
- Each Synadia spec version bump → shim major version bump
- Adapter API additions → minor version bump
- Bug fixes → patch

The `Adapter` and `Config` shapes are frozen at v1.

## Wire surface

- Service name: `<SubjectPrefix>` (default `agents`), per Synadia §3.1
- Prompt subject: `<SubjectPrefix>.prompt.<token>.<owner>.<session-or-pane>`
- Status subject: `<SubjectPrefix>.status.<token>.<owner>.<session-or-pane>`
- Heartbeat: `<SubjectPrefix>.hb.<token>.<owner>.<session-or-pane>` every 30s
- Signal: `<SignalPrefix>.<verb>.<token>.<owner>.<pane>` (orch#133)

Envelope headers: W3C `traceparent` + `Sesh-Task-Id` / `Sesh-Attempt`
when set. See [`docs/architecture.md`](docs/architecture.md) for the
full layout.

## License

Apache-2.0. See [LICENSE](LICENSE).
