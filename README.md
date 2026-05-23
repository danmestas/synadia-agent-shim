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
| Instance ID | `--instance-id` → omitted (no slug-keyed subjects) |
| CWD | `--cwd` → `tmux display-message -p '#{pane_current_path}'` |

The shim exits when the bound pane dies (SIGCHLD from the parent
shell). `orch-spawn` backstops this by `wait`-ing on a sentinel pid.

### `--instance-id`

`--instance-id <slug>` attaches a human-readable worker identity to the
shim. Subject-safe charset `[a-zA-Z0-9._-]`, length 1-128 — invalid
slugs are rejected at startup so a typo fails loud, not at first publish.

When set, the shim:

1. Adds `instance_id: "<slug>"` to `$SRV.INFO.agents` metadata
   alongside the existing `pane_id`. Discovery tools can filter by
   either; `pane_id` stays so pane-watchdog and `tmux send-keys`
   consumers keep working.
2. Registers a SECOND prompt + status endpoint on the slug-keyed
   subjects:
   - `agents.prompt.<token>.<owner>.<slug>`
   - `agents.status.<token>.<owner>.<slug>`
3. Publishes heartbeats on the slug-keyed subject too:
   - `agents.hb.<token>.<owner>.<slug>`

Dual-publish is gated by env var `ORCH_SLUG_DUAL_PUBLISH`:

| Value | Behavior (with `--instance-id` set) |
| --- | --- |
| unset / `1` | legacy `pct<N>` track + slug track both live (default — safe during rollout) |
| `0` | slug track only; legacy `pct<N>` subjects have no subscriber |

When `--instance-id` is **not** set, the shim runs as before — only the
legacy `pct<N>`-keyed track is registered, regardless of the env var.

The dual-publish window is intended to last **two releases**; the
legacy `pct<N>` track will be retired in a follow-up issue. Track the
deprecation in `CHANGELOG.md`.

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

## Releasing

Releases are tag-driven. Pushing an annotated tag `vX.Y.Z` triggers
`.github/workflows/release.yml`:

1. `goreleaser` builds platform archives + creates the GitHub Release.
2. `publish-npm` syncs `package.json` version from the tag, then runs
   `npm publish --access public`.

To cut a release:

```sh
git tag vX.Y.Z
git push --tags
```

One-time operator setup: set the `NPM_TOKEN` secret in the repo's
GitHub settings (Settings → Secrets → Actions) with an npm automation
token that has publish access to `@agent-ops/synadia-agent-shim`.
PRs run `npm publish --dry-run` in CI to catch packaging breakage
before a tag is pushed.

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
