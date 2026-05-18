# orch-agent-shim

A single Go binary that wraps an arbitrary agent CLI and exposes it via
the [Synadia Agent Protocol v0.3](https://github.com/synadia-io/agents)
on a sesh hub (or standalone NATS server). v1 ships with one adapter
(`claude-code`). The keystone for orch's adoption of Synadia.

## Why

Before #94, orch ran panes that published events to NATS via the
`hooks/orch-nats-publish-*.sh` scripts — an ad-hoc, 4-subject convention
documented in the (now-historical) `docs/nats-bridge.md`. Those scripts
were emit-only: callers couldn't *prompt* a pane over NATS, only observe
it.

Synadia's protocol fixes both gaps: callers discover panes via
`$SRV.INFO.agents`, prompt them on a documented subject, and receive
typed streamed chunks back. Heartbeats and a status request/reply
endpoint give first-class liveness.

The shim is now the **only** event channel between orch panes and the
outside world. The legacy bridge + publish hooks + marker hooks were
retired in #94; every spawned pane reaches the bus exclusively through
its shim sibling.

## Architecture

```
            ┌─────────────────────────────────────────────────────────┐
            │  caller (orch-tell v2 / Synadia SDK / nats req / ...)    │
            └─────────────────────────────────────────────────────────┘
                          │ $SRV.INFO.agents  + agents.prompt.cc.<owner>.pct<pane>
                          ▼
            ┌─────────────────────────────────────────────────────────┐
            │           NATS micro service `agents`                    │
            │                                                          │
            │   shim.go    ── registration · chunk encoding ·          │
            │                  heartbeat · prompt dispatch              │
            │   adapter.go ── Adapter interface (OnPrompt, Events,     │
            │                  Close) and Chunk wire types              │
            └─────────────────────────────────────────────────────────┘
                          │
                          ▼
            ┌─────────────────────────────────────────────────────────┐
            │  claudecode/cc.go      sibling background process         │
            │                                                          │
            │  • tails ~/.claude/projects/<enc-cwd>/<sid>.jsonl         │
            │    → emits one `response` chunk per assistant text block │
            │    + `thinking` / `tool_use` chunks for other blocks     │
            │  • watches ~/.cache/orch-stop/<pane>.event               │
            │    → emits terminator → closes active stream             │
            │  • watches ~/.cache/orch-notify/<pane>.notify            │
            │    → emits §7 query chunk                                │
            │  • inbound prompt → tmux send-keys -l + Enter            │
            └─────────────────────────────────────────────────────────┘
                          │
                          ▼
            ┌─────────────────────────────────────────────────────────┐
            │  tmux pane: `claude --dangerously-skip-permissions ...`  │
            └─────────────────────────────────────────────────────────┘
```

## Modules

| File                                  | Purpose                                                                                |
|---------------------------------------|----------------------------------------------------------------------------------------|
| `cmd/orch-agent-shim/main.go`         | Flag/env parsing, adapter selection, signal plumbing, `shim.Run` invocation.            |
| `internal/shim/shim.go`               | Service registration, chunk encoding, heartbeat loop, prompt dispatcher, status reply. |
| `internal/shim/adapter.go`            | `Adapter` interface and `Chunk` wire types.                                             |
| `internal/adapter/claudecode/cc.go`   | claude-code adapter: JSONL tail, marker watch, tmux send-keys.                          |
| `internal/adapter/gemini/gemini.go`   | gemini adapter: marker watch (AfterAgent → terminator, Notification → query), tmux send-keys. Transcript emission deferred (see below). |
| `internal/adapter/pi/pi.go`           | pi adapter: JSONL tail, stop-marker watch, synthetic query chunks, tmux send-keys.      |
| `internal/adapter/codex/codex.go`     | codex adapter: rollout JSONL tail, stop-marker watch, synthetic idle-query chunks.      |

## Adapter matrix

| `--agent` value | Transcript source                              | Stop detection         | Notification / Query         | Inbound prompt    |
|-----------------|------------------------------------------------|------------------------|------------------------------|-------------------|
| `claude-code` / `claude` | `~/.claude/projects/<enc>/<sid>.jsonl` tail | `~/.cache/orch-stop/<pane>.event` fsnotify | `~/.cache/orch-notify/<pane>.notify` fsnotify → §7 query | tmux send-keys |
| `pi`            | `~/.pi/agent/sessions/<enc>/<ts>_<sid>.jsonl` tail | `~/.cache/orch-stop/<pane>.event` fsnotify | Synthetic §7 query at turn-end (Plan 11 idle heuristic — pi has no Notification event) | tmux send-keys |
| `codex`         | `~/.codex/sessions/<Y>/<M>/<D>/rollout-<ts>-<uuid>.jsonl` | `~/.cache/orch-stop/<pane>.event` fsnotify | Synthetic: idle 5s + TUI prompt pattern → §7 query chunk | tmux send-keys |

`<enc>` in both cases: replace `/` and `.` with `-` in the pane's CWD.

**Marker-watch notice (orch#94):** The shim adapters watch
`~/.cache/orch-{stop,notify}/<pane>.{event,notify}` files. The legacy
hook writers (`orch-stop-marker.sh`, `orch-notify-marker.sh`, the
`pi-extensions/orch-*` scripts) that produced those files were retired in
#94. The fsnotify watch loops remain so the test suites can drive them
directly. Replacing the loops with a bus-native turn-end detector is a
follow-up; in the meantime, live turn-end detection in these adapters
relies on the transcript-tail signals + the synthetic heuristics.

## Configuration

CLI exposes two required flags; everything else falls back to env vars,
then resolved defaults.

| Flag       | Env             | Fallback                                  | Notes                                                      |
|------------|-----------------|-------------------------------------------|------------------------------------------------------------|
| `--agent`  | —               | (required)                                | `claude-code` (or `claude`), `codex`, `pi`, or `gemini`.   |
| `--pane`   | —               | (required)                                | Raw tmux pane id, e.g. `%37`.                              |
| `--owner`  | `ORCH_OWNER`    | `$USER` / passwd lookup                   | Lands in metadata.owner.                                   |
| `--session`| `SESH_SESSION`  | `""` (omitted from metadata)              | Marks the agent as session-aware per §3.2.                 |
| `--nats`   | `NATS_URL`      | `~/.sesh/hub.url` → `nats://127.0.0.1:4222` | URL resolution per `shim.ReadNATSURL`.                     |
| `--outfit` | `ORCH_OUTFIT`   | `""`                                      | orch-specific metadata (forward-compat per §12).           |
| `--role`   | `ORCH_ROLE`     | `worker`                                  | orch-specific metadata.                                    |
| `--cwd`    | —               | `tmux display-message -p '#{pane_current_path}'` | Used by the adapter to locate the transcript directory.    |
| `--interval`| —              | `30s`                                     | Heartbeat cadence. Clamped to ≥ 1s per §8.2.               |

## Subjects

The shim follows the §2.3 channel-plugin default layout, using `cc` as
the abbreviated subject token for claude-code (Synadia convention).
The raw `%`-bearing pane id is preserved in `metadata.pane_id`; the
subject form replaces `%` with `pct`.

| Verb       | Subject                                              |
|------------|------------------------------------------------------|
| Prompt     | `agents.prompt.cc.<owner>.pct<n>`                    |
| Status     | `agents.status.cc.<owner>.pct<n>`                    |
| Heartbeat  | `agents.hb.cc.<owner>.pct<n>`                        |

Discovery happens through `$SRV.INFO.agents` (and `$SRV.PING.agents`
for liveness probes) — callers MUST read the subject off the endpoint
record (§2.1), not construct it from identity (§12 caller checklist).

## §12 conformance map

| §12 requirement                                                        | Implemented in                                         |
|-------------------------------------------------------------------------|--------------------------------------------------------|
| Registers as `agents` micro service                                     | `shim.go: start` → `micro.AddService` with `name=agents`. |
| `metadata.agent / owner / protocol_version` declared                    | `shim.go: serviceMetadata`. Protocol pinned to `"0.3"`. |
| `metadata.session` added when session-aware                             | `serviceMetadata`: emitted iff `cfg.Session != ""`.    |
| `prompt` endpoint with queue group `agents`, `subject` agent-chosen      | `start: AddEndpoint("prompt", ..., WithEndpointQueueGroup("agents"))`. |
| `prompt` endpoint metadata: `max_payload`, `attachments_ok`             | `start: WithEndpointMetadata({max_payload, attachments_ok})`. |
| `status` endpoint with queue group `agents`, §8.3 heartbeat-shaped reply | `start: AddEndpoint("status")` + `handleStatus`.       |
| Accepts plain-text + JSON envelopes                                     | `parseEnvelope` (discrimination on leading `{`).        |
| Rejects malformed / empty / oversize / attachments-when-disallowed → 400 | `handlePrompt: respondError(400, …)`.                  |
| Tolerates and preserves unknown envelope fields                         | `requestEnvelope` ignores unknown keys via `encoding/json`. |
| `ack` is first chunk on reply subject (§6.4)                            | `handlePrompt: publishChunk(reply, NewStatusChunk("ack"))`. |
| Typed chunks `{type, data}` in publication order                        | `encodeChunk` + `eventPump`.                            |
| Empty-payload headerless terminator (§6.5)                              | `publishTerminator` (uses `nc.Publish(reply, nil)`).    |
| Errors precede terminator with §9 headers (pre-ack AND mid-stream)      | `respondError` (or `publishErrorOnReply`) + `publishTerminator`. Every error path emits exactly two messages: the header-bearing signal, then the empty terminator. |
| Heartbeats on `agents.hb.<agent>.<owner>.<name>` at configured cadence  | `heartbeatLoop` + `publishHeartbeat`.                   |
| All §8.3 fields in heartbeat payload                                    | `buildHeartbeat` / `heartbeatPayload`.                  |
| Responds to `$SRV.PING.agents` / `$SRV.INFO.agents`                     | Handled by `nats.go/micro` framework.                   |
| Mid-stream queries conform to §7                                        | `claudecode/cc.go: markerLoop` → `shim.NewQueryChunk`; `codex/codex.go: idleQueryLoop` → synthetic query chunk. |
| `Nats-Service-Error-Code` from §9.2 taxonomy on errors                  | `respondError` / `publishErrorOnReply` set code + body. |

## Lifecycle

The shim is a sibling background process of the pane it wraps —
lifetime bound to the pane via the parent shell's death (and an
optional `wait` on a sentinel pid in orch-spawn's WRAP).

Startup ordering (matches §8.2's "agents SHOULD begin heartbeats only
after service registration" guidance):

1. Dial NATS.
2. Register the `agents` micro service with metadata + endpoints.
3. Call `adapter.Start(shimCtx)` — binds the adapter's long-lived
   watchers (file tailers, marker watchers) to the shim's lifetime
   context, NOT to a per-prompt context. This is the contract the
   Adapter interface documents: prompt-scope cancellation must never
   dismantle the adapter.
4. Start the heartbeat loop (immediate publish, then every `interval`).
5. Start the adapter's event pump.
6. Block on `ctx.Done()`; on signal, drain the connection and close
   the adapter. `Close()` MUST close the channel returned by `Events()`
   so the event pump exits; `Close` is idempotent.

## Integration with orch-spawn

`orch-spawn` launches `orch-agent-shim` as a default sidecar for every
spawned pane (Plan 8 / orch#58). No operator flag is required.

```
orch-spawn claude --project myapp
# → splits pane, starts claude, then forks orch-agent-shim for that pane
# → pane is discoverable via $SRV.INFO.agents within a few seconds
```

### Env propagation

Five env vars are resolved in the parent shell and forwarded explicitly
into the shim process (not inherited via shell env, so they survive
`disown` correctly in both bash and zsh):

| Var           | Source                              |
|---------------|-------------------------------------|
| `ORCH_OWNER`  | `$SESH_OWNER` or `$USER`            |
| `ORCH_OUTFIT` | resolved outfit name (may be empty) |
| `ORCH_ROLE`   | `worker` or `observer`              |
| `SESH_SESSION`| resolved session label              |
| `NATS_URL`    | from sesh hub URL or env            |

### Adapter-less harnesses

Harnesses without a shipped adapter (codex, pi, gemini — Plans 11-13)
cause `orch-agent-shim --help` to exit 2 (adapter-not-found). orch-spawn
detects this and falls back to the existing marker-hook-only path with a
warning to stderr. No operator intervention required.

### Disable knob

Pass `--no-shim` to suppress the shim launch for a specific spawn —
useful when diagnosing shim issues or when running agent-less test panes:

```
orch-spawn claude --project myapp --no-shim
```

### Shim logs

Stderr from each shim is redirected to `~/.cache/orch-shim/<pctN>.log`
(`%` in pane ids replaced with `pct` for safe filenames).

### Teardown / orphan cleanup

`orch-down` kills all running `orch-agent-shim` processes and removes
`~/.cache/orch-shim/`. Shims are normally bound to their pane's shell
lifetime; the orch-down sweep handles abrupt teardowns (e.g. `tmux
kill-session`).

## Wire-compat

Tests in `test/wire-compat/` drive the shim from a Synadia-protocol
caller built against `@synadia-ai/agents` (the upstream TS SDK), so
any wire drift from the spec surfaces immediately. The smoke runner is
`test/test-orch-agent-shim.sh`.

The spawn integration test is `test/test-orch-spawn-shim.sh` — it
starts a test NATS server, forks the shim, and asserts `$SRV.INFO.agents`
returns the pane within 5 s.

## Gemini adapter notes

### AfterAgent quirk
gemini-cli's turn-end event is named `AfterAgent`, **not** `Stop`. The
gemini adapter's marker-watch loop survives post-#94 for the test suite
and as substrate for a future bus-native turn-end detector; the legacy
hook writer wired under `AfterAgent` is gone.

### Transcript-path deferral
gemini stores chat logs at
`~/.gemini/tmp/<scope>/chats/session-<ts>-<sessionId>.jsonl`, where `<scope>`
varies by project context. The CWD→scope encoding is not yet confirmed from
gemini-cli source. Full transcript chunk emission is deferred to a follow-up;
v1 emits only the stop terminator and native Notification query chunks.

See the `TODO(transcript)` comment in `internal/adapter/gemini/gemini.go`.

## Open work

- **Bus-native turn-end detection.** All four adapters
  (`claudecode`, `codex`, `pi`, `gemini`) currently watch
  `~/.cache/orch-stop/<pane>.event` files. Live writers were retired in
  #94; the watch loops are now driven only by the test suites. A
  follow-up issue should refactor each adapter to detect turn-end from
  its native signal (transcript-tail idle, TUI prompt pattern, etc.)
  rather than from a marker file.
- **Per-query reply routing.** The adapters emit §7 query chunks but
  don't wire the caller's reply back through `tmux send-keys`. Reply
  routing requires a reply-subject subscriber — follow-up.
- **pi native Notification.** If pi adds a native mid-turn event
  analogous to claude-code's Notification hook in a future release, the
  pi adapter's synthetic-query heuristic can be replaced with a direct
  watcher without changing the Adapter interface.
