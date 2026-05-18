# orch.signal.> — operator → agent control verbs

`orch.signal.>` is an orch-prefix NATS subject family that carries
operator-to-agent control signals — the bus-native equivalent of `Ctrl-C`
in an interactive REPL. Pure NATS at the wire (no tmux dependency in the
spec); per-adapter implementations decide *how* the harness actually
stops (TUI adapters send tmux `C-c`; future cf-worker / DO adapters
toggle internal flags / alarms).

This is **orch-internal**. It is NOT filed at Synadia. The canonical
`agents.>` subject namespace (Synadia Agent Protocol v0.3) is untouched.

## Wire layout

```
orch.signal.<verb>.<token>.<owner>.<pane-enc>
```

The trailing 3-tuple matches the shim's `agents.prompt.<token>.<owner>.<pane-enc>`
endpoint — so an operator who already knows how to publish a prompt to a
pane knows how to signal it. Subscribe-side: each shim subscribes to
`orch.signal.*.<its-3-tuple>`, so peer panes never see each other's
signals.

## Verbs (v1)

### `orch.signal.interrupt.<3-tuple>`

Body: empty. Fire-and-forget; no reply expected.

Shim behaviour:

1. Cancel the derived `OnPrompt` ctx for the in-flight turn.
2. Call `Adapter.Abort(ctx)` if the adapter implements the `Aborter`
   interface (TUI adapters send `tmux send-keys -t <pane> C-c`).
3. Publish `{"type":"status","data":"aborted"}` on the active reply
   subject (reuses the §6.4 status-chunk shape from the Synadia Agent
   Protocol — subscribers already know how to parse it).
4. Publish the §6.5 zero-byte terminator and release the active-reply
   slot, so a follow-up prompt to the same pane is immediately accepted.

Idempotent. If no turn is active, the verb is a silent no-op. N concurrent
interrupts coalesce — exactly one delivers Abort + status:aborted +
terminator; the rest observe an empty active-reply slot and return.

### `orch.signal.redirect.<3-tuple>`

Body: JSON.

```json
{
  "prompt": "<new prompt text>",
  "reply":  "<optional reply subject for the new turn>"
}
```

Shim behaviour:

1. Perform the full `interrupt` sequence above on the in-flight turn.
2. Dispatch the new prompt via the standard `handlePrompt` machinery —
   the new turn gets its own ack + chunks + terminator on the supplied
   `reply` subject (or a freshly minted `_INBOX.*` if `reply` is empty).

Plain text only in v1 — matches §5.1 plain-text shorthand. No
attachments, no Synadia request envelope. The redirected turn's
traceparent is freshly minted; if you care about correlating the
redirect to a parent trace, set the W3C `traceparent` header on the
publish (NOT yet honoured by the shim — follow-up).

## Room to grow (NOT in v1)

- `orch.signal.pause.<3-tuple>` — stop dispatching new prompts; ack
  inbound but queue.
- `orch.signal.resume.<3-tuple>` — flush the pause queue.
- `orch.signal.snapshot.<3-tuple>` — emit a chunk dump on a side
  channel.

Forward-compat is built in: unknown verbs are logged and ignored, so a
newer operator publishing `orch.signal.<futureverb>.*` against an older
shim degrades to a no-op rather than crashing or misrouting.

## Adapter contract

See `internal/shim/adapter.go` — the `Aborter` interface:

```go
type Aborter interface {
    Abort(ctx context.Context) error
}
```

Adapters whose underlying harness fully honours OnPrompt's ctx don't
need Abort — the shim's per-turn `context.CancelFunc` IS the abort.
Adapters whose harness needs an imperative stop signal (the v1 TUI
REPLs — claude-code, codex, pi, gemini, all running foreground in
tmux panes that don't observe Go ctx) implement Abort to deliver that
signal.

Abort is distinct from Close. Close tears down the adapter for good
(shim shutdown). Abort stops the current turn but leaves the adapter
alive for the next prompt — implementations MUST NOT close channels
or release files that Close is the one to clean up.

## Operator CLI

```sh
orch-interrupt <pane_id|alias>                     # interrupt verb
orch-interrupt <pane_id|alias> --redirect "<text>" # interrupt + redirect
```

See `bin/orch-interrupt`. Resolves pane → 3-tuple via `$SRV.INFO.agents`
(same path `orch-tell` uses), then publishes the verb subject. For
redirect, mints an `_INBOX.orch-redirect.*` reply subject and prints it
so operators can `nats sub` to observe the new turn's chunks.

## Out of scope (filed as follow-ups if needed)

- Synadia upstream of the verb family — orch-internal only.
- Traceparent propagation on redirect bodies — shim mints fresh for v1.
- Multi-pane broadcast (`orch.signal.interrupt.*` wildcard) — every
  shim subscribed with its own 3-tuple already isolates per-pane; a
  wildcard publish would require subjects without per-pane suffixes.
