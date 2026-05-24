// engine.go: persistence-engine detection + engine-native send dispatch.
//
// The shim's job has always been the same — deliver an inbound NATS prompt
// to the agent's PTY — but the original implementation hard-coded
// `tmux send-keys`. orch now supports three persistence engines:
//
//   - tmux (default; long-standing)
//   - cmux (orch#207, 2026-05-24)
//   - zmx  (orch#210, 2026-05-24)
//
// Each engine has its own send verb and locator shape:
//
//	tmux: tmux send-keys -l -t %ID text  +  tmux send-keys -t %ID Enter
//	cmux: cmux send --surface <ref> -- <text>\n
//	zmx:  zmx send <session-name> <text>\r
//
// The engine is detected from environment variables the engine itself
// injects into every spawned pane/session:
//
//	tmux → $TMUX (full server-socket path) + $TMUX_PANE (pane id, "%37")
//	cmux → $CMUX_SURFACE_ID (UUID; auto-set in cmux terminals)
//	zmx  → $ZMX_SESSION (session name; injected automatically by zmx)
//
// Detection precedence is "most specific wins": cmux and zmx pane often
// runs INSIDE a tmux server-of-record (cmux launches its own embedded
// terminals, zmx sets up a pty), but those won't set $TMUX_PANE for the
// shim because the shim is spawned directly into the engine-native
// surface. We check the engine-specific vars first and only fall back to
// $TMUX if neither cmux nor zmx markers are present.
//
// Callers wanting to override detection use the `--locator TYPE:VALUE`
// flag, which decodes into a Locator{Engine, Value} and short-circuits
// env-var sniffing.

package shim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Engine identifies a persistence engine. Drives the SendKeys dispatch
// table and the heartbeat metadata `engine` field.
type Engine string

const (
	// EngineUnknown is the zero value — detection failed and no
	// --locator override was supplied. The shim refuses to start in
	// this state because there's no engine-native send verb to
	// dispatch to.
	EngineUnknown Engine = ""

	// EngineTmux is the long-standing default. Locator value is the raw
	// tmux pane id (e.g. "%37"). Detection: $TMUX_PANE / $TMUX.
	EngineTmux Engine = "tmux"

	// EngineCmux is orch's cmux backend (orch#207). Locator value is
	// the cmux surface ref (e.g. "surface:30" or a UUID). Detection:
	// $CMUX_SURFACE_ID.
	EngineCmux Engine = "cmux"

	// EngineZmx is orch's zmx backend (orch#210). Locator value is the
	// zmx session name (e.g. "engineer-a"). Detection: $ZMX_SESSION.
	EngineZmx Engine = "zmx"
)

// String makes Engine satisfy fmt.Stringer for log lines and metadata.
func (e Engine) String() string { return string(e) }

// Locator binds an engine to its engine-native target identifier. The
// shim carries one of these for the lifetime of the run; it's published
// as the `locator` heartbeat metadata field (engine + ":" + value) and
// drives the engine-native send verb.
//
// The zero value Locator{} has Engine == EngineUnknown and is invalid
// for sending. Callers MUST detect (DetectEngine) or parse (ParseLocator)
// to populate it.
type Locator struct {
	Engine Engine
	// Value is the engine-native identifier:
	//
	//   tmux → pane id ("%37")
	//   cmux → surface ref ("surface:30") or UUID
	//   zmx  → session name ("engineer-a")
	Value string
}

// String returns the typed wire form "engine:value" used by the
// --locator flag and the heartbeat `locator` metadata field. The zero
// Locator returns "unknown:" — callers should validate IsValid first.
func (l Locator) String() string {
	return fmt.Sprintf("%s:%s", l.Engine, l.Value)
}

// IsValid reports whether the Locator has a known engine and a
// non-empty value. Used by the CLI to fail fast on a malformed
// --locator instead of waiting until the first send attempt.
func (l Locator) IsValid() bool {
	switch l.Engine {
	case EngineTmux, EngineCmux, EngineZmx:
		return l.Value != ""
	default:
		return false
	}
}

// DetectEngine inspects the process environment and returns the engine
// the shim is running under, plus an inferred Locator. The lookup
// function defaults to os.Getenv when nil — tests pass a stub.
//
// Precedence (most specific wins; see file header for rationale):
//
//  1. cmux  → $CMUX_SURFACE_ID present
//  2. zmx   → $ZMX_SESSION present
//  3. tmux  → $TMUX_PANE present (preferred — it's the pane id we'd
//             pass to `tmux send-keys -t`) OR $TMUX present (server
//             socket, only as a fallback signal that we're inside tmux)
//
// When tmux is detected via $TMUX without $TMUX_PANE, the returned
// Locator has Engine=EngineTmux but Value="" — the caller must supply
// the pane id explicitly via --locator. This is the rare case (a shim
// launched manually inside tmux without orch-spawn setting $TMUX_PANE).
//
// Returns (Locator{EngineUnknown, ""}, false) when no engine marker
// was found.
func DetectEngine(getenv func(string) string) (Locator, bool) {
	if getenv == nil {
		getenv = os.Getenv
	}
	if v := getenv("CMUX_SURFACE_ID"); v != "" {
		return Locator{Engine: EngineCmux, Value: v}, true
	}
	if v := getenv("ZMX_SESSION"); v != "" {
		return Locator{Engine: EngineZmx, Value: v}, true
	}
	if v := getenv("TMUX_PANE"); v != "" {
		return Locator{Engine: EngineTmux, Value: v}, true
	}
	if getenv("TMUX") != "" {
		// Inside a tmux server but no $TMUX_PANE — the engine is tmux,
		// the locator value isn't known. Caller (cmd/main.go) reports a
		// clear error so the operator can pass --locator tmux:%ID.
		return Locator{Engine: EngineTmux, Value: ""}, true
	}
	return Locator{}, false
}

// ParseLocator decodes a --locator flag value of the form "engine:value"
// or the legacy bare "%ID" / pane-id form (which is interpreted as
// engine=tmux for back-compat with the deprecated --pane flag).
//
// Examples:
//
//	"tmux:%37"        → {EngineTmux, "%37"}
//	"cmux:surface:30" → {EngineCmux, "surface:30"}   (note: cmux
//	                     refs contain ":" so we split only on the
//	                     FIRST colon, preserving the rest verbatim)
//	"zmx:engineer-a"  → {EngineZmx, "engineer-a"}
//	"%64"             → {EngineTmux, "%64"}   (legacy --pane back-compat)
//
// Returns an error for an empty value or an unrecognised engine token.
func ParseLocator(s string) (Locator, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Locator{}, errors.New("empty locator")
	}
	// Legacy --pane back-compat: a bare value starting with "%" is a
	// raw tmux pane id. Map to tmux so deprecated callers keep working.
	if strings.HasPrefix(s, "%") {
		return Locator{Engine: EngineTmux, Value: s}, nil
	}
	// "engine:value" — split on the FIRST colon only. cmux surface refs
	// look like "surface:30", so the typed form is "cmux:surface:30"
	// and the second-and-following colons are part of the value.
	idx := strings.IndexByte(s, ':')
	if idx <= 0 || idx == len(s)-1 {
		return Locator{}, fmt.Errorf("locator %q: want engine:value (e.g. tmux:%%37, cmux:surface:30, zmx:session-name)", s)
	}
	engTok := s[:idx]
	val := s[idx+1:]
	switch Engine(engTok) {
	case EngineTmux:
		return Locator{Engine: EngineTmux, Value: val}, nil
	case EngineCmux:
		return Locator{Engine: EngineCmux, Value: val}, nil
	case EngineZmx:
		return Locator{Engine: EngineZmx, Value: val}, nil
	default:
		return Locator{}, fmt.Errorf("locator %q: unknown engine %q (supported: tmux, cmux, zmx)", s, engTok)
	}
}

// SendFunc is the seam adapters use to deliver a prompt's literal text
// to the bound surface. The default production implementation is
// LocatorSendKeys (engine-aware dispatch); tests inject a recorder.
//
// Implementations MUST append a trailing Enter so the in-pane REPL
// submits the prompt — the contract for callers (claude-code, codex,
// pi, gemini adapters) is "the text and the Enter both arrive".
type SendFunc func(loc Locator, text string) error

// InterruptFunc is the engine-aware analogue of "send Ctrl-C" used by
// the Aborter implementation. Each engine has a different way to
// deliver an interrupt to its surface (see LocatorInterrupt).
type InterruptFunc func(loc Locator) error

// LocatorSendKeys is the production SendFunc: dispatches to the
// engine-native send verb based on loc.Engine. Empty values are
// rejected (returning an error) rather than silently no-op — a misrouted
// prompt should surface immediately, not be swallowed.
//
// Per-engine behaviour:
//
//	tmux: tmux send-keys -l -t <pane> <text>
//	      tmux send-keys    -t <pane> Enter
//	      (literal + separate Enter — the original behaviour; -l
//	      suppresses key-spec interpretation so prompts containing
//	      "C-c", "Up", etc. aren't reinterpreted)
//
//	cmux: cmux send --surface <ref> -- <text>\n
//	      (single invocation; cmux send interprets "\n" as Enter per
//	      its --help output)
//
//	zmx:  zmx send <session> <text>\r
//	      (single invocation; zmx send forwards raw bytes to the PTY,
//	      \r is the canonical "submit" character for TUI prompts)
//
// Returns the first underlying command error wrapped with the engine
// name for log-line clarity.
func LocatorSendKeys(loc Locator, text string) error {
	return locatorSend(context.Background(), loc, text, runCommand)
}

// locatorSend is the testable core of LocatorSendKeys. `runCmd` is the
// command-runner seam — tests pass a recorder that captures invocations
// without shelling out.
func locatorSend(ctx context.Context, loc Locator, text string, runCmd commandRunner) error {
	if !loc.IsValid() {
		return fmt.Errorf("send: invalid locator %s", loc)
	}
	switch loc.Engine {
	case EngineTmux:
		// Two-step: literal text, then a separate Enter. -l prevents
		// "C-c" / "Up" / "Enter" in prompt body from being interpreted
		// as key specs. The Enter is a deliberately-NON-literal second
		// call so tmux DOES interpret it as the key.
		if err := runCmd(ctx, "tmux", "send-keys", "-l", "-t", loc.Value, text); err != nil {
			return fmt.Errorf("tmux: send-keys text: %w", err)
		}
		if err := runCmd(ctx, "tmux", "send-keys", "-t", loc.Value, "Enter"); err != nil {
			return fmt.Errorf("tmux: send-keys enter: %w", err)
		}
		return nil
	case EngineCmux:
		// `cmux send --surface <ref> -- <text>\n`. The "--" stops flag
		// parsing so a prompt body starting with "-" doesn't get
		// misinterpreted as a flag. The trailing "\n" is cmux's
		// documented escape for Enter.
		if err := runCmd(ctx, "cmux", "send", "--surface", loc.Value, "--", text+"\n"); err != nil {
			return fmt.Errorf("cmux: send: %w", err)
		}
		return nil
	case EngineZmx:
		// `zmx send <session> <text>\r`. zmx forwards raw bytes; \r is
		// the canonical "Enter" character for most REPLs. We deliver
		// the text and the \r in one invocation rather than two —
		// zmx's send semantics are atomic-per-call.
		if err := runCmd(ctx, "zmx", "send", loc.Value, text+"\r"); err != nil {
			return fmt.Errorf("zmx: send: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("send: engine %q unsupported", loc.Engine)
	}
}

// LocatorInterrupt is the engine-aware "deliver an interrupt to the
// surface" verb, called from each adapter's Aborter.Abort path on the
// orch.signal.interrupt event.
//
// Per-engine behaviour:
//
//	tmux: tmux send-keys -t <pane> C-c
//	cmux: cmux send-key  --surface <ref> ctrl+c
//	zmx:  zmx send <session> $'\x03'   (raw Ctrl-C byte)
//
// Best-effort: a failed interrupt is logged at the call site; the shim
// keeps running.
func LocatorInterrupt(loc Locator) error {
	return locatorInterrupt(context.Background(), loc, runCommand)
}

// locatorInterrupt is the testable core. See locatorSend for the
// runCmd seam rationale.
func locatorInterrupt(ctx context.Context, loc Locator, runCmd commandRunner) error {
	if !loc.IsValid() {
		return fmt.Errorf("interrupt: invalid locator %s", loc)
	}
	switch loc.Engine {
	case EngineTmux:
		return runCmd(ctx, "tmux", "send-keys", "-t", loc.Value, "C-c")
	case EngineCmux:
		// cmux exposes send-key for special keys; ctrl+c is the
		// documented form.
		return runCmd(ctx, "cmux", "send-key", "--surface", loc.Value, "ctrl+c")
	case EngineZmx:
		// zmx send forwards raw bytes — 0x03 is the SIGINT (Ctrl-C)
		// character that any line-disciplined PTY interprets as
		// "interrupt foreground process".
		return runCmd(ctx, "zmx", "send", loc.Value, "\x03")
	default:
		return fmt.Errorf("interrupt: engine %q unsupported", loc.Engine)
	}
}

// commandRunner is the seam used by locatorSend / locatorInterrupt so
// tests can recording all invocations without shelling out. Production
// uses runCommand which actually exec's the command.
type commandRunner func(ctx context.Context, name string, args ...string) error

// runCommand is the production commandRunner. Exec's the named command
// and returns the combined exit status. stdout/stderr are discarded —
// the exit code is the contract (tmux / cmux / zmx all report success
// or failure through the exit code; their stderr is debug noise on
// failure paths).
func runCommand(ctx context.Context, name string, args ...string) error {
	return exec.CommandContext(ctx, name, args...).Run()
}
