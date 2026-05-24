// orch-agent-shim wraps an agent CLI (e.g. claude-code) and exposes it
// via the Synadia Agent Protocol v0.3 on a sesh hub or standalone NATS
// server. See docs/orch-agent-shim.md for the architecture overview.
//
// Usage:
//
//	orch-agent-shim --agent claude-code --locator tmux:%37
//	orch-agent-shim --agent claude-code --locator cmux:surface:30
//	orch-agent-shim --agent claude-code --locator zmx:engineer-a
//	orch-agent-shim --agent claude-code --pane %37             # deprecated; tmux only
//
// Resolution order (most explicit wins):
//
//	NATS URL:    --nats flag → $NATS_URL → ~/.sesh/hub.nats.url
//	             → ~/.sesh/hub.url (legacy, deprecated) → nats://127.0.0.1:4222
//	Owner:       --owner flag → $ORCH_OWNER → $USER → /etc/passwd lookup
//	Session:     --session flag → $SESH_SESSION → "" (omitted from metadata)
//	CWD:         --cwd flag → tmux display-message -p '#{pane_current_path}'
//	Instance ID: --instance-id flag → "" (no slug-keyed subjects)
//	Locator:     --locator → autodetect ($CMUX_SURFACE_ID, $ZMX_SESSION,
//	             $TMUX_PANE) → --pane (deprecated, infers tmux:)
//
// Lifetime: process exits when the pane it's bound to dies (SIGCHLD
// from the parent shell does the right thing under most spawn setups;
// orch-spawn additionally backstops with `wait` on a sentinel pid).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/danmestas/synadia-agent-shim/adapter/claudecode"
	"github.com/danmestas/synadia-agent-shim/adapter/codex"
	"github.com/danmestas/synadia-agent-shim/adapter/echo"
	"github.com/danmestas/synadia-agent-shim/adapter/gemini"
	"github.com/danmestas/synadia-agent-shim/adapter/pi"
	"github.com/danmestas/synadia-agent-shim/shim"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "orch-agent-shim: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		agent             = flag.String("agent", "", "agent harness name — required (claude-code in v1; codex/pi/gemini to come in Plans 11-13)")
		pane              = flag.String("pane", "", "DEPRECATED: tmux pane id (e.g. %37). Use --locator tmux:%37 instead. Will be removed in the next shim release.")
		locator           = flag.String("locator", "", "engine-aware surface locator (TYPE:VALUE). Examples: tmux:%37 | cmux:surface:30 | zmx:engineer-a. Default: autodetect via $CMUX_SURFACE_ID, $ZMX_SESSION, or $TMUX_PANE.")
		owner             = flag.String("owner", "", "owner override (default $ORCH_OWNER or $USER)")
		session           = flag.String("session", "", "session label override (default $SESH_SESSION)")
		natsURL           = flag.String("nats", "", "NATS URL override (default $NATS_URL or ~/.sesh/hub.nats.url; legacy ~/.sesh/hub.url is read as a deprecated fallback)")
		outfit            = flag.String("outfit", "", "outfit name (default $ORCH_OUTFIT)")
		role              = flag.String("role", "", "role override (default $ORCH_ROLE, fallback worker)")
		cwd               = flag.String("cwd", "", "working directory (default resolved via tmux)")
		interval          = flag.Duration("interval", 30*time.Second, "heartbeat interval")
		paneWatchInterval = flag.Duration("pane-watch-interval", 30*time.Second, "how often to poll tmux to see if the bound pane is still alive; empty --pane disables the watchdog")
		taskID            = flag.String("task-id", "", "Sesh-Task-Id envelope header (default $ORCH_TASK_ID, empty omits header)")
		instanceID        = flag.String("instance-id", "", "human-readable worker slug; when set, shim adds metadata.instance_id, registers slug-keyed prompt/status endpoints (agents.{prompt,status}.<token>.<owner>.<slug>), and publishes heartbeats on the slug-keyed hb subject. Legacy pct-keyed track stays live unless ORCH_SLUG_DUAL_PUBLISH=0. Must match [a-zA-Z0-9._-]{1,128}.")
	)
	flag.Parse()

	if *agent == "" {
		flag.Usage()
		return fmt.Errorf("--agent is required")
	}

	// Resolve the surface locator. Precedence:
	//   1. --locator TYPE:VALUE (explicit, most specific)
	//   2. --pane VALUE         (deprecated; assumed tmux:VALUE)
	//   3. autodetect via env vars (DetectEngine in shim/engine.go)
	//
	// Steps 2 and 3 are tried in withDefaults; we only handle step 1 +
	// the deprecation warning here. Validation (was anything resolved?)
	// is shim.Config.validate.
	resolvedLoc, paneForBackCompat, err := resolveLocator(*locator, *pane)
	if err != nil {
		flag.Usage()
		return err
	}

	cfg := shim.Config{
		Agent:             *agent,
		Pane:              paneForBackCompat,
		Locator:           resolvedLoc,
		Owner:             firstNonEmpty(*owner, os.Getenv("ORCH_OWNER"), os.Getenv("USER")),
		Session:           firstNonEmpty(*session, os.Getenv("SESH_SESSION")),
		InstanceID:        *instanceID,
		NATSURL:           shim.ReadNATSURL(*natsURL),
		Outfit:            firstNonEmpty(*outfit, os.Getenv("ORCH_OUTFIT")),
		Role:              firstNonEmpty(*role, os.Getenv("ORCH_ROLE")),
		CWD:               firstNonEmpty(*cwd, resolveCWD(paneForBackCompat)),
		Interval:          *interval,
		PaneWatchInterval: *paneWatchInterval,
		TaskID:            firstNonEmpty(*taskID, os.Getenv("ORCH_TASK_ID")),
	}

	a, err := buildAdapter(*agent, paneForBackCompat, cfg.CWD)
	if err != nil {
		return err
	}
	cfg.Adapter = a

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.SetFlags(log.LstdFlags | log.LUTC)
	log.Printf("orch-agent-shim starting: agent=%s pane=%s locator=%s owner=%s nats=%s",
		cfg.Agent, cfg.Pane, cfg.Locator, cfg.Owner, cfg.NATSURL)

	return shim.Run(ctx, cfg)
}

// resolveLocator picks the surface locator from the CLI flags. Returns:
//
//   - the resolved Locator (zero value when neither flag was passed —
//     withDefaults will then attempt env-var autodetection),
//   - the back-compat Pane string (used for adapter construction, the
//     pane-watchdog, and metadata.pane_id),
//   - an error for malformed --locator or empty --pane fallback.
//
// --locator wins outright; --pane is a deprecated alias that infers
// engine=tmux and emits a stderr deprecation warning so operators see
// the migration prompt without breaking their scripts. Passing neither
// is fine here — autodetect runs in shim.withDefaults.
func resolveLocator(locator, pane string) (shim.Locator, string, error) {
	if locator != "" {
		loc, err := shim.ParseLocator(locator)
		if err != nil {
			return shim.Locator{}, "", fmt.Errorf("--locator: %w", err)
		}
		// Keep Pane populated when the resolved engine is tmux so the
		// existing tmux-keyed paths (resolveCWD, metadata.pane_id,
		// pane-watchdog) still work. For cmux/zmx the legacy Pane field
		// carries the engine-native id so the subject token derivation
		// has something — orch's registry can read pane_id and find the
		// worker; the typed locator metadata field is the migration
		// path off pane_id.
		return loc, loc.Value, nil
	}
	if pane != "" {
		// Deprecated alias path. Emit one warning to stderr (not log,
		// to avoid the LstdFlags prefix — operators reading the warning
		// want a clean line) and infer tmux semantics.
		fmt.Fprintln(os.Stderr, "orch-agent-shim: --pane is deprecated; use --locator tmux:"+pane+" instead. --pane will be removed in the next shim release.")
		return shim.Locator{Engine: shim.EngineTmux, Value: pane}, pane, nil
	}
	// Neither flag passed — return zero-value so withDefaults runs the
	// env-var autodetection path. The caller-side validation
	// (shim.Config.validate) handles the "still nothing resolved" case
	// with a clearer error than we could give from here.
	return shim.Locator{}, "", nil
}

func buildAdapter(agent, pane, cwd string) (shim.Adapter, error) {
	switch agent {
	case "claude-code", "claude":
		// Normalize: accept both "claude" (orch-spawn arg) and the
		// canonical "claude-code" (Synadia §C identifier).
		return claudecode.New(pane, cwd), nil
	case "codex":
		// codex adapter: tails ~/.codex/sessions rollout JSONL, watches the
		// orch-stop marker, and emits synthetic query chunks on idle detection.
		return codex.New(pane), nil
	case "gemini":
		// gemini-cli adapter. Uses AfterAgent (NOT Stop) for turn-end
		// detection; native Notification events for query chunks.
		// CWD is not used in v1 (transcript-path deferral — see
		// internal/adapter/gemini/gemini.go TODO comment).
		return gemini.New(pane), nil
	case "pi":
		return pi.New(pane, cwd), nil
	case "echo":
		// echo is the reference adapter: no external tooling required.
		// Use for smoke tests, protocol demonstrations, and as a template
		// when writing a new harness adapter.
		return echo.New(), nil
	default:
		return nil, fmt.Errorf("no adapter for agent %q (supported: claude-code, codex, gemini, pi, echo)", agent)
	}
}

// resolveCWD asks tmux for the pane's current path. Failure modes
// (tmux not running, pane gone) are non-fatal — we fall back to the
// shim's own cwd and log a warning.
func resolveCWD(pane string) string {
	if pane == "" {
		return ""
	}
	out, err := exec.Command("tmux", "display-message", "-p", "-t", pane, "#{pane_current_path}").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
