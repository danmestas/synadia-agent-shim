// orch-agent-shim wraps an agent CLI (e.g. claude-code) and exposes it
// via the Synadia Agent Protocol v0.3 on a sesh hub or standalone NATS
// server. See docs/orch-agent-shim.md for the architecture overview.
//
// Usage:
//
//	orch-agent-shim --agent claude-code --pane %37
//
// Resolution order (most explicit wins):
//
//	NATS URL: --nats flag → $NATS_URL → ~/.sesh/hub.nats.url
//	          → ~/.sesh/hub.url (legacy, deprecated) → nats://127.0.0.1:4222
//	Owner:    --owner flag → $ORCH_OWNER → $USER → /etc/passwd lookup
//	Session:  --session flag → $SESH_SESSION → "" (omitted from metadata)
//	CWD:      --cwd flag → tmux display-message -p '#{pane_current_path}'
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
		pane              = flag.String("pane", "", "tmux pane id (e.g. %37) — required")
		owner             = flag.String("owner", "", "owner override (default $ORCH_OWNER or $USER)")
		session           = flag.String("session", "", "session label override (default $SESH_SESSION)")
		natsURL           = flag.String("nats", "", "NATS URL override (default $NATS_URL or ~/.sesh/hub.nats.url; legacy ~/.sesh/hub.url is read as a deprecated fallback)")
		outfit            = flag.String("outfit", "", "outfit name (default $ORCH_OUTFIT)")
		role              = flag.String("role", "", "role override (default $ORCH_ROLE, fallback worker)")
		cwd               = flag.String("cwd", "", "working directory (default resolved via tmux)")
		interval          = flag.Duration("interval", 30*time.Second, "heartbeat interval")
		paneWatchInterval = flag.Duration("pane-watch-interval", 30*time.Second, "how often to poll tmux to see if the bound pane is still alive; empty --pane disables the watchdog")
		taskID            = flag.String("task-id", "", "Sesh-Task-Id envelope header (default $ORCH_TASK_ID, empty omits header)")
	)
	flag.Parse()

	if *agent == "" || *pane == "" {
		flag.Usage()
		return fmt.Errorf("--agent and --pane are required")
	}

	cfg := shim.Config{
		Agent:             *agent,
		Pane:              *pane,
		Owner:             firstNonEmpty(*owner, os.Getenv("ORCH_OWNER"), os.Getenv("USER")),
		Session:           firstNonEmpty(*session, os.Getenv("SESH_SESSION")),
		NATSURL:           shim.ReadNATSURL(*natsURL),
		Outfit:            firstNonEmpty(*outfit, os.Getenv("ORCH_OUTFIT")),
		Role:              firstNonEmpty(*role, os.Getenv("ORCH_ROLE")),
		CWD:               firstNonEmpty(*cwd, resolveCWD(*pane)),
		Interval:          *interval,
		PaneWatchInterval: *paneWatchInterval,
		TaskID:            firstNonEmpty(*taskID, os.Getenv("ORCH_TASK_ID")),
	}

	a, err := buildAdapter(*agent, *pane, cfg.CWD)
	if err != nil {
		return err
	}
	cfg.Adapter = a

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.SetFlags(log.LstdFlags | log.LUTC)
	log.Printf("orch-agent-shim starting: agent=%s pane=%s owner=%s nats=%s",
		cfg.Agent, cfg.Pane, cfg.Owner, cfg.NATSURL)

	return shim.Run(ctx, cfg)
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
