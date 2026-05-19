// pane.go: watchdog that self-cancels the shim when its bound tmux pane
// disappears.
//
// Why this exists: orch-spawn launches the shim as a sibling process of
// the claude-code (or codex/pi/gemini) CLI inside a tmux pane. When the
// operator runs `tmux kill-pane -t %N`, tmux kills the CLI but the shim
// has no parent-child relationship to it — the shim is reparented to
// init/launchd and keeps publishing heartbeats forever. After a day of
// normal use an operator workstation accumulates dozens of orphan shims
// and orch-registry reports all of them as alive.
//
// Fix shape: poll `tmux display-message -t <pane> -p '#{pane_id}'` on a
// configurable interval (default 30s, matching the heartbeat cadence so
// orphan-detection latency is bounded by the same number operators
// already think about). tmux exits non-zero when the pane is gone; that
// non-zero is the watchdog's signal to cancel the shim's root context,
// which unwinds Run() → drains the NATS connection → exits the process.
//
// Empty `--pane=""` disables the watchdog (preserves CI / non-tmux use:
// echo-adapter smoke tests, conformance harness, etc.).
//
// Paired orch-side bug: github.com/danmestas/orch/issues/167.

package shim

import (
	"context"
	"log"
	"os/exec"
	"time"
)

// defaultPaneWatchInterval matches the default heartbeat cadence (§8.2).
// Orphan detection latency = 2x this in the worst case, so 30s gives a
// dead pane up to ~1min before the shim self-exits. Operators rarely
// notice; orch-registry consumers care that it happens at all.
const defaultPaneWatchInterval = 30 * time.Second

// paneAliveCheck is the signature for "is this tmux pane still there?"
// probes. Returns nil if the pane exists, error otherwise. Tests inject
// a stub implementation; production calls paneAliveTmux.
//
// The function variant (rather than an interface) keeps the watchdog
// loop a single ~30-line function with no struct ceremony — the only
// state it carries is the timer.
type paneAliveCheck func(ctx context.Context, pane string) error

// paneAliveTmux is the production paneAliveCheck. Runs `tmux
// display-message -t <pane> -p '#{pane_id}'` and reports the exit
// status. The output is discarded — only the exit code matters. tmux
// returns 1 with "can't find pane" on stderr when the pane is gone; we
// don't parse the message because the exit code is the contract.
func paneAliveTmux(ctx context.Context, pane string) error {
	cmd := exec.CommandContext(ctx, "tmux", "display-message", "-t", pane, "-p", "#{pane_id}")
	// Discard stdout/stderr — exit code is the signal.
	return cmd.Run()
}

// watchPane polls `check` on `interval` and calls `cancel` when the
// pane is gone. Returns when ctx is done or after cancel has fired.
//
// Empty `pane` is a no-op: returns immediately without polling. This
// preserves backwards-compat for the echo adapter and any non-tmux CI
// caller that constructs a Config{} without a pane id.
//
// A nil `check` defaults to paneAliveTmux — call sites that already
// know the production check is what they want can pass nil and stay
// out of the implementation's way. Tests pass a stub.
//
// Designed to be called as `go watchPane(...)` from RunWithConn.
func watchPane(ctx context.Context, pane string, interval time.Duration, check paneAliveCheck, cancel context.CancelFunc) {
	if pane == "" {
		// Non-tmux caller — nothing to watch. Returning here keeps the
		// goroutine count honest (caller can still `go watchPane(...)`
		// unconditionally without leaking a goroutine that never exits).
		return
	}
	if interval <= 0 {
		interval = defaultPaneWatchInterval
	}
	if check == nil {
		check = paneAliveTmux
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := check(ctx, pane); err != nil {
				// ctx.Err() check guards against the race where the
				// shim is already shutting down — we don't want to log
				// "pane gone" when really the context was cancelled
				// out from under us.
				if ctx.Err() != nil {
					return
				}
				log.Printf("shim: pane %s is gone (%v), cancelling shim context", pane, err)
				cancel()
				return
			}
		}
	}
}
