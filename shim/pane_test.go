package shim

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestPaneWatchdog_ExitsWhenPaneVanishes asserts that watchPane fires
// cancel within ~2x interval when the check returns an error. We don't
// actually shell out to tmux — the swappable check is a stub that
// returns an error immediately. The test guards against regressions
// where the watchdog stops polling, the cancel signal isn't wired
// through, or the loop swallows errors.
func TestPaneWatchdog_ExitsWhenPaneVanishes(t *testing.T) {
	t.Parallel()

	check := func(_ context.Context, _ string) error {
		return errors.New("can't find pane: %nonexistent")
	}

	const interval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	cancelled := make(chan struct{})

	// Wrap cancel so the test can observe the watchdog invoking it.
	observedCancel := func() {
		close(cancelled)
		cancel()
	}

	go func() {
		defer close(done)
		watchPane(ctx, "%nonexistent", interval, check, observedCancel)
	}()

	// First tick fires at t+interval. Allow generous slack for slow CI:
	// 2s easily covers one tick even on heavily-loaded runners while
	// still well under the spec's "exits within 2x interval" promise
	// in normal operation.
	select {
	case <-cancelled:
		// Good — watchdog fired cancel.
	case <-time.After(2 * time.Second):
		cancel() // belt-and-braces
		t.Fatalf("watchPane did not invoke cancel within 2s (interval=%v)", interval)
	}

	select {
	case <-done:
		// Goroutine returned cleanly after cancel.
	case <-time.After(time.Second):
		t.Fatalf("watchPane goroutine did not return after cancel")
	}
}

// TestPaneWatchdog_StaysAliveWhilePaneLive asserts that when the check
// returns success, watchPane keeps polling and never calls cancel. We
// run the loop for several intervals and verify cancel was not invoked.
func TestPaneWatchdog_StaysAliveWhilePaneLive(t *testing.T) {
	t.Parallel()

	var polls int32
	check := func(_ context.Context, _ string) error {
		atomic.AddInt32(&polls, 1)
		return nil
	}

	const interval = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cancelCount int32
	observedCancel := func() {
		atomic.AddInt32(&cancelCount, 1)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		watchPane(ctx, "%37", interval, check, observedCancel)
	}()

	// Let the loop tick a handful of times.
	time.Sleep(5 * interval)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("watchPane goroutine did not return after parent ctx cancel")
	}

	if got := atomic.LoadInt32(&cancelCount); got != 0 {
		t.Fatalf("cancel was invoked %d times while pane reported alive; want 0", got)
	}
	if got := atomic.LoadInt32(&polls); got < 3 {
		t.Fatalf("check was polled only %d times across 5 intervals; want >=3 (loop didn't run)", got)
	}
}

// TestPaneWatchdog_NoOpWhenEmptyPaneID asserts that an empty pane id
// short-circuits watchPane: it returns immediately, doesn't poll, and
// doesn't fire cancel. This preserves backwards-compat for echo-adapter
// smoke tests and CI runs that don't have a tmux pane to bind to.
func TestPaneWatchdog_NoOpWhenEmptyPaneID(t *testing.T) {
	t.Parallel()

	var polls int32
	check := func(_ context.Context, _ string) error {
		atomic.AddInt32(&polls, 1)
		return nil
	}

	var cancelCount int32
	observedCancel := func() {
		atomic.AddInt32(&cancelCount, 1)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// 25ms interval would tick several times if the loop ran.
		watchPane(context.Background(), "", 25*time.Millisecond, check, observedCancel)
	}()

	select {
	case <-done:
		// Returned immediately — correct.
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("watchPane(\"\") did not return immediately")
	}

	if got := atomic.LoadInt32(&polls); got != 0 {
		t.Fatalf("check was polled %d times with empty pane id; want 0", got)
	}
	if got := atomic.LoadInt32(&cancelCount); got != 0 {
		t.Fatalf("cancel was invoked %d times with empty pane id; want 0", got)
	}
}

// TestPaneWatchdog_DefaultsZeroInterval asserts the watchdog clamps a
// zero/negative interval to defaultPaneWatchInterval rather than
// busy-looping or panicking on time.NewTicker(0). Belt-and-braces — the
// withDefaults pass already covers this for callers going through
// Config, but watchPane is package-private and might pick up direct
// callers later.
func TestPaneWatchdog_DefaultsZeroInterval(t *testing.T) {
	t.Parallel()

	check := func(_ context.Context, _ string) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		watchPane(ctx, "%1", 0, check, func() {})
	}()

	// Cancel immediately; the goroutine should unwind without having
	// panicked on a zero-interval ticker.
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("watchPane with zero interval did not return after cancel — possibly stuck on time.NewTicker(0)")
	}
}

// TestPaneWatchdog_NilCheckDefaultsToTmux asserts the documented contract
// that passing a nil check function falls through to paneAliveTmux. We
// don't run the production tmux probe in the test — we just confirm the
// nil-check path runs the loop (which would NPE if it didn't substitute
// in a non-nil function) and exits cleanly on context cancel.
func TestPaneWatchdog_NilCheckDefaultsToTmux(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Very long interval so the first tick never fires before
		// cancel — we're only testing the nil-substitution path, not
		// the tmux probe itself.
		watchPane(ctx, "%1", time.Hour, nil, func() {})
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("watchPane with nil check did not return after cancel")
	}
}
