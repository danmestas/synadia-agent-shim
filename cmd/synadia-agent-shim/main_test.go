package main

import (
	"strings"
	"testing"

	"github.com/danmestas/synadia-agent-shim/shim"
)

// TestResolveLocator covers the --locator flag + --pane deprecation
// path. The CLI surface is small but easy to regress on — pin every
// fork.

func TestResolveLocator_LocatorFlagWins(t *testing.T) {
	// --locator takes precedence over --pane outright (even when both
	// are passed; we don't error on the combination, we just use
	// --locator).
	loc, pane, err := resolveLocator("cmux:surface:30", "%99")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loc.Engine != shim.EngineCmux || loc.Value != "surface:30" {
		t.Errorf("locator: got %+v want cmux/surface:30", loc)
	}
	// Pane is the locator value so back-compat tmux-keyed paths
	// (resolveCWD, marker file paths) still work — for cmux that means
	// they key off the surface ref, which is per-worker stable.
	if pane != "surface:30" {
		t.Errorf("pane back-compat: got %q want surface:30", pane)
	}
}

func TestResolveLocator_PaneFlagInfersTmux(t *testing.T) {
	// Deprecated alias path. --pane %37 becomes Locator{tmux, %37} and
	// the legacy Pane string stays %37 for downstream tmux consumers.
	// The deprecation warning is emitted to stderr — we don't capture
	// it here (would need to redirect os.Stderr which races with the
	// rest of the suite); just confirm the resolution shape.
	loc, pane, err := resolveLocator("", "%37")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loc.Engine != shim.EngineTmux || loc.Value != "%37" {
		t.Errorf("locator: got %+v want tmux/%%37", loc)
	}
	if pane != "%37" {
		t.Errorf("pane: got %q want %%37", pane)
	}
}

func TestResolveLocator_NeitherFlag_DefersToAutodetect(t *testing.T) {
	// When neither --locator nor --pane is passed, resolveLocator
	// returns the zero Locator (signal to withDefaults to run env-var
	// autodetect). Pane is empty too — withDefaults fills it from the
	// detected Locator's value.
	loc, pane, err := resolveLocator("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loc.IsValid() {
		t.Errorf("locator should be zero-valued for autodetect, got %+v", loc)
	}
	if pane != "" {
		t.Errorf("pane should be empty, got %q", pane)
	}
}

func TestResolveLocator_MalformedLocatorErrors(t *testing.T) {
	_, _, err := resolveLocator("foo:bar", "")
	if err == nil {
		t.Fatal("malformed --locator: want error, got nil")
	}
	if !strings.Contains(err.Error(), "--locator") {
		t.Errorf("error should mention --locator flag: %v", err)
	}
}

func TestResolveLocator_ZmxLocator(t *testing.T) {
	loc, pane, err := resolveLocator("zmx:engineer-a", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loc.Engine != shim.EngineZmx || loc.Value != "engineer-a" {
		t.Errorf("locator: got %+v want zmx/engineer-a", loc)
	}
	if pane != "engineer-a" {
		t.Errorf("pane back-compat: got %q want engineer-a", pane)
	}
}
