package shim

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Integration tests for the engine-aware-send PR: confirm the new
// `engine` and `locator` fields appear in service metadata + heartbeat
// payloads, and confirm the back-compat path (only Pane set, no
// explicit Locator) synthesises a tmux locator.
//
// Wire-level tests (per-engine command dispatch) live in engine_test.go.
// These tests live one layer up — at the point where Locator metadata
// is meant to reach the bus.

func TestServiceMetadata_IncludesEngineAndLocatorFields(t *testing.T) {
	// Operator-visible contract: $SRV.INFO.agents metadata carries both
	// the typed `locator` field and the `engine` identifier alongside
	// the legacy `pane_id`. orch's registry will adopt these at its own
	// pace; until then, pane_id continues to work for the back-compat
	// reader.
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:   "claude-code",
		Pane:    "%37",
		Owner:   "tester",
		Adapter: &nopAdapter{},
		// No Locator passed — withDefaults must synthesize tmux:%37
		// from Pane.
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	msg, err := nc.Request("$SRV.INFO.agents", nil, time.Second)
	if err != nil {
		t.Fatalf("$SRV.INFO.agents: %v", err)
	}
	var info serviceInfo
	if err := json.Unmarshal(msg.Data, &info); err != nil {
		t.Fatalf("decode INFO: %v\nraw: %s", err, msg.Data)
	}

	if info.Metadata["pane_id"] != "%37" {
		t.Errorf("metadata.pane_id (back-compat): got %q want %%37", info.Metadata["pane_id"])
	}
	if info.Metadata["engine"] != "tmux" {
		t.Errorf("metadata.engine: got %q want tmux", info.Metadata["engine"])
	}
	if info.Metadata["locator"] != "tmux:%37" {
		t.Errorf("metadata.locator: got %q want tmux:%%37", info.Metadata["locator"])
	}
}

func TestServiceMetadata_CmuxLocatorRenders(t *testing.T) {
	// Same test but with an explicit cmux Locator. Confirms the
	// engine+locator fields render the cmux engine identifier and the
	// full "cmux:surface:30" wire form.
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:   "claude-code",
		Pane:    "surface:30", // best-effort back-compat token
		Owner:   "tester",
		Adapter: &nopAdapter{},
		Locator: Locator{Engine: EngineCmux, Value: "surface:30"},
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	msg, err := nc.Request("$SRV.INFO.agents", nil, time.Second)
	if err != nil {
		t.Fatalf("$SRV.INFO.agents: %v", err)
	}
	var info serviceInfo
	if err := json.Unmarshal(msg.Data, &info); err != nil {
		t.Fatalf("decode INFO: %v", err)
	}
	if info.Metadata["engine"] != "cmux" {
		t.Errorf("metadata.engine: got %q want cmux", info.Metadata["engine"])
	}
	if info.Metadata["locator"] != "cmux:surface:30" {
		t.Errorf("metadata.locator: got %q want cmux:surface:30", info.Metadata["locator"])
	}
}

func TestServiceMetadata_ZmxLocatorRenders(t *testing.T) {
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:   "claude-code",
		Pane:    "engineer-a",
		Owner:   "tester",
		Adapter: &nopAdapter{},
		Locator: Locator{Engine: EngineZmx, Value: "engineer-a"},
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	msg, err := nc.Request("$SRV.INFO.agents", nil, time.Second)
	if err != nil {
		t.Fatalf("$SRV.INFO.agents: %v", err)
	}
	var info serviceInfo
	if err := json.Unmarshal(msg.Data, &info); err != nil {
		t.Fatalf("decode INFO: %v", err)
	}
	if info.Metadata["engine"] != "zmx" {
		t.Errorf("metadata.engine: got %q want zmx", info.Metadata["engine"])
	}
	if info.Metadata["locator"] != "zmx:engineer-a" {
		t.Errorf("metadata.locator: got %q want zmx:engineer-a", info.Metadata["locator"])
	}
}

func TestHeartbeatPayload_CarriesEngineAndLocator(t *testing.T) {
	// Heartbeats (§8.2) MUST publish engine + locator so downstream
	// consumers building topology views can render the worker's
	// surface natively without having to first probe $SRV.INFO.
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:    "claude-code",
		Pane:     "surface:30",
		Owner:    "u",
		Adapter:  &nopAdapter{},
		Locator:  Locator{Engine: EngineCmux, Value: "surface:30"},
		Interval: time.Second, // §8.2 floor for fast assertion
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	sub, err := nc.SubscribeSync("agents.hb.cc.u.pctsurface:30") // Pane→encodePane
	if err != nil {
		// Subject derivation: encodePane strips a leading "%" and
		// prefixes "pct". "surface:30" has no "%", so encoded is
		// "pctsurface:30". (The colon is fine in subjects, but use the
		// derived form so this test stays stable across encodePane
		// tweaks.)
		t.Fatalf("sub hb: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no heartbeat received: %v", err)
	}
	var p heartbeatPayload
	if err := json.Unmarshal(msg.Data, &p); err != nil {
		t.Fatalf("hb payload malformed: %v\nraw: %s", err, msg.Data)
	}
	if p.Engine != "cmux" {
		t.Errorf("hb.engine: got %q want cmux", p.Engine)
	}
	if p.Locator != "cmux:surface:30" {
		t.Errorf("hb.locator: got %q want cmux:surface:30", p.Locator)
	}
}

func TestHeartbeatPayload_TmuxBackCompat_OmitsAreFilled(t *testing.T) {
	// Pre-engine-aware shim's heartbeat had no engine/locator fields.
	// Post-PR, even a Pane-only caller produces tmux:<pane> via
	// withDefaults, so the heartbeat carries the new fields without
	// the caller having to supply --locator.
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:    "claude-code",
		Pane:     "%1",
		Owner:    "u",
		Adapter:  &nopAdapter{},
		Interval: time.Second,
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	sub, err := nc.SubscribeSync("agents.hb.cc.u.pct1")
	if err != nil {
		t.Fatalf("sub hb: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no heartbeat received: %v", err)
	}
	var p heartbeatPayload
	if err := json.Unmarshal(msg.Data, &p); err != nil {
		t.Fatalf("hb payload malformed: %v\nraw: %s", err, msg.Data)
	}
	if p.Engine != "tmux" {
		t.Errorf("hb.engine (back-compat default): got %q want tmux", p.Engine)
	}
	if p.Locator != "tmux:%1" {
		t.Errorf("hb.locator (back-compat synth): got %q want tmux:%%1", p.Locator)
	}
}

func TestWithDefaults_PaneAlone_SynthesisesTmuxLocator(t *testing.T) {
	// Back-compat: pre-engine-aware callers pass Pane but no Locator.
	// withDefaults turns that into Locator{tmux, Pane}. This is the
	// hot-path for orch-spawn'd workers running the existing tmux
	// engine — they continue to work without flag changes on the orch
	// side until orch adopts --locator.
	cfg := withDefaults(Config{Agent: "claude-code", Pane: "%5", Adapter: &nopAdapter{}})
	if cfg.Locator.Engine != EngineTmux {
		t.Errorf("synth engine: got %q want tmux", cfg.Locator.Engine)
	}
	if cfg.Locator.Value != "%5" {
		t.Errorf("synth value: got %q want %%5", cfg.Locator.Value)
	}
}

func TestWithDefaults_LocatorAlone_PopulatesBackCompatPane(t *testing.T) {
	// Reciprocal back-compat: when withDefaults sees a Locator but no
	// Pane (the orch-spawn-cmux/zmx path), it mirrors the locator
	// value into Pane so downstream tmux-shaped consumers
	// (metadata.pane_id, the subject token derivation, the
	// pane-watchdog when applicable) have something.
	cfg := withDefaults(Config{
		Agent:   "claude-code",
		Adapter: &nopAdapter{},
		Locator: Locator{Engine: EngineZmx, Value: "engineer-a"},
	})
	if cfg.Pane != "engineer-a" {
		t.Errorf("Pane should mirror Locator.Value when empty: got %q want engineer-a", cfg.Pane)
	}
	if cfg.Locator.Engine != EngineZmx {
		t.Errorf("Locator preserved: got %q want zmx", cfg.Locator.Engine)
	}
}

func TestValidate_NoLocatorAndNoPane_FailsClear(t *testing.T) {
	// Operator-facing error: when neither --locator nor --pane is
	// passed AND the env-var autodetect comes up empty, the shim
	// refuses to start with a clear list of acceptable sources.
	cfg := Config{Agent: "claude-code", Adapter: &nopAdapter{}}
	err := cfg.validate()
	if err == nil {
		t.Fatal("validate without locator/pane: want error, got nil")
	}
	for _, want := range []string{"locator", "tmux", "cmux", "zmx", "--locator"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error message missing %q: %v", want, err)
		}
	}
}
