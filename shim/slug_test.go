package shim

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// -----------------------------------------------------------------------------
// Issue #19 (orch#181) — --instance-id slug flag + dual-publish.
//
// Acceptance per the issue body:
//
//   - --instance-id ce-worker launches, subjects published on legacy +
//     slug tracks (default ORCH_SLUG_DUAL_PUBLISH=1).
//   - no --instance-id → legacy only (back-compat).
//   - ORCH_SLUG_DUAL_PUBLISH=0 with --instance-id → slug-only.
//   - $SRV.INFO.agents includes instance_id when slug set; pane_id always.
//   - slug validation rejects bad input at startup (not lazily).
// -----------------------------------------------------------------------------

func TestValidate_InstanceID_AcceptsValidSlugs(t *testing.T) {
	// Issue #19: ^[a-zA-Z0-9._-]{1,128}$. Cover the corners — single
	// char (lower bound), all classes mixed, the 128-char upper bound.
	cases := []string{
		"a",
		"ce-worker",
		"engineer.42",
		"my_worker-001",
		"X.y_z-9",
		strings.Repeat("a", 128),
	}
	for _, slug := range cases {
		cfg := Config{Agent: "claude-code", Pane: "%1", Adapter: &nopAdapter{}, InstanceID: slug}
		if err := cfg.validate(); err != nil {
			t.Errorf("slug %q should validate, got: %v", slug, err)
		}
	}
}

func TestValidate_InstanceID_RejectsInvalidSlugs(t *testing.T) {
	// Reject up-front — startup error, not "succeeds then fails at
	// first publish on a NATS-illegal subject token."
	cases := []struct {
		slug string
		why  string
	}{
		{"", "...empty is allowed (back-compat); checked separately"},
		{"has space", "whitespace not in charset"},
		{"has.dot.but/slash", "/ not in charset"},
		{"with*star", "* not in charset (NATS wildcard)"},
		{"with>gt", "> not in charset (NATS wildcard)"},
		{"emoji😀", "non-ASCII"},
		{strings.Repeat("a", 129), "exceeds 128 chars"},
	}
	for _, c := range cases {
		if c.slug == "" {
			// Empty InstanceID is the back-compat case — validate must
			// accept it (the rest of cfg is otherwise valid).
			cfg := Config{Agent: "x", Pane: "%1", Adapter: &nopAdapter{}, InstanceID: ""}
			if err := cfg.validate(); err != nil {
				t.Errorf("empty InstanceID should be allowed: %v", err)
			}
			continue
		}
		cfg := Config{Agent: "x", Pane: "%1", Adapter: &nopAdapter{}, InstanceID: c.slug}
		err := cfg.validate()
		if err == nil {
			t.Errorf("slug %q (%s) should be rejected", c.slug, c.why)
			continue
		}
		// Error message MUST include the offending slug + the pattern
		// so operators can fix it without grep'ing source.
		if !strings.Contains(err.Error(), c.slug) {
			t.Errorf("slug %q error should quote the slug, got: %v", c.slug, err)
		}
		if !strings.Contains(err.Error(), "instance_id") {
			t.Errorf("slug %q error should mention instance_id, got: %v", c.slug, err)
		}
	}
}

func TestRun_RejectsInvalidInstanceIDAtStartup(t *testing.T) {
	// The validate() unit test pins the message shape; this one pins
	// that the rejection actually fires through the Run() entry path
	// the CLI uses, so a typo'd --instance-id never reaches NATS.
	url := startEmbeddedNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	cfg := Config{
		Agent:      "claude-code",
		Pane:       "%1",
		Owner:      "u",
		Adapter:    &nopAdapter{},
		InstanceID: "bad slug",
	}
	if err := RunWithConn(t.Context(), nc, cfg); err == nil {
		t.Fatal("RunWithConn should reject invalid InstanceID")
	} else if !strings.Contains(err.Error(), "instance_id") {
		t.Errorf("error should mention instance_id, got: %v", err)
	}
}

func TestSlugSubjectHelpers_EmptyWhenNoInstanceID(t *testing.T) {
	// Defensive: helpers must return "" when no slug is set so an
	// accidental publish/subscribe fails loud at the NATS layer rather
	// than landing on a weird "agents.prompt.cc.u." subject.
	s := &shim{cfg: Config{
		Agent: "claude-code", AgentToken: "cc", Pane: "%1", Owner: "u",
		SubjectPrefix: "agents",
	}}
	if got := s.slugPromptSubject(); got != "" {
		t.Errorf("slugPromptSubject without slug: got %q, want \"\"", got)
	}
	if got := s.slugStatusSubject(); got != "" {
		t.Errorf("slugStatusSubject without slug: got %q, want \"\"", got)
	}
	if got := s.slugHeartbeatSubject(); got != "" {
		t.Errorf("slugHeartbeatSubject without slug: got %q, want \"\"", got)
	}
}

func TestSlugSubjectHelpers_AssembledFromConfig(t *testing.T) {
	s := &shim{cfg: Config{
		Agent: "claude-code", AgentToken: "cc", Pane: "%37", Owner: "tester",
		SubjectPrefix: "agents", InstanceID: "ce-worker",
	}}
	wantPrompt := "agents.prompt.cc.tester.ce-worker"
	wantStatus := "agents.status.cc.tester.ce-worker"
	wantHB := "agents.hb.cc.tester.ce-worker"
	if got := s.slugPromptSubject(); got != wantPrompt {
		t.Errorf("slugPromptSubject: got %q want %q", got, wantPrompt)
	}
	if got := s.slugStatusSubject(); got != wantStatus {
		t.Errorf("slugStatusSubject: got %q want %q", got, wantStatus)
	}
	if got := s.slugHeartbeatSubject(); got != wantHB {
		t.Errorf("slugHeartbeatSubject: got %q want %q", got, wantHB)
	}
}

func TestSlugDualPublishLegacy_EnvVarParsing(t *testing.T) {
	// "" / "1" → on; "0" → off; anything else → on with a warning.
	cases := []struct {
		val  string
		want bool
	}{
		{"", true},
		{"1", true},
		{"0", false},
		{"true", true},  // not recognised → defaults to dual-publish on
		{"false", true}, // same
	}
	for _, c := range cases {
		t.Setenv(EnvSlugDualPublish, c.val)
		if got := slugDualPublishLegacy(); got != c.want {
			t.Errorf("%s=%q: got %v want %v", EnvSlugDualPublish, c.val, got, c.want)
		}
	}
}

func TestServiceMetadata_IncludesInstanceIDWhenSet(t *testing.T) {
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:      "claude-code",
		Pane:       "%37",
		Owner:      "tester",
		Adapter:    &nopAdapter{},
		InstanceID: "ce-worker",
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
	// Both pane_id (always) and instance_id (only when slug set) MUST
	// be in metadata. Issue #19: pane_id stays so pane-watchdog and
	// tmux send-keys consumers don't break.
	if info.Metadata["pane_id"] != "%37" {
		t.Errorf("metadata.pane_id: got %q want %q", info.Metadata["pane_id"], "%37")
	}
	if info.Metadata["instance_id"] != "ce-worker" {
		t.Errorf("metadata.instance_id: got %q want %q", info.Metadata["instance_id"], "ce-worker")
	}
}

func TestServiceMetadata_OmitsInstanceIDWhenUnset(t *testing.T) {
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:   "claude-code",
		Pane:    "%37",
		Owner:   "tester",
		Adapter: &nopAdapter{},
		// No InstanceID set — back-compat.
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
	if info.Metadata["pane_id"] != "%37" {
		t.Errorf("metadata.pane_id should still be present: got %q", info.Metadata["pane_id"])
	}
	if _, present := info.Metadata["instance_id"]; present {
		t.Errorf("metadata.instance_id should be omitted when InstanceID is unset, got %q", info.Metadata["instance_id"])
	}
}

func TestEndpoints_DualPublishOn_RegistersBothTracks(t *testing.T) {
	t.Setenv(EnvSlugDualPublish, "1")
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:      "claude-code",
		Pane:       "%37",
		Owner:      "tester",
		Adapter:    &nopAdapter{},
		InstanceID: "ce-worker",
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
	// Expect FOUR endpoints when dual-publish is on: legacy
	// prompt+status keyed on pct37, plus slug prompt+status keyed on
	// ce-worker. The exact name strings ("prompt", "status",
	// "prompt_slug", "status_slug") are internal; what matters is the
	// subject coverage.
	subjects := map[string]bool{}
	for _, ep := range info.Endpoints {
		subjects[ep.Subject] = true
	}
	want := []string{
		"agents.prompt.cc.tester.pct37",
		"agents.status.cc.tester.pct37",
		"agents.prompt.cc.tester.ce-worker",
		"agents.status.cc.tester.ce-worker",
	}
	for _, s := range want {
		if !subjects[s] {
			t.Errorf("expected endpoint subject %q not registered. got: %v", s, subjects)
		}
	}
}

func TestEndpoints_SlugOnly_SkipsLegacyTrack(t *testing.T) {
	// ORCH_SLUG_DUAL_PUBLISH=0 with --instance-id → slug-only. Legacy
	// pct-keyed endpoints MUST NOT be registered (subscribers on those
	// subjects should miss).
	t.Setenv(EnvSlugDualPublish, "0")
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:      "claude-code",
		Pane:       "%37",
		Owner:      "tester",
		Adapter:    &nopAdapter{},
		InstanceID: "ce-worker",
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
	subjects := map[string]bool{}
	for _, ep := range info.Endpoints {
		subjects[ep.Subject] = true
	}
	wantPresent := []string{
		"agents.prompt.cc.tester.ce-worker",
		"agents.status.cc.tester.ce-worker",
	}
	wantAbsent := []string{
		"agents.prompt.cc.tester.pct37",
		"agents.status.cc.tester.pct37",
	}
	for _, s := range wantPresent {
		if !subjects[s] {
			t.Errorf("slug-only mode: expected slug subject %q. got: %v", s, subjects)
		}
	}
	for _, s := range wantAbsent {
		if subjects[s] {
			t.Errorf("slug-only mode: legacy subject %q should NOT be registered. got: %v", s, subjects)
		}
	}
}

func TestEndpoints_NoInstanceID_LegacyOnly_BackCompat(t *testing.T) {
	// Pre-#19 spawners that don't pass --instance-id MUST see exactly
	// the same endpoint surface as before — legacy track only, no slug
	// endpoints, regardless of ORCH_SLUG_DUAL_PUBLISH.
	t.Setenv(EnvSlugDualPublish, "0") // verifies env var has no effect when slug is unset
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:   "claude-code",
		Pane:    "%37",
		Owner:   "tester",
		Adapter: &nopAdapter{},
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
	subjects := map[string]bool{}
	for _, ep := range info.Endpoints {
		subjects[ep.Subject] = true
	}
	if !subjects["agents.prompt.cc.tester.pct37"] {
		t.Error("legacy prompt subject must be present when no slug is set")
	}
	if !subjects["agents.status.cc.tester.pct37"] {
		t.Error("legacy status subject must be present when no slug is set")
	}
	// No slug subjects should leak in.
	for s := range subjects {
		if strings.Contains(s, "ce-worker") {
			t.Errorf("unexpected slug subject leaked into endpoints: %q", s)
		}
	}
}

func TestPromptDelivery_DualPublishOn_BothSubjectsRouteToHandler(t *testing.T) {
	// Both subjects must DELIVER prompts: a request on either lands a
	// reply. This is the "no break" part of the dual-publish window.
	t.Setenv(EnvSlugDualPublish, "1")
	url := startEmbeddedNATS(t)
	adapter := newScriptedAdapter(NewResponseChunk("ok"), NewTerminatorChunk())
	cfg := Config{
		Agent:      "claude-code",
		Pane:       "%1",
		Owner:      "u",
		Adapter:    adapter,
		InstanceID: "ce-worker",
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	// First, legacy subject still works (back-compat).
	if _, err := nc.Request("agents.prompt.cc.u.pct1", []byte("hi"), 2*time.Second); err != nil {
		t.Fatalf("legacy subject delivery failed: %v", err)
	}
	// Then the slug subject works too.
	if _, err := nc.Request("agents.prompt.cc.u.ce-worker", []byte("hi"), 2*time.Second); err != nil {
		t.Fatalf("slug subject delivery failed: %v", err)
	}
}

func TestPromptDelivery_SlugOnly_LegacySubjectMisses(t *testing.T) {
	t.Setenv(EnvSlugDualPublish, "0")
	url := startEmbeddedNATS(t)
	adapter := newScriptedAdapter(NewResponseChunk("ok"), NewTerminatorChunk())
	cfg := Config{
		Agent:      "claude-code",
		Pane:       "%1",
		Owner:      "u",
		Adapter:    adapter,
		InstanceID: "ce-worker",
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	// Slug subject works.
	if _, err := nc.Request("agents.prompt.cc.u.ce-worker", []byte("hi"), 2*time.Second); err != nil {
		t.Fatalf("slug subject delivery failed: %v", err)
	}
	// Legacy subject MUST miss — no subscriber means request times out.
	_, err := nc.Request("agents.prompt.cc.u.pct1", []byte("hi"), 250*time.Millisecond)
	if err == nil {
		t.Fatal("slug-only mode: legacy subject should have no subscriber")
	}
}

func TestHeartbeat_DualPublishOn_BothSubjectsReceive(t *testing.T) {
	// Use a 1s interval (the §8.2 clamp floor) so a missed tick shows
	// up fast. The initial heartbeat is published before the subscribe
	// races; we rely on the ticker for an assertable second one.
	t.Setenv(EnvSlugDualPublish, "1")
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:      "claude-code",
		Pane:       "%1",
		Owner:      "u",
		Adapter:    &nopAdapter{},
		InstanceID: "ce-worker",
		Interval:   time.Second,
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	subLegacy, err := nc.SubscribeSync("agents.hb.cc.u.pct1")
	if err != nil {
		t.Fatalf("sub legacy hb: %v", err)
	}
	defer func() { _ = subLegacy.Unsubscribe() }()
	subSlug, err := nc.SubscribeSync("agents.hb.cc.u.ce-worker")
	if err != nil {
		t.Fatalf("sub slug hb: %v", err)
	}
	defer func() { _ = subSlug.Unsubscribe() }()

	mLegacy, errL := subLegacy.NextMsg(3 * time.Second)
	if errL != nil {
		t.Fatalf("no legacy heartbeat: %v", errL)
	}
	mSlug, errS := subSlug.NextMsg(3 * time.Second)
	if errS != nil {
		t.Fatalf("no slug heartbeat: %v", errS)
	}
	// Sanity: payloads are non-empty + parse as heartbeat.
	var p heartbeatPayload
	if err := json.Unmarshal(mLegacy.Data, &p); err != nil {
		t.Errorf("legacy hb payload malformed: %v", err)
	}
	if err := json.Unmarshal(mSlug.Data, &p); err != nil {
		t.Errorf("slug hb payload malformed: %v", err)
	}
}

func TestHeartbeat_SlugOnly_LegacySubjectSilent(t *testing.T) {
	t.Setenv(EnvSlugDualPublish, "0")
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:      "claude-code",
		Pane:       "%1",
		Owner:      "u",
		Adapter:    &nopAdapter{},
		InstanceID: "ce-worker",
		// Force a short interval so a missed heartbeat shows up fast.
		Interval: time.Second,
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	subLegacy, err := nc.SubscribeSync("agents.hb.cc.u.pct1")
	if err != nil {
		t.Fatalf("sub legacy hb: %v", err)
	}
	defer func() { _ = subLegacy.Unsubscribe() }()
	subSlug, err := nc.SubscribeSync("agents.hb.cc.u.ce-worker")
	if err != nil {
		t.Fatalf("sub slug hb: %v", err)
	}
	defer func() { _ = subSlug.Unsubscribe() }()

	// Slug heartbeat: SHOULD arrive within the interval.
	if _, err := subSlug.NextMsg(3 * time.Second); err != nil {
		t.Fatalf("slug heartbeat must arrive: %v", err)
	}
	// Legacy heartbeat: MUST NOT arrive — slug-only mode disables it.
	if msg, err := subLegacy.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("slug-only mode: legacy heartbeat leaked: subj=%q data=%q", msg.Subject, msg.Data)
	}
}

func TestHeartbeat_NoInstanceID_OnlyLegacyPublished(t *testing.T) {
	// Pre-#19 caller: no slug → legacy subject only. We pin this by
	// subscribing to a concrete (non-wildcard) hypothetical slug
	// subject and confirming nothing arrives — the slug helpers return
	// "" so no publish targets that subject at all. Wildcard
	// subscribers are out of scope here; this guards back-compat per
	// the issue's "back-compat" acceptance criterion.
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

	subLegacy, err := nc.SubscribeSync("agents.hb.cc.u.pct1")
	if err != nil {
		t.Fatalf("sub legacy hb: %v", err)
	}
	defer func() { _ = subLegacy.Unsubscribe() }()
	// Concrete-token sub — would fire only if the shim published on a
	// slug-named subject (which it shouldn't, since InstanceID is "").
	subSlug, err := nc.SubscribeSync("agents.hb.cc.u.would-be-slug")
	if err != nil {
		t.Fatalf("sub slug hb: %v", err)
	}
	defer func() { _ = subSlug.Unsubscribe() }()

	if _, err := subLegacy.NextMsg(3 * time.Second); err != nil {
		t.Fatalf("legacy heartbeat must arrive when no slug: %v", err)
	}
	// Slug-keyed subject must stay silent — the shim has no slug to
	// derive one. A timeout here is the pass condition.
	if msg, err := subSlug.NextMsg(500 * time.Millisecond); err == nil {
		t.Fatalf("no-slug mode: unexpected hb on slug-shaped subject: subj=%q data=%q", msg.Subject, msg.Data)
	}
}
