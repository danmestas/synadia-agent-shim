package shim

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// -----------------------------------------------------------------------------
// Unit tests: chunk encoders against Synadia Appendix B byte examples.
//
// These tests pin the EXACT wire shape — JSON key order, snake_case,
// no trailing newline. encoding/json sorts struct fields by declaration
// order, so the assertions match the JSON examples in the spec verbatim.
// -----------------------------------------------------------------------------

func TestEncodeChunk_ResponseString_B4(t *testing.T) {
	// Appendix B.4: {"type":"response","data":"Hello, world."}
	got, err := encodeChunk(NewResponseChunk("Hello, world."))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := `{"type":"response","data":"Hello, world."}`
	if string(got) != want {
		t.Fatalf("B.4 mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestEncodeChunk_ResponseObject_B5(t *testing.T) {
	// Appendix B.5: {"type":"response","data":{"text":"Hello, world."}}
	got, err := encodeChunk(NewResponseChunk(map[string]string{"text": "Hello, world."}))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := `{"type":"response","data":{"text":"Hello, world."}}`
	if string(got) != want {
		t.Fatalf("B.5 mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestEncodeChunk_StatusAck_B6(t *testing.T) {
	// Appendix B.6: {"type":"status","data":"ack"}
	got, err := encodeChunk(NewStatusChunk("ack"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := `{"type":"status","data":"ack"}`
	if string(got) != want {
		t.Fatalf("B.6 mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestEncodeChunk_Query_B7(t *testing.T) {
	// Appendix B.7 shape (id varies per emit, but field order is fixed):
	// {"type":"query","data":{"id":"...","reply_subject":"...","prompt":"..."}}
	got, err := encodeChunk(NewQueryChunk("abc", "_INBOX.X", "Confirm? (yes/no)"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := `{"type":"query","data":{"id":"abc","reply_subject":"_INBOX.X","prompt":"Confirm? (yes/no)"}}`
	if string(got) != want {
		t.Fatalf("B.7 mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestEncodeChunk_RejectsTerminator(t *testing.T) {
	// Terminator chunks aren't JSON-encoded — the shim publishes a
	// zero-byte body directly. encodeChunk should refuse them so
	// accidental misuse fails loudly.
	if _, err := encodeChunk(NewTerminatorChunk()); err == nil {
		t.Fatal("expected encodeChunk to refuse terminator chunks")
	}
}

func TestHeartbeatPayload_Shape_B11(t *testing.T) {
	// Appendix B.11 shape:
	// {"agent":"claude-code","owner":"aconnolly","session":"synadia-com-2",
	//  "instance_id":"...","ts":"...","interval_s":30}
	p := heartbeatPayload{
		Agent:      "claude-code",
		Owner:      "aconnolly",
		Session:    "synadia-com-2",
		InstanceID: "VMKS6MHK71PCPWGY38A7N5",
		TS:         "2026-04-28T14:23:01Z",
		IntervalS:  30,
	}
	got, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"agent":"claude-code","owner":"aconnolly","session":"synadia-com-2","instance_id":"VMKS6MHK71PCPWGY38A7N5","ts":"2026-04-28T14:23:01Z","interval_s":30}`
	if string(got) != want {
		t.Fatalf("B.11 mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestHeartbeatPayload_SessionOmittedWhenEmpty(t *testing.T) {
	// §8.3: session is "present iff metadata.session is set". Empty
	// string MUST be omitted.
	p := heartbeatPayload{
		Agent: "claude-code", Owner: "x",
		InstanceID: "y", TS: "2026-01-01T00:00:00Z", IntervalS: 30,
	}
	got, _ := json.Marshal(p)
	if strings.Contains(string(got), "session") {
		t.Fatalf("session leaked into payload when empty: %s", got)
	}
}

func TestParseEnvelope_PlainText(t *testing.T) {
	// §5.1: plain UTF-8 text is shorthand for {prompt:<text>}.
	env, err := parseEnvelope([]byte("summarize the report"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if env.Prompt != "summarize the report" {
		t.Fatalf("prompt mismatch: %q", env.Prompt)
	}
}

func TestParseEnvelope_JSONEnvelope(t *testing.T) {
	env, err := parseEnvelope([]byte(`{"prompt":"hello","extras":{"k":"v"}}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if env.Prompt != "hello" {
		t.Fatalf("prompt mismatch: %q", env.Prompt)
	}
}

func TestParseEnvelope_RejectsEmpty(t *testing.T) {
	// §12: empty payload → 400.
	if _, err := parseEnvelope([]byte("")); err == nil {
		t.Fatal("expected error on empty body")
	}
	if _, err := parseEnvelope([]byte("   ")); err == nil {
		t.Fatal("expected error on whitespace-only body")
	}
}

func TestParseEnvelope_RejectsMalformedJSON(t *testing.T) {
	if _, err := parseEnvelope([]byte(`{"prompt":}`)); err == nil {
		t.Fatal("expected error on malformed JSON")
	}
}

func TestEncodePane(t *testing.T) {
	// "%37" → "pct37"; raw "%" is forbidden in NATS subjects.
	if got := encodePane("%37"); got != "pct37" {
		t.Fatalf("encodePane %%37: got %q want %q", got, "pct37")
	}
	// Already-stripped values pass through.
	if got := encodePane("37"); got != "pct37" {
		t.Fatalf("encodePane 37: got %q want %q", got, "pct37")
	}
}

func TestWithDefaults_FillsAgentToken(t *testing.T) {
	// claude-code abbreviates to "cc"; everything else passes through.
	cases := []struct{ in, want string }{
		{"claude-code", "cc"},
		{"codex", "codex"},
		{"pi", "pi"},
	}
	for _, c := range cases {
		got := withDefaults(Config{Agent: c.in, Pane: "%1", Adapter: &nopAdapter{}}).AgentToken
		if got != c.want {
			t.Fatalf("AgentToken(%q): got %q want %q", c.in, got, c.want)
		}
	}
}

func TestWithDefaults_ClampsIntervalBelowOneSecond(t *testing.T) {
	// §8.2: values below 1s SHOULD NOT be used. We clamp rather than
	// error so test overrides don't break the contract.
	got := withDefaults(Config{Agent: "x", Pane: "%1", Adapter: &nopAdapter{}, Interval: time.Millisecond}).Interval
	if got != time.Second {
		t.Fatalf("clamp: got %v want %v", got, time.Second)
	}
}

// -----------------------------------------------------------------------------
// Behavior tests: end-to-end against an embedded NATS server.
// -----------------------------------------------------------------------------

// nopAdapter satisfies the Adapter interface for tests that only care
// about the shim's protocol surface, not adapter behavior.
type nopAdapter struct {
	mu        sync.Mutex
	ch        chan Chunk
	closeOnce sync.Once
}

func (a *nopAdapter) Start(_ context.Context) error              { return nil }
func (a *nopAdapter) OnPrompt(_ context.Context, _ string) error { return nil }
func (a *nopAdapter) Events() <-chan Chunk {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ch == nil {
		a.ch = make(chan Chunk, 8)
	}
	return a.ch
}
func (a *nopAdapter) Close() error {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.ch == nil {
			a.ch = make(chan Chunk, 8)
		}
		close(a.ch)
	})
	return nil
}

// scriptedAdapter emits a pre-canned chunk sequence in response to each
// OnPrompt. Used to verify the shim publishes them in order, terminates
// the stream, and re-opens for the next prompt.
type scriptedAdapter struct {
	script    []Chunk
	ch        chan Chunk
	closeOnce sync.Once
}

func newScriptedAdapter(script ...Chunk) *scriptedAdapter {
	return &scriptedAdapter{script: script, ch: make(chan Chunk, len(script)+4)}
}
func (a *scriptedAdapter) Start(_ context.Context) error { return nil }
func (a *scriptedAdapter) OnPrompt(_ context.Context, _ string) error {
	for _, c := range a.script {
		a.ch <- c
	}
	return nil
}
func (a *scriptedAdapter) Events() <-chan Chunk { return a.ch }
func (a *scriptedAdapter) Close() error {
	a.closeOnce.Do(func() { close(a.ch) })
	return nil
}

// startEmbeddedNATS runs an in-process server on a random port and
// returns the URL. The server stops when the test ends.
func startEmbeddedNATS(t *testing.T) string {
	t.Helper()
	opts := test.DefaultTestOptions
	opts.Port = -1 // auto-pick
	s := test.RunServer(&opts)
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS not ready")
	}
	return s.ClientURL()
}

// runShimInBackground starts the shim against the given URL with the
// given config and returns a teardown closer.
func runShimInBackground(t *testing.T, url string, cfg Config) (*nats.Conn, func()) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunWithConn(ctx, nc, cfg)
	}()
	// Wait for service registration before returning. Tight loop with
	// a hard cap — if we never see the service it's a real failure.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var found bool
		discoverService(t, nc, &found)
		if found {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nc, func() {
		cancel()
		<-done
		nc.Drain()
	}
}

// discoverService sends $SRV.PING.agents and sets *found if any
// instance replies. Used as a readiness probe.
func discoverService(t *testing.T, nc *nats.Conn, found *bool) {
	t.Helper()
	msg, err := nc.Request("$SRV.PING.agents", nil, 200*time.Millisecond)
	if err == nil && msg != nil {
		*found = true
	}
}

func TestServiceDiscovery_INFO_HasExpectedShape(t *testing.T) {
	// Acceptance criterion: `nats req '$SRV.INFO.agents'` returns shim
	// metadata in the documented shape. We test this via the same micro
	// service discovery the spec requires.
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent:   "claude-code",
		Pane:    "%37",
		Owner:   "tester",
		Session: "sesh-x",
		Outfit:  "engineer",
		Role:    "worker",
		CWD:     "/tmp/proj",
		Adapter: &nopAdapter{},
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	msg, err := nc.Request("$SRV.INFO.agents", nil, 1*time.Second)
	if err != nil {
		t.Fatalf("$SRV.INFO.agents: %v", err)
	}
	var info serviceInfo
	if err := json.Unmarshal(msg.Data, &info); err != nil {
		t.Fatalf("decode INFO: %v\nraw: %s", err, msg.Data)
	}

	// Service-level checks: §3.1 name, §3.2 metadata.
	if info.Name != "agents" {
		t.Errorf("service name: got %q want %q", info.Name, "agents")
	}
	if info.Metadata["agent"] != "claude-code" {
		t.Errorf("metadata.agent: got %q want %q", info.Metadata["agent"], "claude-code")
	}
	if info.Metadata["owner"] != "tester" {
		t.Errorf("metadata.owner: got %q", info.Metadata["owner"])
	}
	if info.Metadata["protocol_version"] != "0.3" {
		t.Errorf("metadata.protocol_version: got %q want 0.3", info.Metadata["protocol_version"])
	}
	if info.Metadata["pane_id"] != "%37" {
		t.Errorf("metadata.pane_id should preserve raw %%37: got %q", info.Metadata["pane_id"])
	}
	if info.Metadata["session"] != "sesh-x" {
		t.Errorf("metadata.session: got %q want sesh-x", info.Metadata["session"])
	}

	// Endpoint checks: §12 requires "prompt" + "status" endpoints with
	// queue_group "agents" and the spec subject layout.
	// With Session set, subjects use the session token (operator-readable)
	// instead of the pct-encoded pane id. Pane fall-back still tested in
	// TestServiceDiscovery_INFO_NoSession_FallsBackToPaneToken below.
	want := map[string]string{
		"prompt": "agents.prompt.cc.tester.sesh-x",
		"status": "agents.status.cc.tester.sesh-x",
	}
	got := map[string]string{}
	for _, ep := range info.Endpoints {
		got[ep.Name] = ep.Subject
		if ep.QueueGroup != "agents" {
			t.Errorf("endpoint %s queue_group: got %q want agents", ep.Name, ep.QueueGroup)
		}
	}
	for n, sub := range want {
		if got[n] != sub {
			t.Errorf("endpoint %s subject: got %q want %q", n, got[n], sub)
		}
	}

	// §2.1 + §12 prompt endpoint advertises max_payload + attachments_ok.
	var promptEP *serviceEndpoint
	for i, ep := range info.Endpoints {
		if ep.Name == "prompt" {
			promptEP = &info.Endpoints[i]
			break
		}
	}
	if promptEP == nil {
		t.Fatal("prompt endpoint missing from INFO")
	}
	if promptEP.Metadata["max_payload"] == "" {
		t.Error("prompt endpoint missing max_payload metadata")
	}
	if promptEP.Metadata["attachments_ok"] == "" {
		t.Error("prompt endpoint missing attachments_ok metadata")
	}
}

// serviceInfo mirrors the relevant fields of $SRV.INFO.agents (B.12).
type serviceInfo struct {
	Name      string            `json:"name"`
	ID        string            `json:"id"`
	Version   string            `json:"version"`
	Metadata  map[string]string `json:"metadata"`
	Endpoints []serviceEndpoint `json:"endpoints"`
}

type serviceEndpoint struct {
	Name       string            `json:"name"`
	Subject    string            `json:"subject"`
	QueueGroup string            `json:"queue_group"`
	Metadata   map[string]string `json:"metadata"`
}

func TestPromptStream_AckFirstThenChunksThenTerminator(t *testing.T) {
	url := startEmbeddedNATS(t)
	adapter := newScriptedAdapter(
		NewResponseChunk("hello"),
		NewResponseChunk("world"),
		NewTerminatorChunk(),
	)
	cfg := Config{Agent: "claude-code", Pane: "%1", Owner: "u", Adapter: adapter}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	// Subscribe to a sync inbox so we collect the entire stream.
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Publish a prompt to the documented subject layout.
	subject := "agents.prompt.cc.u.pct1"
	if err := nc.PublishRequest(subject, inbox, []byte("hi")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Expected sequence: ack (status), response, response, terminator.
	msgs := readStream(t, sub, 4, 2*time.Second)

	// Message 1: ack
	if got, want := string(msgs[0].Data), `{"type":"status","data":"ack"}`; got != want {
		t.Errorf("first chunk: got %s want %s", got, want)
	}
	// Message 2: response "hello"
	if got, want := string(msgs[1].Data), `{"type":"response","data":"hello"}`; got != want {
		t.Errorf("second chunk: got %s want %s", got, want)
	}
	// Message 3: response "world"
	if got, want := string(msgs[2].Data), `{"type":"response","data":"world"}`; got != want {
		t.Errorf("third chunk: got %s want %s", got, want)
	}
	// Message 4: terminator (zero-byte, no headers)
	if len(msgs[3].Data) != 0 {
		t.Errorf("terminator should be empty body, got %d bytes", len(msgs[3].Data))
	}
	if len(msgs[3].Header) != 0 {
		t.Errorf("terminator should have no headers, got %v", msgs[3].Header)
	}
}

func TestPromptStream_RejectsEmptyPayload_400(t *testing.T) {
	url := startEmbeddedNATS(t)
	cfg := Config{Agent: "claude-code", Pane: "%1", Owner: "u", Adapter: &nopAdapter{}}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	msg, err := nc.Request("agents.prompt.cc.u.pct1", []byte(""), 500*time.Millisecond)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if got := msg.Header.Get("Nats-Service-Error-Code"); got != "400" {
		t.Errorf("error code: got %q want 400", got)
	}
}

// §9.3 + B.10: an error reply MUST be followed by a §6.5 terminator —
// even pre-ack. This catches the regression where pre-ack 400s emitted
// the error message but left callers spinning on the §6.6 inactivity
// timeout.
func TestPromptStream_PreAckError_FollowedByTerminator(t *testing.T) {
	url := startEmbeddedNATS(t)
	cfg := Config{Agent: "claude-code", Pane: "%2", Owner: "u", Adapter: &nopAdapter{}}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := nc.PublishRequest("agents.prompt.cc.u.pct2", inbox, []byte("")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Message 1: error header.
	msg, err := sub.NextMsg(500 * time.Millisecond)
	if err != nil {
		t.Fatalf("first msg: %v", err)
	}
	if msg.Header.Get("Nats-Service-Error-Code") != "400" {
		t.Errorf("first message should carry 400 error header, got %v", msg.Header)
	}

	// Message 2: terminator (zero body, no headers).
	msg, err = sub.NextMsg(500 * time.Millisecond)
	if err != nil {
		t.Fatalf("terminator missing: %v", err)
	}
	if len(msg.Data) != 0 {
		t.Errorf("terminator should be empty body, got %d bytes", len(msg.Data))
	}
	if len(msg.Header) != 0 {
		t.Errorf("terminator should have no headers, got %v", msg.Header)
	}
}

// §6.5: an adapter that never emits a terminator (mock harness) MUST
// still produce a zero-body terminator. The shim's watchdog (#102) is
// the safety net — it force-emits the terminator after
// terminatorWatchdog so callers using `nats req --replies=0` don't hang.
//
// Test approach: nopAdapter never emits any chunk on Events(); we
// override terminatorWatchdog to 100ms and assert the terminator lands
// within 2× the watchdog window.
func TestPromptStream_WatchdogEmitsTerminator_WhenAdapterSilent(t *testing.T) {
	// Override the package-level watchdog for the duration of this test.
	// Restore on cleanup so subsequent tests see the production value.
	orig := terminatorWatchdog
	terminatorWatchdog = 100 * time.Millisecond
	t.Cleanup(func() { terminatorWatchdog = orig })

	url := startEmbeddedNATS(t)
	cfg := Config{Agent: "claude-code", Pane: "%102", Owner: "u", Adapter: &nopAdapter{}}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := nc.PublishRequest("agents.prompt.cc.u.pct102", inbox, []byte("hi")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Message 1: the §6.4 ack (the shim always emits this synchronously).
	msg, err := sub.NextMsg(500 * time.Millisecond)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if string(msg.Data) != `{"type":"status","data":"ack"}` {
		t.Fatalf("ack mismatch: got %s", msg.Data)
	}

	// Message 2: the §6.5 terminator, force-emitted by the watchdog.
	// Wall-clock bound is 2× watchdog to absorb scheduler jitter.
	msg, err = sub.NextMsg(2 * terminatorWatchdog)
	if err != nil {
		t.Fatalf("watchdog terminator never arrived (>2x watchdog): %v", err)
	}
	if len(msg.Data) != 0 {
		t.Errorf("terminator should be empty body, got %d bytes", len(msg.Data))
	}
	if len(msg.Header) != 0 {
		t.Errorf("terminator should have no headers, got %v", msg.Header)
	}

	// And no extra messages: the watchdog must NOT double-emit if the
	// adapter later closes its channel.
	if msg, err := sub.NextMsg(200 * time.Millisecond); err == nil {
		t.Errorf("unexpected extra message after terminator: %q (headers=%v)", msg.Data, msg.Header)
	}
}

// Companion to TestPromptStream_WatchdogEmitsTerminator_WhenAdapterSilent:
// when the adapter DOES emit a terminator within the watchdog window,
// the watchdog MUST NOT double-emit. Asserts idempotency by counting
// terminator-shaped messages over the stream.
func TestPromptStream_WatchdogIdempotent_WhenAdapterTerminates(t *testing.T) {
	orig := terminatorWatchdog
	terminatorWatchdog = 100 * time.Millisecond
	t.Cleanup(func() { terminatorWatchdog = orig })

	url := startEmbeddedNATS(t)
	adapter := newScriptedAdapter(
		NewResponseChunk("hello"),
		NewTerminatorChunk(),
	)
	cfg := Config{Agent: "claude-code", Pane: "%103", Owner: "u", Adapter: adapter}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := nc.PublishRequest("agents.prompt.cc.u.pct103", inbox, []byte("hi")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Expected sequence: ack, response "hello", terminator. No second
	// terminator from the watchdog.
	msgs := readStream(t, sub, 3, 1*time.Second)
	if len(msgs[2].Data) != 0 {
		t.Errorf("third msg should be terminator, got %s", msgs[2].Data)
	}

	// Wait past the watchdog deadline; assert no extra terminator lands.
	if msg, err := sub.NextMsg(3 * terminatorWatchdog); err == nil {
		t.Errorf("watchdog double-emitted: %q (headers=%v)", msg.Data, msg.Header)
	}
}

func TestStatusEndpoint_RepliesWithHeartbeatShape(t *testing.T) {
	// §8.7: status endpoint replies with a fresh §8.3 heartbeat payload.
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent: "claude-code", Pane: "%5", Owner: "u", Session: "s",
		Adapter: &nopAdapter{},
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	// Session "s" → status subject uses "s" not "pct5" (sessionToken).
	msg, err := nc.Request("agents.status.cc.u.s", nil, 1*time.Second)
	if err != nil {
		t.Fatalf("status request: %v", err)
	}
	var hb heartbeatPayload
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		t.Fatalf("decode hb: %v", err)
	}
	if hb.Agent != "claude-code" {
		t.Errorf("agent: got %q", hb.Agent)
	}
	if hb.Owner != "u" {
		t.Errorf("owner: got %q", hb.Owner)
	}
	if hb.Session != "s" {
		t.Errorf("session: got %q", hb.Session)
	}
	if hb.InstanceID == "" {
		t.Error("instance_id should be non-empty")
	}
	if hb.IntervalS == 0 {
		t.Error("interval_s should be > 0")
	}
}

func TestHeartbeat_Published(t *testing.T) {
	// §8.1 + §8.2: heartbeats appear on agents.hb.<token>.<owner>.<name>.
	// We set a tight interval so we don't wait long.
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent: "claude-code", Pane: "%9", Owner: "u",
		Interval: 100 * time.Millisecond, // will be clamped to 1s
		Adapter:  &nopAdapter{},
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	sub, err := nc.SubscribeSync("agents.hb.cc.u.pct9")
	if err != nil {
		t.Fatalf("subscribe hb: %v", err)
	}
	// The immediate publish in heartbeatLoop should land within a beat.
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("no heartbeat: %v", err)
	}
	var hb heartbeatPayload
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		t.Fatalf("decode hb: %v", err)
	}
	if hb.Agent != "claude-code" || hb.Owner != "u" {
		t.Errorf("hb identity: agent=%q owner=%q", hb.Agent, hb.Owner)
	}
}

// readStream collects up to n messages from sub, failing the test on
// timeout.
func readStream(t *testing.T, sub *nats.Subscription, n int, total time.Duration) []*nats.Msg {
	t.Helper()
	out := make([]*nats.Msg, 0, n)
	deadline := time.Now().Add(total)
	for len(out) < n {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("readStream: timeout after %d msgs", len(out))
		}
		msg, err := sub.NextMsg(remaining)
		if err != nil {
			t.Fatalf("readStream: NextMsg: %v", err)
		}
		out = append(out, msg)
	}
	return out
}

// -----------------------------------------------------------------------------
// orch.signal.> verb tests (issue #133).
// -----------------------------------------------------------------------------

// pendingAdapter records OnPrompt invocations + Abort calls without ever
// producing chunks. Used to exercise interrupt/redirect handlers — the
// only messages on the reply subject are the shim's own ack/aborted/
// terminator, so the test can assert their order without race-prone
// chunk-draining.
type pendingAdapter struct {
	mu          sync.Mutex
	ch          chan Chunk
	closeOnce   sync.Once
	abortCount  int
	lastPrompt  string
	promptCalls int
}

func newPendingAdapter() *pendingAdapter {
	return &pendingAdapter{ch: make(chan Chunk, 8)}
}
func (a *pendingAdapter) Start(_ context.Context) error { return nil }
func (a *pendingAdapter) OnPrompt(_ context.Context, text string) error {
	a.mu.Lock()
	a.lastPrompt = text
	a.promptCalls++
	a.mu.Unlock()
	// Return immediately — the shim only requires OnPrompt to KICK off
	// the turn. Chunks (and the eventual terminator or abort) flow
	// asynchronously via Events().
	return nil
}
func (a *pendingAdapter) Events() <-chan Chunk { return a.ch }
func (a *pendingAdapter) Close() error {
	a.closeOnce.Do(func() { close(a.ch) })
	return nil
}
func (a *pendingAdapter) Abort(_ context.Context) error {
	a.mu.Lock()
	a.abortCount++
	a.mu.Unlock()
	return nil
}
func (a *pendingAdapter) AbortCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.abortCount
}
func (a *pendingAdapter) PromptCalls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.promptCalls
}
func (a *pendingAdapter) LastPrompt() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastPrompt
}

// promptAndDrainAck publishes a prompt and reads the mandatory ack
// chunk from the inbox, leaving the stream open for the test to assert
// further chunks (interrupt or redirect). Returns the inbox subscription.
func promptAndDrainAck(t *testing.T, nc *nats.Conn, subject string) *nats.Subscription {
	t.Helper()
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe inbox: %v", err)
	}
	if err := nc.PublishRequest(subject, inbox, []byte("hello")); err != nil {
		t.Fatalf("publish prompt: %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if got, want := string(msg.Data), `{"type":"status","data":"ack"}`; got != want {
		t.Fatalf("expected ack first, got %s", got)
	}
	return sub
}

func TestSignal_InterruptCancelsTurn_EmitsAbortedStatus_AndTerminator(t *testing.T) {
	url := startEmbeddedNATS(t)
	adapter := newPendingAdapter()
	cfg := Config{Agent: "claude-code", Pane: "%9", Owner: "u", Adapter: adapter}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	sub := promptAndDrainAck(t, nc, "agents.prompt.cc.u.pct9")
	defer sub.Unsubscribe()

	if err := nc.Publish("orch.signal.interrupt.cc.u.pct9", nil); err != nil {
		t.Fatalf("publish interrupt: %v", err)
	}

	msgs := readStream(t, sub, 2, 2*time.Second)
	if got, want := string(msgs[0].Data), `{"type":"status","data":"aborted"}`; got != want {
		t.Errorf("aborted chunk: got %s want %s", got, want)
	}
	if len(msgs[1].Data) != 0 {
		t.Errorf("terminator should be empty body, got %d bytes", len(msgs[1].Data))
	}
	// Abort must have been invoked on the adapter (TUI adapters in
	// production deliver Ctrl-C via this hook).
	if n := adapter.AbortCount(); n != 1 {
		t.Errorf("Abort call count: got %d want 1", n)
	}
	// After interrupt, the active slot is released — a follow-up
	// prompt must succeed (proves clearActive ran).
	follow := promptAndDrainAck(t, nc, "agents.prompt.cc.u.pct9")
	defer follow.Unsubscribe()
	if calls := adapter.PromptCalls(); calls < 2 {
		t.Errorf("expected ≥2 prompt calls after interrupt; got %d", calls)
	}
}

func TestSignal_InterruptIsIdempotent_NoActiveTurnIsNoop(t *testing.T) {
	url := startEmbeddedNATS(t)
	adapter := newPendingAdapter()
	cfg := Config{Agent: "claude-code", Pane: "%10", Owner: "u", Adapter: adapter}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	// No active turn yet — interrupt must be a silent no-op (and MUST
	// NOT call Abort, since there's nothing to abort).
	if err := nc.Publish("orch.signal.interrupt.cc.u.pct10", nil); err != nil {
		t.Fatalf("publish interrupt: %v", err)
	}
	if err := nc.Publish("orch.signal.interrupt.cc.u.pct10", nil); err != nil {
		t.Fatalf("publish interrupt 2: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	if n := adapter.AbortCount(); n != 0 {
		t.Errorf("idle interrupt should NOT call Abort; got %d", n)
	}

	// Active turn + two back-to-back interrupts: only the first fires
	// Abort; the second observes an empty slot (coalescing).
	sub := promptAndDrainAck(t, nc, "agents.prompt.cc.u.pct10")
	defer sub.Unsubscribe()
	for i := 0; i < 2; i++ {
		if err := nc.Publish("orch.signal.interrupt.cc.u.pct10", nil); err != nil {
			t.Fatalf("publish interrupt loop %d: %v", i, err)
		}
	}
	_ = readStream(t, sub, 2, 2*time.Second)
	if n := adapter.AbortCount(); n != 1 {
		t.Errorf("multi-interrupt coalescing: Abort calls=%d want 1", n)
	}
}

func TestSignal_Redirect_StopsOldStream_StartsNewOnReply(t *testing.T) {
	url := startEmbeddedNATS(t)
	adapter := newPendingAdapter()
	cfg := Config{Agent: "claude-code", Pane: "%11", Owner: "u", Adapter: adapter}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	oldSub := promptAndDrainAck(t, nc, "agents.prompt.cc.u.pct11")
	defer oldSub.Unsubscribe()

	// Operator-supplied reply subject for the redirected turn. Real
	// orch-interrupt mints an _INBOX; the test pins it so the
	// subscription is deterministic.
	newReply := nats.NewInbox()
	newSub, err := nc.SubscribeSync(newReply)
	if err != nil {
		t.Fatalf("subscribe new reply: %v", err)
	}
	defer newSub.Unsubscribe()

	body := []byte(`{"prompt":"new direction","reply":"` + newReply + `"}`)
	if err := nc.Publish("orch.signal.redirect.cc.u.pct11", body); err != nil {
		t.Fatalf("publish redirect: %v", err)
	}

	// Old stream gets aborted + terminator.
	oldMsgs := readStream(t, oldSub, 2, 2*time.Second)
	if got, want := string(oldMsgs[0].Data), `{"type":"status","data":"aborted"}`; got != want {
		t.Errorf("old stream aborted chunk: got %s want %s", got, want)
	}
	if len(oldMsgs[1].Data) != 0 {
		t.Errorf("old stream terminator: expected empty body, got %d bytes", len(oldMsgs[1].Data))
	}

	// New stream gets its own ack on the operator-supplied reply
	// subject — and the adapter saw the redirected prompt text.
	ack, err := newSub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("new reply ack: %v", err)
	}
	if got, want := string(ack.Data), `{"type":"status","data":"ack"}`; got != want {
		t.Errorf("new reply first chunk: got %s want %s", got, want)
	}
	if calls := adapter.PromptCalls(); calls != 2 {
		t.Errorf("adapter prompt calls after redirect: got %d want 2", calls)
	}
	if got := adapter.LastPrompt(); got != "new direction" {
		t.Errorf("adapter last prompt: got %q want %q", got, "new direction")
	}
}

// nonAborterPendingAdapter is pendingAdapter without the Abort method —
// proves that the shim's type-assertion path doesn't crash and still
// emits status:aborted + terminator for adapters that don't implement
// Aborter.
type nonAborterPendingAdapter struct {
	mu        sync.Mutex
	ch        chan Chunk
	closeOnce sync.Once
}

func newNonAborterPendingAdapter() *nonAborterPendingAdapter {
	return &nonAborterPendingAdapter{ch: make(chan Chunk, 8)}
}
func (a *nonAborterPendingAdapter) Start(_ context.Context) error              { return nil }
func (a *nonAborterPendingAdapter) OnPrompt(_ context.Context, _ string) error { return nil }
func (a *nonAborterPendingAdapter) Events() <-chan Chunk                       { return a.ch }
func (a *nonAborterPendingAdapter) Close() error {
	a.closeOnce.Do(func() { close(a.ch) })
	return nil
}

func TestSignal_InterruptWithoutAborter_StillTerminatesStream(t *testing.T) {
	url := startEmbeddedNATS(t)
	adapter := newNonAborterPendingAdapter()
	cfg := Config{Agent: "claude-code", Pane: "%12", Owner: "u", Adapter: adapter}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	sub := promptAndDrainAck(t, nc, "agents.prompt.cc.u.pct12")
	defer sub.Unsubscribe()

	if err := nc.Publish("orch.signal.interrupt.cc.u.pct12", nil); err != nil {
		t.Fatalf("publish interrupt: %v", err)
	}

	msgs := readStream(t, sub, 2, 2*time.Second)
	if got, want := string(msgs[0].Data), `{"type":"status","data":"aborted"}`; got != want {
		t.Errorf("aborted chunk: got %s want %s", got, want)
	}
	if len(msgs[1].Data) != 0 {
		t.Errorf("terminator: expected empty body, got %d bytes", len(msgs[1].Data))
	}
}

// Compile-time assertion: nopAdapter and scriptedAdapter satisfy Adapter.
var (
	_ Adapter = (*nopAdapter)(nil)
	_ Adapter = (*scriptedAdapter)(nil)
	_ Adapter = (*pendingAdapter)(nil)
	_ Aborter = (*pendingAdapter)(nil)
	_ Adapter = (*nonAborterPendingAdapter)(nil)
)

// Compile-time check for the embedded test server's option type so this
// file fails to build if upstream renames it (signal to revisit tests).
var _ server.Options = server.Options{}
