package shim

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNewTraceparent_NoParent_ValidFormat(t *testing.T) {
	tp := newTraceparent("")
	// W3C: version-trace_id-span_id-flags = 2-32-16-2 hex chars + 3 dashes = 55
	if len(tp) != 55 {
		t.Fatalf("traceparent length: want 55, got %d (%q)", len(tp), tp)
	}
	if got := parseTraceparent(tp); got == "" {
		t.Fatalf("freshly-minted traceparent should parse, got empty trace_id for %q", tp)
	}
}

func TestNewTraceparent_PropagatesParentTrace(t *testing.T) {
	parentTrace := "0af7651916cd43dd8448eb211c80319c"
	tp := newTraceparent(parentTrace)
	got := parseTraceparent(tp)
	if got != parentTrace {
		t.Fatalf("trace_id propagation: want %q, got %q (full=%q)", parentTrace, got, tp)
	}
}

func TestNewTraceparent_MintsNewSpanIDPerCall(t *testing.T) {
	parentTrace := "0af7651916cd43dd8448eb211c80319c"
	tp1 := newTraceparent(parentTrace)
	tp2 := newTraceparent(parentTrace)
	if tp1 == tp2 {
		t.Fatalf("two calls with same parent should mint distinct span_ids, both %q", tp1)
	}
	// trace_id portion (chars 3..35) MUST match across both.
	if tp1[3:35] != tp2[3:35] {
		t.Fatalf("trace_id portion drifted: %q vs %q", tp1[3:35], tp2[3:35])
	}
}

func TestNewTraceparent_RejectsShortParent_MintsFresh(t *testing.T) {
	tp := newTraceparent("tooshort")
	got := parseTraceparent(tp)
	if got == "" {
		t.Fatalf("fallback traceparent should be valid, got empty trace_id for %q", tp)
	}
	if got == "tooshort" {
		t.Fatalf("malformed parent should not be reused")
	}
}

func TestParseTraceparent_RejectsAllZeroTraceID(t *testing.T) {
	// W3C spec: 32 zeros is the reserved "invalid" trace_id.
	got := parseTraceparent("00-00000000000000000000000000000000-1234567890abcdef-01")
	if got != "" {
		t.Fatalf("all-zero trace_id should be rejected, got %q", got)
	}
}

func TestParseTraceparent_RejectsAllZeroSpanID(t *testing.T) {
	got := parseTraceparent("00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01")
	if got != "" {
		t.Fatalf("all-zero span_id should be rejected, got %q", got)
	}
}

func TestParseTraceparent_RejectsMalformed(t *testing.T) {
	cases := []string{
		"",
		"not-a-traceparent",
		"00-0af7-1234-01",                                                          // wrong lengths
		"00-0af7651916cd43dd8448eb211c80319c-1234567890abcdef-0",                   // 1-char flag
		"00-0af7651916cd43dd8448eb211c80319cZ-1234567890abcdef-01",                 // non-hex char
		"00-0af7651916cd43dd8448eb211c80319c-1234567890abcdef-01-extra",            // trailing
	}
	for _, c := range cases {
		if got := parseTraceparent(c); got != "" {
			t.Fatalf("parseTraceparent(%q) should reject, got %q", c, got)
		}
	}
}

func TestEnvelopeHeaders_RequiredKeysPresent(t *testing.T) {
	h := envelopeHeaders("worker", "", "", firstAttempt)
	for _, key := range []string{HeaderTraceparent, HeaderEnvelope, HeaderRole, HeaderAttempt} {
		if v := h.Get(key); v == "" {
			t.Errorf("envelope header %q missing", key)
		}
	}
}

func TestEnvelopeHeaders_EnvelopeVersionIs1(t *testing.T) {
	h := envelopeHeaders("worker", "", "", firstAttempt)
	if got := h.Get(HeaderEnvelope); got != "1" {
		t.Fatalf("Sesh-Envelope: want 1, got %q", got)
	}
}

func TestEnvelopeHeaders_RoleDefaultsToWorker(t *testing.T) {
	h := envelopeHeaders("", "", "", firstAttempt)
	if got := h.Get(HeaderRole); got != "worker" {
		t.Fatalf("Sesh-Role default: want worker, got %q", got)
	}
}

func TestEnvelopeHeaders_RolePropagated(t *testing.T) {
	h := envelopeHeaders("engineer", "", "", firstAttempt)
	if got := h.Get(HeaderRole); got != "engineer" {
		t.Fatalf("Sesh-Role: want engineer, got %q", got)
	}
}

func TestEnvelopeHeaders_TaskIDOmittedWhenEmpty(t *testing.T) {
	h := envelopeHeaders("worker", "", "", firstAttempt)
	if _, ok := h[HeaderTaskID]; ok {
		t.Fatalf("Sesh-Task-Id should be omitted when empty, got %v", h.Get(HeaderTaskID))
	}
}

func TestEnvelopeHeaders_TaskIDIncludedWhenSet(t *testing.T) {
	h := envelopeHeaders("worker", "01HXX2YJN9G", "", firstAttempt)
	if got := h.Get(HeaderTaskID); got != "01HXX2YJN9G" {
		t.Fatalf("Sesh-Task-Id: want 01HXX2YJN9G, got %q", got)
	}
}

func TestEnvelopeHeaders_AttemptDefaultsTo1(t *testing.T) {
	h := envelopeHeaders("worker", "", "", 0)
	if got := h.Get(HeaderAttempt); got != "1" {
		t.Fatalf("Sesh-Attempt: want 1 (clamped from 0), got %q", got)
	}
}

func TestEnvelopeHeaders_AttemptHonored(t *testing.T) {
	h := envelopeHeaders("worker", "", "", 4)
	if got := h.Get(HeaderAttempt); got != "4" {
		t.Fatalf("Sesh-Attempt: want 4, got %q", got)
	}
}

func TestEnvelopeHeaders_TraceparentValidFormat(t *testing.T) {
	h := envelopeHeaders("worker", "", "", firstAttempt)
	tp := h.Get(HeaderTraceparent)
	if !strings.HasPrefix(tp, "00-") || len(tp) != 55 {
		t.Fatalf("traceparent format: got %q (len=%d)", tp, len(tp))
	}
	if parseTraceparent(tp) == "" {
		t.Fatalf("traceparent should parse: %q", tp)
	}
}

func TestEnvelopeHeaders_TraceparentPropagatesParent(t *testing.T) {
	parent := "0af7651916cd43dd8448eb211c80319c"
	h := envelopeHeaders("worker", "", parent, firstAttempt)
	if got := parseTraceparent(h.Get(HeaderTraceparent)); got != parent {
		t.Fatalf("parent trace propagation: want %q, got %q", parent, got)
	}
}

func TestTraceFromHeaders_ExtractsTraceID(t *testing.T) {
	parent := "0af7651916cd43dd8448eb211c80319c"
	h := nats.Header{}
	h.Set(HeaderTraceparent, "00-"+parent+"-1234567890abcdef-01")
	if got := traceFromHeaders(h); got != parent {
		t.Fatalf("traceFromHeaders: want %q, got %q", parent, got)
	}
}

func TestTraceFromHeaders_EmptyOnAbsentHeader(t *testing.T) {
	if got := traceFromHeaders(nats.Header{}); got != "" {
		t.Fatalf("absent header: want empty, got %q", got)
	}
}

func TestTraceFromHeaders_EmptyOnNilHeader(t *testing.T) {
	if got := traceFromHeaders(nil); got != "" {
		t.Fatalf("nil header: want empty, got %q", got)
	}
}

func TestTraceFromHeaders_EmptyOnMalformedHeader(t *testing.T) {
	h := nats.Header{}
	h.Set(HeaderTraceparent, "garbage")
	if got := traceFromHeaders(h); got != "" {
		t.Fatalf("malformed header: want empty, got %q", got)
	}
}

// -----------------------------------------------------------------------------
// Behavior tests: assert envelope headers on actual published messages.
// -----------------------------------------------------------------------------

// TestPromptStream_ChunksCarryEnvelopeHeaders verifies the ack and every
// response chunk carry the required envelope headers — traceparent,
// Sesh-Envelope, Sesh-Role, Sesh-Attempt. The terminator is checked
// separately (and intentionally headerless per Synadia §6.5).
func TestPromptStream_ChunksCarryEnvelopeHeaders(t *testing.T) {
	url := startEmbeddedNATS(t)
	adapter := newScriptedAdapter(
		NewResponseChunk("hello"),
		NewTerminatorChunk(),
	)
	cfg := Config{
		Agent: "claude-code", Pane: "%200", Owner: "u",
		Role: "verifier", Adapter: adapter,
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := nc.PublishRequest("agents.prompt.cc.u.pct200", inbox, []byte("hi")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// ack, response, terminator
	msgs := readStream(t, sub, 3, 2*time.Second)

	// ack (msg 0) and response (msg 1) MUST carry envelope headers.
	for i, msg := range msgs[:2] {
		if got := msg.Header.Get(HeaderEnvelope); got != "1" {
			t.Errorf("msg[%d] Sesh-Envelope: want 1, got %q", i, got)
		}
		if got := msg.Header.Get(HeaderRole); got != "verifier" {
			t.Errorf("msg[%d] Sesh-Role: want verifier, got %q", i, got)
		}
		if got := msg.Header.Get(HeaderAttempt); got != "1" {
			t.Errorf("msg[%d] Sesh-Attempt: want 1, got %q", i, got)
		}
		tp := msg.Header.Get(HeaderTraceparent)
		if parseTraceparent(tp) == "" {
			t.Errorf("msg[%d] traceparent invalid: %q", i, tp)
		}
	}
}

// TestPromptStream_PropagatesInboundTraceparent verifies that when the
// inbound prompt carries a traceparent header, the trace_id portion
// shows up on every outbound chunk (ack + responses). Each chunk's
// span_id MUST be fresh (no two chunks share the span_id).
func TestPromptStream_PropagatesInboundTraceparent(t *testing.T) {
	url := startEmbeddedNATS(t)
	adapter := newScriptedAdapter(
		NewResponseChunk("hello"),
		NewResponseChunk("world"),
		NewTerminatorChunk(),
	)
	cfg := Config{
		Agent: "claude-code", Pane: "%201", Owner: "u",
		Role: "engineer", Adapter: adapter,
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Build a request with explicit inbound traceparent.
	parentTrace := "4bf92f3577b34da6a3ce929d0e0e4736"
	parentTP := "00-" + parentTrace + "-00f067aa0ba902b7-01"
	req := &nats.Msg{
		Subject: "agents.prompt.cc.u.pct201",
		Reply:   inbox,
		Header:  nats.Header{HeaderTraceparent: []string{parentTP}},
		Data:    []byte("hi"),
	}
	if err := nc.PublishMsg(req); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs := readStream(t, sub, 4, 2*time.Second) // ack, resp, resp, terminator

	seenSpans := map[string]bool{}
	for i, msg := range msgs[:3] {
		tp := msg.Header.Get(HeaderTraceparent)
		got := parseTraceparent(tp)
		if got != parentTrace {
			t.Errorf("msg[%d] trace_id: want %q (propagated), got %q (from %q)", i, parentTrace, got, tp)
		}
		// W3C: span_id is chars 36..52. Each chunk must mint a fresh one.
		if len(tp) >= 52 {
			sp := tp[36:52]
			if seenSpans[sp] {
				t.Errorf("msg[%d] reused span_id %q (must be fresh per hop)", i, sp)
			}
			seenSpans[sp] = true
		}
	}
}

// TestPromptStream_NoInboundTrace_MintsFresh verifies that when the
// inbound prompt has no traceparent header, the ack/response chunks
// still carry valid envelope headers with freshly-minted traces, and
// all chunks within one turn share the same trace_id (so observability
// can still group them as one turn even when the caller skipped tracing).
func TestPromptStream_NoInboundTrace_MintsFresh(t *testing.T) {
	url := startEmbeddedNATS(t)
	adapter := newScriptedAdapter(
		NewResponseChunk("hello"),
		NewResponseChunk("world"),
		NewTerminatorChunk(),
	)
	cfg := Config{Agent: "claude-code", Pane: "%202", Owner: "u", Adapter: adapter}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := nc.PublishRequest("agents.prompt.cc.u.pct202", inbox, []byte("hi")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs := readStream(t, sub, 4, 2*time.Second)

	var sharedTrace string
	for i, msg := range msgs[:3] {
		got := parseTraceparent(msg.Header.Get(HeaderTraceparent))
		if got == "" {
			t.Errorf("msg[%d] should carry a fresh valid trace, got empty", i)
			continue
		}
		if sharedTrace == "" {
			sharedTrace = got
		} else if got != sharedTrace {
			t.Errorf("msg[%d] trace drift within turn: want %q (from msg[0]), got %q", i, sharedTrace, got)
		}
	}
}

// TestHeartbeat_CarriesEnvelopeHeaders asserts the §8.2 heartbeat publish
// includes envelope headers. Heartbeats are not associated with any
// inbound prompt, so each heartbeat carries a fresh trace_id.
func TestHeartbeat_CarriesEnvelopeHeaders(t *testing.T) {
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent: "claude-code", Pane: "%203", Owner: "u",
		Role:     "engineer",
		Interval: 100 * time.Millisecond, // clamped to 1s — sub catches the next beat
		Adapter:  &nopAdapter{},
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	sub, err := nc.SubscribeSync("agents.hb.cc.u.pct203")
	if err != nil {
		t.Fatalf("subscribe hb: %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("no heartbeat: %v", err)
	}
	if got := msg.Header.Get(HeaderEnvelope); got != "1" {
		t.Errorf("heartbeat Sesh-Envelope: want 1, got %q", got)
	}
	if got := msg.Header.Get(HeaderRole); got != "engineer" {
		t.Errorf("heartbeat Sesh-Role: want engineer, got %q", got)
	}
	tp := msg.Header.Get(HeaderTraceparent)
	if parseTraceparent(tp) == "" {
		t.Errorf("heartbeat traceparent invalid: %q", tp)
	}
}

// TestStatus_CarriesEnvelopeHeaders asserts the §8.7 status reply
// carries envelope headers. Status is request/reply, so the reply
// SHOULD propagate the inbound traceparent if any.
func TestStatus_CarriesEnvelopeHeaders(t *testing.T) {
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent: "claude-code", Pane: "%204", Owner: "u",
		Role:    "engineer",
		Adapter: &nopAdapter{},
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	parentTrace := "4bf92f3577b34da6a3ce929d0e0e4736"
	parentTP := "00-" + parentTrace + "-00f067aa0ba902b7-01"

	req := &nats.Msg{
		Subject: "agents.status.cc.u.pct204",
		Reply:   nats.NewInbox(),
		Header:  nats.Header{HeaderTraceparent: []string{parentTP}},
	}
	msg, err := nc.RequestMsg(req, 1*time.Second)
	if err != nil {
		t.Fatalf("status request: %v", err)
	}
	if got := msg.Header.Get(HeaderEnvelope); got != "1" {
		t.Errorf("status Sesh-Envelope: want 1, got %q", got)
	}
	if got := msg.Header.Get(HeaderRole); got != "engineer" {
		t.Errorf("status Sesh-Role: want engineer, got %q", got)
	}
	if got := parseTraceparent(msg.Header.Get(HeaderTraceparent)); got != parentTrace {
		t.Errorf("status trace propagation: want %q, got %q (header %q)",
			parentTrace, got, msg.Header.Get(HeaderTraceparent))
	}
}

// TestErrorOnReply_CarriesEnvelopeHeaders asserts that mid-stream error
// messages (the §9.3 / B.10 path) carry sesh envelope headers alongside
// the Nats-Service-Error-* headers. Trigger via empty-prompt 400.
func TestErrorOnReply_CarriesEnvelopeHeaders(t *testing.T) {
	url := startEmbeddedNATS(t)
	cfg := Config{
		Agent: "claude-code", Pane: "%205", Owner: "u",
		Role: "engineer", Adapter: &nopAdapter{},
	}
	nc, cleanup := runShimInBackground(t, url, cfg)
	defer cleanup()

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := nc.PublishRequest("agents.prompt.cc.u.pct205", inbox, []byte("")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, err := sub.NextMsg(500 * time.Millisecond)
	if err != nil {
		t.Fatalf("first msg: %v", err)
	}
	// The error message carries Nats-Service-Error AND envelope.
	if msg.Header.Get("Nats-Service-Error-Code") != "400" {
		t.Fatalf("expected 400 error, got headers=%v", msg.Header)
	}
	if got := msg.Header.Get(HeaderEnvelope); got != "1" {
		t.Errorf("error msg Sesh-Envelope: want 1, got %q", got)
	}
	if got := msg.Header.Get(HeaderRole); got != "engineer" {
		t.Errorf("error msg Sesh-Role: want engineer, got %q", got)
	}
	if parseTraceparent(msg.Header.Get(HeaderTraceparent)) == "" {
		t.Errorf("error msg traceparent missing or invalid: %q", msg.Header.Get(HeaderTraceparent))
	}
}

// TestPromptStream_TaskIDOptional asserts Sesh-Task-Id rides on chunks
// when configured, and is absent when not.
func TestPromptStream_TaskIDOptional(t *testing.T) {
	url := startEmbeddedNATS(t)

	t.Run("absent when unset", func(t *testing.T) {
		adapter := newScriptedAdapter(NewResponseChunk("x"), NewTerminatorChunk())
		cfg := Config{Agent: "claude-code", Pane: "%206", Owner: "u", Adapter: adapter}
		nc, cleanup := runShimInBackground(t, url, cfg)
		defer cleanup()
		inbox := nats.NewInbox()
		sub, _ := nc.SubscribeSync(inbox)
		defer sub.Unsubscribe()
		_ = nc.PublishRequest("agents.prompt.cc.u.pct206", inbox, []byte("hi"))
		msgs := readStream(t, sub, 2, 2*time.Second)
		if _, ok := msgs[0].Header[HeaderTaskID]; ok {
			t.Errorf("Sesh-Task-Id leaked when unset: %v", msgs[0].Header.Get(HeaderTaskID))
		}
	})

	t.Run("present when set", func(t *testing.T) {
		adapter := newScriptedAdapter(NewResponseChunk("x"), NewTerminatorChunk())
		cfg := Config{
			Agent: "claude-code", Pane: "%207", Owner: "u",
			TaskID:  "01HXXTASK",
			Adapter: adapter,
		}
		nc, cleanup := runShimInBackground(t, url, cfg)
		defer cleanup()
		inbox := nats.NewInbox()
		sub, _ := nc.SubscribeSync(inbox)
		defer sub.Unsubscribe()
		_ = nc.PublishRequest("agents.prompt.cc.u.pct207", inbox, []byte("hi"))
		msgs := readStream(t, sub, 2, 2*time.Second)
		if got := msgs[0].Header.Get(HeaderTaskID); got != "01HXXTASK" {
			t.Errorf("Sesh-Task-Id on ack: want 01HXXTASK, got %q", got)
		}
	})
}

