// Package shim implements the Synadia Agent Protocol v0.3 agent role
// for an orch-managed pane. One process per pane.
//
// Wire reference: github.com/synadia-io/agents/agent-sdk-docs/core-protocol.md
//
// Spec compliance map (matches the §12 agent checklist; see also
// docs/orch-agent-shim.md):
//
//   - Service registered as `agents` (§3.1)              → newService
//   - Metadata: agent/owner/protocol_version/session...  → newService
//   - `prompt` endpoint with queue group "agents"        → registerEndpoints
//   - `status` endpoint with queue group "agents"        → registerEndpoints / statusHandler
//   - Plain-text and JSON envelopes accepted             → parseEnvelope
//   - Mandatory `ack` as first reply chunk (§6.4)        → handlePrompt
//   - Typed chunks + zero-byte terminator (§6)           → publishChunk / publishTerminator
//   - Error path uses Nats-Service-Error headers (§9)    → publishError
//   - Heartbeats on `agents.hb.<token>.<owner>.<name>`   → startHeartbeats
//   - Status endpoint replies with §8.3 payload          → statusHandler
package shim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// firstAttempt is the Sesh-Attempt value the shim emits on outbound
// publishes. The shim does not implement publish-level retries (failures
// surface to the operator-facing publish-error channel and we move on)
// so every header sets attempt=1. The wiring stays in place so retry
// logic added later can increment without restructuring the call sites.
const firstAttempt = 1

// ProtocolVersion is the Synadia Agent Protocol revision this shim
// implements (§11.1). MAJOR.MINOR only; patch has no compatibility
// meaning. v0.3 introduced the verb-first subject layout and the
// §8.7 status request/reply endpoint.
const ProtocolVersion = "0.3"

// defaultSubjectPrefix is the Synadia §3.1 conventional service / subject
// namespace ("agents"). Callers can override via Config.SubjectPrefix.
const defaultSubjectPrefix = "agents"

// defaultSignalPrefix is the orch.signal.> namespace used by orch#133
// interrupt/redirect verbs. Callers can override via Config.SignalPrefix.
const defaultSignalPrefix = "orch.signal"

// Default heartbeat cadence (§8.2). Recommended 30s. We expose the
// override via Config.Interval and clamp the lower bound to 1s.
const defaultInterval = 30 * time.Second

// terminatorWatchdog bounds how long the shim waits for the adapter's
// §6.5 zero-body terminator after emitting the §6.4 ack. If the adapter
// never closes its Events() channel and never sends a Terminator chunk
// (the failure mode #102 documents in mock harnesses), the watchdog
// force-emits the terminator so callers using `nats req --replies=0`
// don't hang for the full reply timeout on every prompt.
//
// Declared `var` instead of `const` so behavior tests can swap it for a
// short value (e.g. 100ms) without timing out the suite. Production code
// MUST treat this as a constant — there is no CLI flag.
var terminatorWatchdog = 30 * time.Second

// Default advertised `prompt` endpoint metadata (§2.1). 1MB is the
// NATS default max payload; advertising more would tell callers we
// accept what the broker would actually drop. attachments_ok is false
// because the claude-code adapter has no facility for them in v1.
const (
	defaultMaxPayload    = "1MB"
	defaultAttachmentsOK = false
)

// Config tunes a single shim run. All fields except Adapter have env
// defaults applied by the caller (cmd/orch-agent-shim/main.go).
type Config struct {
	// Agent is the canonical harness name in metadata.agent —
	// "claude-code", "codex", "pi", "gemini". Distinct from the
	// subject token, which abbreviates (claude-code → cc).
	Agent string

	// AgentToken is the abbreviated subject token. Synadia spec §2.4
	// allows full and abbreviated tokens to coexist in the namespace
	// as long as one deployment commits to one convention per
	// metadata.agent — we use "cc" for claude-code to converge with
	// Synadia's own claude-code plugin.
	AgentToken string

	// Pane is the raw tmux pane id (e.g. "%37"). It's preserved
	// verbatim in metadata.pane_id; the subject-safe form is derived
	// via encodePane.
	Pane string

	// Owner is the operator identifier. Defaults to $USER.
	Owner string

	// Session is the optional session label. When set, lands in
	// metadata.session and the heartbeat payload (§8.3) — and per
	// §3.2 marks the agent as "session-aware". Empty = omitted.
	Session string

	// SessionID, when non-empty, pins the adapter to the specific
	// harness-side JSONL transcript identified by this id (issue #11
	// path A). Each adapter resolves the id against its own naming
	// convention — claudecode opens `<encoded-cwd>/<SessionID>.jsonl`,
	// pi globs `<encoded-cwd>/*_<SessionID>.jsonl`. Empty falls back to
	// the historical latest-mtime discovery, preserving v1 behaviour for
	// callers that don't yet know the harness's session id.
	SessionID string

	// NATSURL is the bus to dial. Resolution order in main.go:
	// flag → $NATS_URL → ~/.sesh/hub.url.
	NATSURL string

	// Outfit / Role / CWD / Harness become metadata fields the
	// operator UX uses to filter discovery; they're opaque to
	// Synadia callers (which silently preserve unknown metadata
	// per §3.2 / §12).
	Outfit  string
	Role    string
	CWD     string
	Harness string

	// TaskID populates the Sesh-Task-Id envelope header on outbound
	// publishes. Empty omits the header — most callers leave this
	// unset until orch adopts sesh's task-CAS pull protocol. See
	// docs/message-envelope.md and docs/task-management.md in
	// ~/projects/sesh for the header semantics.
	TaskID string

	// Interval is the heartbeat cadence (§8.2). Defaults to 30s.
	Interval time.Duration

	// Adapter wires the harness's stdio to the shim. Required.
	Adapter Adapter

	// SubjectPrefix is the root subject for agent endpoints — also the
	// micro service name (Synadia §3.1) and the queue group (§3.3).
	// Defaults to "agents" via withDefaults. Non-orch consumers
	// (e.g. a future dagnats-agent) override to live in their own
	// namespace; Synadia's $SRV.INFO.<name> discovery then keys on the
	// override and won't collide with orch's "agents" service.
	//
	// Pulled out of the implementation per Ousterhout (information
	// leakage): callers shouldn't have to fork the shim to retarget
	// the subject tree.
	SubjectPrefix string

	// SignalPrefix is the root subject for orch.signal.> control-plane
	// handlers (orch#133's interrupt/redirect verbs). Defaults to
	// "orch.signal" via withDefaults. Non-orch consumers override to
	// route signals through their own namespace.
	SignalPrefix string
}

// Run is the shim entry point. Blocks until ctx is done or the NATS
// connection fails fatally. main.go is a thin wrapper that handles
// flags + signal plumbing.
//
// Run handles:
//
//   - dialling NATS,
//   - registering the `agents` micro service with metadata + endpoints,
//   - starting the heartbeat loop,
//   - dispatching prompts to the adapter and streaming chunks back.
//
// On return, the NATS connection is drained (so in-flight chunks
// finish flushing) and the adapter is closed.
func Run(ctx context.Context, cfg Config) error {
	cfg = withDefaults(cfg)
	if err := cfg.validate(); err != nil {
		return fmt.Errorf("shim: config: %w", err)
	}

	nc, err := nats.Connect(cfg.NATSURL,
		nats.Name(fmt.Sprintf("orch-agent-shim %s %s", cfg.Agent, cfg.Pane)),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return fmt.Errorf("shim: nats dial %q: %w", cfg.NATSURL, err)
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			log.Printf("shim: nats drain: %v", err)
		}
	}()

	return RunWithConn(ctx, nc, cfg)
}

// RunWithConn is the test-friendly entry point: same as Run but takes a
// pre-built NATS connection. Behavior tests use this with an embedded
// server. The caller owns the connection's lifetime — RunWithConn does
// NOT drain or close it.
func RunWithConn(ctx context.Context, nc *nats.Conn, cfg Config) error {
	cfg = withDefaults(cfg)
	if err := cfg.validate(); err != nil {
		return fmt.Errorf("shim: config: %w", err)
	}

	s := &shim{cfg: cfg, nc: nc, runCtx: ctx}
	if err := s.start(); err != nil {
		return err
	}
	defer s.stop()

	// Heartbeat publish loop (§8.2). Best-effort: a publish failure
	// (broker hiccup, reconnect-in-progress) just retries on next tick.
	go s.heartbeatLoop(ctx)

	// Pump the adapter's event channel onto whichever reply subject the
	// active prompt is using. Single goroutine, single active stream
	// at a time — claude-code is inherently serial (one turn per pane).
	go s.eventPump(ctx)

	<-ctx.Done()
	return nil
}

// withDefaults fills in everything the caller can reasonably omit.
// Pulled out so RunWithConn picks up the same defaults as Run.
func withDefaults(cfg Config) Config {
	if cfg.Owner == "" {
		cfg.Owner = currentOwner()
	}
	if cfg.AgentToken == "" {
		// Subject-token abbreviations the Synadia spec sanctions in §2.4.
		// Adding adapters? Add the token here. The full form (cfg.Agent)
		// always lands in metadata.agent.
		switch cfg.Agent {
		case "claude-code":
			cfg.AgentToken = "cc"
		default:
			cfg.AgentToken = cfg.Agent
		}
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.Interval < time.Second {
		// §8.2: values below 1s SHOULD NOT be used on shared infra.
		// Clamp rather than error — callers passing 0 already get the
		// 30s default; sub-second is only ever a test override. We log
		// the clamp so an operator who intentionally configured a fast
		// cadence (e.g. 500ms) sees why their setting didn't take.
		log.Printf("shim: heartbeat interval %v below 1s, clamping to 1s (§8.2)", cfg.Interval)
		cfg.Interval = time.Second
	}
	if cfg.Role == "" {
		cfg.Role = "worker"
	}
	if cfg.Harness == "" {
		cfg.Harness = cfg.Agent
	}
	if cfg.SubjectPrefix == "" {
		cfg.SubjectPrefix = defaultSubjectPrefix
	}
	if cfg.SignalPrefix == "" {
		cfg.SignalPrefix = defaultSignalPrefix
	}
	return cfg
}

func (c Config) validate() error {
	if c.Agent == "" {
		return errors.New("agent is required")
	}
	if c.Pane == "" {
		return errors.New("pane is required")
	}
	if c.Adapter == nil {
		return errors.New("adapter is required")
	}
	return nil
}

func currentOwner() string {
	if v := os.Getenv("ORCH_OWNER"); v != "" {
		return v
	}
	if v := os.Getenv("USER"); v != "" {
		return v
	}
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return "unknown"
}

// encodePane turns a raw tmux pane id like "%37" into a NATS-legal
// subject token "pct37". NATS subject grammar forbids "%". The raw id
// is preserved in metadata.pane_id so operators can map back.
//
// Decoder side: strip "pct" prefix and prepend "%" — but in practice
// callers just read metadata.pane_id.
func encodePane(pane string) string {
	// Strip the leading "%" if present, prefix with "pct". We don't
	// percent-encode arbitrary bytes — tmux pane ids are always %\d+
	// (or %s\d+ for split-window batches), so this is enough.
	return "pct" + strings.TrimPrefix(pane, "%")
}

// shim is the live agent instance. One per Run() call.
type shim struct {
	cfg Config
	nc  *nats.Conn

	// runCtx is the shim's lifetime context. Propagates to the adapter
	// via OnPrompt so cancelling the shim cancels in-flight turns,
	// and is the context the eventPump / heartbeatLoop / adapter
	// watchers select on.
	runCtx context.Context

	svc micro.Service

	// activeReply is the reply subject of the in-flight prompt; the
	// event pump publishes chunks here until a Terminator chunk arrives.
	//
	// Read on the event-pump hot path (every chunk), so we keep it in
	// an atomic.Value rather than under a mutex — writes are rare (one
	// per prompt boundary), reads are frequent. Empty string ("") means
	// no active stream. Stores are guarded by activeMu so the
	// "transition from busy to idle" check in handlePrompt is atomic
	// with the store.
	activeReply atomic.Value // string

	// activeTrace is the W3C trace_id extracted from the inbound
	// prompt's traceparent header. Propagated onto every reply chunk
	// + terminator + error in the stream's lifetime so downstream
	// observability can correlate the whole turn to one trace. Empty
	// when the inbound prompt had no traceparent — outbound publishes
	// then mint fresh traces.
	activeTrace atomic.Value // string

	// activeCancel cancels the derived context handed to Adapter.OnPrompt
	// for the in-flight turn. handleInterrupt invokes it before calling
	// Adapter.Abort so adapters that honour ctx.Done() get the stop signal
	// even if they ignore Abort. Read/written only under activeMu; the
	// hot path (eventPump) does not touch it.
	activeCancel context.CancelFunc

	activeMu sync.Mutex

	// signalSub is the nc.Subscribe handle for orch.signal.>; unsubscribed
	// on stop() so a shim restart on the same connection doesn't double-
	// dispatch signals.
	signalSub *nats.Subscription
}

// loadActiveReply returns the current active reply subject or "" if idle.
func (s *shim) loadActiveReply() string {
	v := s.activeReply.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// loadActiveTrace returns the inbound traceparent's trace_id for the
// active stream or "" when no parent context is propagating. Mid-stream
// callers (eventPump, watchdog) use this to feed envelopeHeaders so
// every reply chunk on a turn shares one trace.
func (s *shim) loadActiveTrace() string {
	v := s.activeTrace.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// sessionToken returns the 5th-token portion of the agent subject.
// Prefers cfg.Session (operator-readable name) when set; falls back to
// encodePane(Pane) for compatibility with callers that don't supply a
// session label (bench / orch-spawn-without-SESH_SESSION).
//
// The operator is expected to choose a subject-safe Session value (no
// '.', '>', '*', whitespace). No sanitization here — bad values surface
// as NATS protocol errors at subscription time.
func (s *shim) sessionToken() string {
	if s.cfg.Session != "" {
		return s.cfg.Session
	}
	return encodePane(s.cfg.Pane)
}

// promptSubject returns the §2.3 prompt endpoint subject:
//
//	<SubjectPrefix>.prompt.<token>.<owner>.<session-or-pane-enc>
func (s *shim) promptSubject() string {
	return fmt.Sprintf("%s.prompt.%s.%s.%s",
		s.cfg.SubjectPrefix, s.cfg.AgentToken, s.cfg.Owner, s.sessionToken())
}

func (s *shim) statusSubject() string {
	return fmt.Sprintf("%s.status.%s.%s.%s",
		s.cfg.SubjectPrefix, s.cfg.AgentToken, s.cfg.Owner, s.sessionToken())
}

func (s *shim) heartbeatSubject() string {
	return fmt.Sprintf("%s.hb.%s.%s.%s",
		s.cfg.SubjectPrefix, s.cfg.AgentToken, s.cfg.Owner, s.sessionToken())
}

// signalSubject is the wildcard the shim subscribes to for
// <SignalPrefix>.> dispatch. The verb (interrupt|redirect|...) is the
// wildcard token; the identity tuple (token/owner/pane-enc) pins
// delivery to this shim so peer panes don't see each other's signals.
// See docs/orch-signals.md.
func (s *shim) signalSubject() string {
	return fmt.Sprintf("%s.*.%s.%s.%s",
		s.cfg.SignalPrefix, s.cfg.AgentToken, s.cfg.Owner, encodePane(s.cfg.Pane))
}

// start registers the micro service with metadata + endpoints. Heartbeats
// are NOT yet running — caller starts heartbeatLoop after start returns.
// (§8.2: agents SHOULD begin heartbeats only after registration so
// $SRV.INFO discovery races land on a fully-formed instance.)
func (s *shim) start() error {
	cfg := micro.Config{
		Name:        s.cfg.SubjectPrefix,
		Version:     "0.3.0",
		Description: fmt.Sprintf("%s shim — %s/%s", s.cfg.Agent, s.cfg.Owner, s.cfg.Pane),
		Metadata:    s.serviceMetadata(),
	}
	svc, err := micro.AddService(s.nc, cfg)
	if err != nil {
		return fmt.Errorf("shim: register service: %w", err)
	}
	s.svc = svc

	// §12 requires both endpoints. Subject is agent-chosen — we use the
	// channel-plugin default layout from §2.3.
	if err := svc.AddEndpoint("prompt",
		micro.HandlerFunc(s.handlePrompt),
		micro.WithEndpointSubject(s.promptSubject()),
		micro.WithEndpointQueueGroup(s.cfg.SubjectPrefix),
		micro.WithEndpointMetadata(map[string]string{
			"max_payload":    defaultMaxPayload,
			"attachments_ok": strconv.FormatBool(defaultAttachmentsOK),
		}),
	); err != nil {
		_ = svc.Stop()
		return fmt.Errorf("shim: prompt endpoint: %w", err)
	}

	if err := svc.AddEndpoint("status",
		micro.HandlerFunc(s.handleStatus),
		micro.WithEndpointSubject(s.statusSubject()),
		micro.WithEndpointQueueGroup(s.cfg.SubjectPrefix),
	); err != nil {
		_ = svc.Stop()
		return fmt.Errorf("shim: status endpoint: %w", err)
	}

	// Bind the adapter's background watchers to the shim's lifetime
	// context — NOT a per-prompt context. The adapter's chunks have a
	// service to attach to by this point (above), so it's safe to
	// flush early events into the pump even before the first prompt.
	if err := s.cfg.Adapter.Start(s.runCtx); err != nil {
		_ = svc.Stop()
		return fmt.Errorf("shim: adapter start: %w", err)
	}

	// orch.signal.> subscription. Plain (non-queue, non-service)
	// subscribe: signals are fire-and-forget control-plane events, not
	// request/reply traffic, and we want every shim with the matching
	// identity tuple to receive them (no queue-group sharding).
	sub, err := s.nc.Subscribe(s.signalSubject(), s.handleSignal)
	if err != nil {
		_ = svc.Stop()
		return fmt.Errorf("shim: signal subscribe: %w", err)
	}
	s.signalSub = sub

	return nil
}

func (s *shim) stop() {
	if s.signalSub != nil {
		_ = s.signalSub.Unsubscribe()
	}
	if s.svc != nil {
		_ = s.svc.Stop()
	}
	if s.cfg.Adapter != nil {
		_ = s.cfg.Adapter.Close()
	}
}

// serviceMetadata builds the §3.2 + §12 metadata block. Keys are
// flattened to strings (micro service metadata is map[string]string).
func (s *shim) serviceMetadata() map[string]string {
	m := map[string]string{
		"agent":            s.cfg.Agent,
		"owner":            s.cfg.Owner,
		"protocol_version": ProtocolVersion,
		"pane_id":          s.cfg.Pane,
		"role":             s.cfg.Role,
		"harness":          s.cfg.Harness,
	}
	if s.cfg.Session != "" {
		m["session"] = s.cfg.Session
	}
	if s.cfg.Outfit != "" {
		m["outfit"] = s.cfg.Outfit
	}
	if s.cfg.CWD != "" {
		m["cwd"] = s.cfg.CWD
	}
	return m
}

// instanceID is the micro service id. Stable for the lifetime of the
// process; matches what $SRV.INFO.agents reports under `id`. Used as
// instance_id in heartbeats and status replies (§8.3 / §8.7).
func (s *shim) instanceID() string {
	if s.svc == nil {
		return ""
	}
	return s.svc.Info().ID
}

// heartbeatLoop publishes a §8.3 payload every cfg.Interval. Best-effort —
// a publish error is logged via the nats.Conn error handler (default:
// nowhere) and the loop continues. §8.2: agents SHOULD begin only after
// registration; start() runs before this loop.
func (s *shim) heartbeatLoop(ctx context.Context) {
	// Publish one immediately so any caller that just discovered us via
	// $SRV.INFO can confirm liveness without waiting a full interval.
	s.publishHeartbeat()
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.publishHeartbeat()
		}
	}
}

// buildHeartbeat constructs the §8.3 payload. Shared between the
// pub/sub heartbeat and the §8.7 status endpoint reply, per §8.7.1
// implementation notes.
func (s *shim) buildHeartbeat() heartbeatPayload {
	p := heartbeatPayload{
		Agent:      s.cfg.Agent,
		Owner:      s.cfg.Owner,
		InstanceID: s.instanceID(),
		TS:         time.Now().UTC().Format(time.RFC3339),
		IntervalS:  int(s.cfg.Interval / time.Second),
	}
	if s.cfg.Session != "" {
		p.Session = s.cfg.Session
	}
	return p
}

func (s *shim) publishHeartbeat() {
	body, err := json.Marshal(s.buildHeartbeat())
	if err != nil {
		return
	}
	// Heartbeats are not part of any prompt's trace; mint fresh.
	hdr := envelopeHeaders(s.cfg.Role, s.cfg.TaskID, "", firstAttempt)
	msg := &nats.Msg{
		Subject: s.heartbeatSubject(),
		Header:  hdr,
		Data:    body,
	}
	_ = s.nc.PublishMsg(msg)
}

// heartbeatPayload is the wire shape from §8.3. snake_case keys match
// the on-wire form; the struct is also used by the §8.7 status handler.
type heartbeatPayload struct {
	Agent      string `json:"agent"`
	Owner      string `json:"owner"`
	Session    string `json:"session,omitempty"`
	InstanceID string `json:"instance_id"`
	TS         string `json:"ts"`
	IntervalS  int    `json:"interval_s"`
}

// handleStatus implements §8.7 — request body ignored, reply is a
// freshly-built §8.3 heartbeat payload.
func (s *shim) handleStatus(req micro.Request) {
	parentTrace := traceFromHeaders(nats.Header(req.Headers()))
	if parentTrace == "" {
		parentTrace = newTraceID()
	}
	hdr := envelopeHeaders(s.cfg.Role, s.cfg.TaskID, parentTrace, firstAttempt)
	opt := micro.WithHeaders(micro.Headers(hdr))
	body, err := json.Marshal(s.buildHeartbeat())
	if err != nil {
		// §8.7.1: build failures MUST be a 500.
		_ = req.Error("500", "status payload build failed", nil, opt)
		return
	}
	_ = req.Respond(body, opt)
}

// requestEnvelope is the §5.1 JSON envelope (only the fields the shim
// uses; unknown fields are tolerated per §5.6 / §12).
type requestEnvelope struct {
	Prompt      string                   `json:"prompt"`
	Attachments []map[string]interface{} `json:"attachments,omitempty"`
}

// parseEnvelope accepts either plain UTF-8 text (shorthand) or a JSON
// envelope (§5.1 / §5.3 discrimination rule: leading byte after
// trimming whitespace is `{` → JSON, else plain-text).
func parseEnvelope(body []byte) (requestEnvelope, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return requestEnvelope{}, errors.New("empty payload")
	}
	if strings.HasPrefix(trimmed, "{") {
		var env requestEnvelope
		if err := json.Unmarshal([]byte(trimmed), &env); err != nil {
			return env, fmt.Errorf("malformed JSON envelope: %w", err)
		}
		if env.Prompt == "" {
			return env, errors.New("envelope.prompt is required")
		}
		return env, nil
	}
	return requestEnvelope{Prompt: trimmed}, nil
}

// handlePrompt is the §5 / §6 dispatcher. It:
//
//   - validates the envelope (§5.4 / §12: reject malformed with 400),
//   - delegates the busy/ack/OnPrompt/watchdog work to dispatchPrompt,
//   - maps dispatchPrompt's structured error (today: busy=503) to
//     the §9.x reply.
//
// The handler does NOT block until the adapter is done. micro service
// handlers are synchronous on the dispatch goroutine; blocking here
// would serialize prompts (which for a single pane is actually fine,
// but we keep the option open). We accept new prompts only when no
// other stream is active.
func (s *shim) handlePrompt(req micro.Request) {
	// Extract the inbound traceparent's trace_id once so every outbound
	// publish on this prompt — early-exit rejection, ack, reply chunks,
	// error chunk — shares the same trace. When the inbound carries no
	// traceparent we mint a per-turn trace at the boundary so chunks
	// within the turn still correlate as one workflow span on the
	// observability backend; without this each chunk would be its own
	// trace and downstream tools couldn't group them.
	parentTrace := traceFromHeaders(nats.Header(req.Headers()))
	if parentTrace == "" {
		parentTrace = newTraceID()
	}

	body := req.Data()
	env, err := parseEnvelope(body)
	if err != nil {
		// §9.2: 400 for malformed envelope / empty body. Emit the
		// error-headered message THEN the terminator (§9.3 / B.10) —
		// even pre-ack, so callers waiting on the inactivity timeout
		// (§6.6) see a clean close instead of timing out.
		s.respondError(req, 400, err.Error(), nil, parentTrace)
		s.publishTerminator(req.Reply())
		return
	}
	if !defaultAttachmentsOK && len(env.Attachments) > 0 {
		s.respondError(req, 400, "attachments not accepted by this agent", nil, parentTrace)
		s.publishTerminator(req.Reply())
		return
	}

	if e := s.dispatchPrompt(req.Reply(), env.Prompt, parentTrace); e != nil {
		s.respondError(req, e.Code, e.Message, e.Body, parentTrace)
		s.publishTerminator(req.Reply())
	}
}

// dispatchPrompt is the per-turn core shared between handlePrompt
// (operator-driven via agents.prompt.* request/reply) and handleRedirect
// (operator-driven via orch.signal.redirect.* publish). It:
//
//   - atomically claims the active-reply slot (returns 503 if busy),
//   - publishes the mandatory §6.4 `ack` chunk,
//   - derives a cancellable per-turn ctx (stored as activeCancel so the
//     §interrupt verb can cancel mid-flight),
//   - hands the prompt to Adapter.OnPrompt and arms the §6.5 watchdog.
//
// On structured failure (busy), returns a *Error and leaves the active-
// reply slot untouched (caller emits the header-bearing error message +
// terminator). On success, returns nil and the event pump owns the
// stream until the adapter terminates it. OnPrompt errors and ack
// publish failures are surfaced via the reply stream directly (mid-
// stream error chunk + terminator) rather than back to the caller,
// matching the prior handlePrompt behaviour.
func (s *shim) dispatchPrompt(reply string, prompt string, parentTrace string) *Error {
	// Atomic "transition from idle to busy". activeMu serializes the
	// check-then-store; the value itself is read lock-free in eventPump.
	s.activeMu.Lock()
	if s.loadActiveReply() != "" {
		s.activeMu.Unlock()
		// §9.2 503 (service unavailable) is the cleanest mapping for
		// "agent busy with another turn". Caller can retry.
		return &Error{Code: 503, Message: "agent busy"}
	}
	// Derive a per-turn ctx that the interrupt handler can cancel
	// without dismantling the shim. OnPrompt receives this ctx so
	// adapters honouring ctx.Done() pick up the abort even if they
	// don't implement the Aborter interface.
	promptCtx, cancel := context.WithCancel(s.runCtx)
	s.activeReply.Store(reply)
	// Stash the inbound trace so mid-stream publishes (eventPump,
	// watchdog) propagate it onto every chunk + terminator + error.
	s.activeTrace.Store(parentTrace)
	s.activeCancel = cancel
	s.activeMu.Unlock()

	// §6.4: mandatory `ack` is the FIRST message on the reply subject,
	// before any latency-inducing work. Send before invoking the adapter.
	if err := s.publishChunk(reply, Chunk{Type: ChunkStatus, Data: "ack"}, parentTrace); err != nil {
		s.clearActive(reply)
		return nil
	}

	// Kick off the agent turn. OnPrompt should return promptly — chunks
	// arrive via Events(). The ctx we pass is per-turn-cancellable so
	// the §interrupt verb can stop the adapter mid-flight (adapters
	// honouring ctx.Done() pick it up; the TUI adapters also receive
	// an Abort call from handleInterrupt). If OnPrompt itself fails,
	// we end the stream with a 500 + terminator.
	if err := s.cfg.Adapter.OnPrompt(promptCtx, prompt); err != nil {
		_ = s.publishErrorOnReply(reply, &Error{
			Code:    500,
			Message: err.Error(),
		}, parentTrace)
		s.publishTerminator(reply)
		s.clearActive(reply)
		return nil
	}

	// §6.5 watchdog. The protocol REQUIRES every prompt stream end with
	// a zero-body terminator (§6.5); the event pump emits it when the
	// adapter sends a Terminator chunk or closes its Events() channel.
	// Mock harnesses that never close Events() would otherwise leave
	// callers blocked on the §6.6 inactivity timeout (#102). Centralize
	// the safety net here so no adapter can violate the invariant.
	//
	// Snapshot terminatorWatchdog HERE (on the caller goroutine) instead
	// of reading it inside watchdogTerminator. Tests mutate the
	// package-level value via t.Cleanup; reading it from a long-lived
	// watchdog goroutine races with that cleanup after the test ends but
	// before the goroutine's runCtx fires. Snapshotting at dispatch time
	// pins the value to the prompt's lifetime, eliminating the race.
	go s.watchdogTerminator(reply, terminatorWatchdog)
	return nil
}

// redirectEnvelope is the body shape for orch.signal.redirect.* (§v1).
// Minimal: just the replacement prompt text plus the reply subject the
// new turn's chunks should stream to. ReplyTo is optional — if absent
// the shim mints a fresh _INBOX so the new turn still has somewhere to
// publish (and operators who don't subscribe simply discard chunks).
// See docs/orch-signals.md for forward-compat fields.
type redirectEnvelope struct {
	Prompt  string `json:"prompt"`
	ReplyTo string `json:"reply,omitempty"`
}

// handleSignal is the <SignalPrefix>.> dispatcher. The verb is the
// token immediately after the prefix
// (<SignalPrefix>.<verb>.<token>.<owner>.<pane>) — unknown verbs are
// logged and dropped (forward-compat with future
// pause/resume/snapshot verbs).
func (s *shim) handleSignal(msg *nats.Msg) {
	prefixDots := strings.Count(s.cfg.SignalPrefix, ".") + 1
	parts := strings.Split(msg.Subject, ".")
	if len(parts) <= prefixDots {
		log.Printf("shim: signal: malformed subject %q", msg.Subject)
		return
	}
	verb := parts[prefixDots]
	switch verb {
	case "interrupt":
		s.handleInterrupt()
	case "redirect":
		s.handleRedirect(msg.Data)
	default:
		// Unknown verb — log once and ignore. Keeps the room-to-grow
		// space (pause/resume/snapshot) ABI-stable for callers built
		// against a newer shim that adds verbs we don't recognize.
		log.Printf("shim: signal: unknown verb %q on %q", verb, msg.Subject)
	}
}

// handleInterrupt stops the in-flight turn:
//
//  1. Cancels the derived OnPrompt ctx (adapters honouring ctx.Done()
//     observe the stop immediately).
//  2. Calls Adapter.Abort if the adapter implements Aborter (TUI
//     adapters send `tmux send-keys C-c` to the bound pane).
//  3. Publishes {"type":"status","data":"aborted"} on the active reply
//     subject — reuses the §6.4 status-chunk shape so subscribers don't
//     need a new wire form to recognize the abort.
//  4. Publishes the §6.5 zero-byte terminator and releases the slot.
//
// Idempotent: if no turn is active, the function is a no-op (multi-
// signal coalescing — N concurrent interrupts close once, the rest
// observe an empty activeReply and return).
func (s *shim) handleInterrupt() {
	s.activeMu.Lock()
	reply := s.loadActiveReply()
	if reply == "" {
		s.activeMu.Unlock()
		return
	}
	trace := s.loadActiveTrace()
	cancel := s.activeCancel
	// Release the slot under the mutex so a concurrent prompt arriving
	// between our publishChunk and publishTerminator below sees idle
	// and can claim the slot for its own ack — the just-aborted stream
	// is logically done the moment we decided to interrupt it.
	s.activeReply.Store("")
	s.activeTrace.Store("")
	s.activeCancel = nil
	s.activeMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if a, ok := s.cfg.Adapter.(Aborter); ok {
		// Use a fresh ctx — the shim's lifetime ctx, not the cancelled
		// per-turn one — so the adapter has time to deliver the stop
		// signal (e.g. tmux send-keys) without being immediately
		// cancelled by its own ctx.
		_ = a.Abort(s.runCtx)
	}
	_ = s.publishChunk(reply, Chunk{Type: ChunkStatus, Data: "aborted"}, trace)
	s.publishTerminator(reply)
}

// handleRedirect = interrupt + dispatch the new prompt. The body is a
// redirectEnvelope ({"prompt":"...","reply":"..."}). If reply is empty
// the shim mints a fresh _INBOX so the new turn has somewhere to stream.
// Malformed bodies are logged and dropped (no caller to reply to).
func (s *shim) handleRedirect(body []byte) {
	var env redirectEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		log.Printf("shim: signal: redirect body parse: %v", err)
		return
	}
	if env.Prompt == "" {
		log.Printf("shim: signal: redirect body missing prompt")
		return
	}
	s.handleInterrupt()
	reply := env.ReplyTo
	if reply == "" {
		reply = nats.NewInbox()
	}
	// Redirects do not carry an inbound traceparent (the publish is
	// fire-and-forget), so the new turn mints its own trace. Operators
	// who care about correlation should set the traceparent header on
	// the redirect publish before we add traceparent extraction here.
	if e := s.dispatchPrompt(reply, env.Prompt, newTraceID()); e != nil {
		// 503 (still busy somehow — race with another prompt that
		// claimed the slot between our interrupt and dispatch) is the
		// only structured error today; publish it on the synthesized
		// reply subject so any subscriber still sees a terminator.
		_ = s.publishErrorOnReply(reply, e, "")
		s.publishTerminator(reply)
	}
}

// watchdogTerminator force-emits a §6.5 terminator on `reply` if no
// terminator has arrived by `timeout`. Idempotent with the event pump's
// terminator path via finishStream's atomic check — whichever fires
// first wins, the other observes a mismatched activeReply and no-ops.
//
// `timeout` is snapshotted by dispatchPrompt so this goroutine never
// touches the package-level terminatorWatchdog, which tests mutate via
// t.Cleanup (would otherwise race when the cleanup runs while a
// watchdog goroutine from the same test is still draining).
func (s *shim) watchdogTerminator(reply string, timeout time.Duration) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-s.runCtx.Done():
		return
	case <-t.C:
		s.finishStream(reply)
	}
}

// finishStream atomically checks whether `reply` is still the active
// stream and, if so, publishes a §6.5 terminator and releases the slot.
// Returns true iff this call performed the close. Used by the watchdog
// to avoid racing the event pump's terminator path: clearActive's mutex
// serializes the two competing closers.
func (s *shim) finishStream(reply string) bool {
	s.activeMu.Lock()
	if s.loadActiveReply() != reply {
		s.activeMu.Unlock()
		return false
	}
	s.activeReply.Store("")
	s.activeTrace.Store("")
	if s.activeCancel != nil {
		s.activeCancel()
		s.activeCancel = nil
	}
	s.activeMu.Unlock()
	s.publishTerminator(reply)
	return true
}

// eventPump drains the adapter's event channel and publishes each chunk
// on whichever reply subject is active. A Terminator chunk closes the
// active stream and clears activeReply so the next prompt can run.
//
// The hot path reads activeReply lock-free via atomic.Value; only the
// "claim the slot" path in handlePrompt and the "release the slot"
// path in clearActive take activeMu.
func (s *shim) eventPump(ctx context.Context) {
	events := s.cfg.Adapter.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case c, ok := <-events:
			if !ok {
				return
			}
			reply := s.loadActiveReply()
			if reply == "" {
				// No active stream — adapter emitted a chunk between
				// turns (or before the first prompt). Drop it; there's
				// nowhere to send it.
				continue
			}
			trace := s.loadActiveTrace()
			if c.Err != nil {
				_ = s.publishErrorOnReply(reply, c.Err, trace)
				s.publishTerminator(reply)
				s.clearActive(reply)
				continue
			}
			if c.Terminator {
				s.publishTerminator(reply)
				s.clearActive(reply)
				continue
			}
			_ = s.publishChunk(reply, c, trace)
		}
	}
}

// clearActive resets activeReply iff it matches `reply`. Guards against
// a race where two streams' terminators arrive in quick succession.
// Also clears activeTrace so the next prompt's trace doesn't inherit
// stale context if the new prompt happens to lack a traceparent.
func (s *shim) clearActive(reply string) {
	s.activeMu.Lock()
	if s.loadActiveReply() == reply {
		s.activeReply.Store("")
		s.activeTrace.Store("")
		if s.activeCancel != nil {
			s.activeCancel()
			s.activeCancel = nil
		}
	}
	s.activeMu.Unlock()
}

// publishChunk encodes a non-terminator §6.2 chunk and publishes it
// with sesh envelope headers (W3C traceparent + Sesh-*). parentTrace
// is the trace_id to propagate from the inbound prompt; pass "" to mint
// a fresh trace. Returns publish error so callers can decide whether to
// keep streaming.
func (s *shim) publishChunk(reply string, c Chunk, parentTrace string) error {
	body, err := encodeChunk(c)
	if err != nil {
		return err
	}
	hdr := envelopeHeaders(s.cfg.Role, s.cfg.TaskID, parentTrace, firstAttempt)
	return s.nc.PublishMsg(&nats.Msg{
		Subject: reply,
		Header:  hdr,
		Data:    body,
	})
}

// encodeChunk produces the JSON bytes for a §6.2 chunk. Separated from
// publishChunk so unit tests can assert exact wire shape against the
// Appendix B examples.
func encodeChunk(c Chunk) ([]byte, error) {
	if c.Terminator || c.Err != nil {
		return nil, errors.New("encodeChunk: terminator/error chunks are not JSON-encoded")
	}
	wire := struct {
		Type string `json:"type"`
		Data any    `json:"data"`
	}{
		Type: string(c.Type),
		Data: c.Data,
	}
	return json.Marshal(wire)
}

// publishTerminator emits the §6.5 zero-byte headerless terminator.
// Sesh envelope headers (traceparent + Sesh-*) are NOT attached here:
// Synadia §6.5 / §9.3 specify the terminator as "no headers" and the
// conformance test enforces it. Trace correlation is preserved across
// the rest of the turn via the ack + response chunks + (when relevant)
// the error chunk — every one of those carries the same trace_id. The
// terminator is the one publish that intentionally drops envelope
// metadata so it stays distinguishable from an error reply (which
// always carries Nats-Service-Error-* headers) and from a normal
// response chunk (which has body bytes).
//
// Best-effort; a publish error here means the caller's stream will
// time out via §6.6 inactivity, which is acceptable degradation.
func (s *shim) publishTerminator(reply string) {
	_ = s.nc.Publish(reply, nil)
}

// respondError emits the FIRST message of an error-terminated stream:
// a message carrying `Nats-Service-Error-Code` / `Nats-Service-Error`
// headers per §9.1 / B.10 (message 1), plus sesh envelope headers for
// trace correlation. Callers MUST publish the §6.5 empty terminator
// separately afterward — `nats.go/micro`'s `request.Error` only
// publishes the single header-bearing message; it does NOT also emit
// the terminator. We invoke publishTerminator from the call sites so
// the invariant is uniform across pre-ack rejection (handlePrompt's
// early-exit paths) and mid-stream errors (eventPump's error-chunk
// path) — every error path produces exactly two messages: the
// header-bearing signal, then the terminator.
func (s *shim) respondError(req micro.Request, code int, msg string, body map[string]any, parentTrace string) {
	codeStr := strconv.Itoa(code)
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	hdr := envelopeHeaders(s.cfg.Role, s.cfg.TaskID, parentTrace, firstAttempt)
	_ = req.Error(codeStr, msg, raw, micro.WithHeaders(micro.Headers(hdr)))
}

// publishErrorOnReply is the §9.3 / B.10 path: error mid-stream. The
// caller publishes one header-bearing message, then the empty terminator
// (which publishTerminator emits separately). Envelope headers ride
// alongside the Nats-Service-Error-* headers so the error event itself
// is traceable.
func (s *shim) publishErrorOnReply(reply string, e *Error, parentTrace string) error {
	if e == nil {
		return nil
	}
	hdr := envelopeHeaders(s.cfg.Role, s.cfg.TaskID, parentTrace, firstAttempt)
	hdr.Set("Nats-Service-Error-Code", strconv.Itoa(e.Code))
	hdr.Set("Nats-Service-Error", e.Message)
	var body []byte
	if e.Body != nil {
		body, _ = json.Marshal(e.Body)
	}
	return s.nc.PublishMsg(&nats.Msg{Subject: reply, Header: hdr, Data: body})
}

// NewResponseChunk is a constructor for `response` chunks. `data` may be
// a string (B.4) or an object (B.5) — anything json.Marshal handles.
func NewResponseChunk(data any) Chunk {
	return Chunk{Type: ChunkResponse, Data: data}
}

// NewStatusChunk wraps a string `status` chunk (§6.4). Adapters
// generally don't call this — the shim emits the mandatory `ack` itself.
func NewStatusChunk(value string) Chunk {
	return Chunk{Type: ChunkStatus, Data: value}
}

// NewQueryChunk wraps a §7.1 mid-stream query.
func NewQueryChunk(id, replySubject, prompt string) Chunk {
	return Chunk{Type: ChunkQuery, Data: QueryData{
		ID: id, ReplySubject: replySubject, Prompt: prompt,
	}}
}

// NewTerminatorChunk signals end-of-stream to the event pump.
func NewTerminatorChunk() Chunk { return Chunk{Terminator: true} }

// NewErrorChunk attaches an error to be emitted before the next terminator.
func NewErrorChunk(code int, msg string, body map[string]any) Chunk {
	return Chunk{Err: &Error{Code: code, Message: msg, Body: body}}
}

// ReadNATSURL resolves the NATS URL using the documented precedence:
// explicit override → env → ~/.sesh/hub.url → default localhost.
// Exposed for main.go and behavior tests.
func ReadNATSURL(override string) string {
	if override != "" {
		return override
	}
	if v := os.Getenv("NATS_URL"); v != "" {
		return v
	}
	if home, err := os.UserHomeDir(); err == nil {
		hubPath := filepath.Join(home, ".sesh", "hub.url")
		if data, err := os.ReadFile(hubPath); err == nil {
			if v := strings.TrimSpace(string(data)); v != "" {
				return v
			}
		}
	}
	return nats.DefaultURL
}
