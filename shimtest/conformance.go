// Package shimtest provides the §12 Synadia Agent Protocol conformance
// test framework for shim.Adapter implementations.
//
// Usage in an adapter test file:
//
//	func TestConformance(t *testing.T) {
//	    shimtest.RunAdapterConformance(t, func() shim.Adapter {
//	        return myadapter.New("%1")
//	    }, shimtest.ConformanceConfig{
//	        Agent: "my-agent",
//	        Pane:  "%1",
//	        Owner: "conformance-runner",
//	    })
//	}
//
// The framework starts an in-process NATS server, registers the shim
// with the given adapter, and asserts each §12 requirement in a
// table-driven sub-test.
package shimtest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/synadia-agent-shim/shim"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// AdapterFactory creates a fresh Adapter for each conformance sub-test.
type AdapterFactory func() shim.Adapter

// ConformanceConfig is the minimal shim.Config subset needed for
// conformance tests. The framework creates a full shim.Config from it.
type ConformanceConfig struct {
	Agent string // canonical harness name, e.g. "echo"
	Pane  string // tmux pane id, e.g. "%1"
	Owner string // owner identifier
}

// conformanceCase is one §12 requirement.
type conformanceCase struct {
	name        string
	needsChunks bool // requires adapter to emit response chunks (skip for nop)
	run         func(t *testing.T, nc *nats.Conn, agent, owner, pane string)
}

var cases = []conformanceCase{
	{
		name: "§3.1 service name is agents",
		run: func(t *testing.T, nc *nats.Conn, _, _, _ string) {
			t.Helper()
			msg, err := nc.Request("$SRV.INFO.agents", nil, time.Second)
			if err != nil {
				t.Fatalf("$SRV.INFO.agents: %v", err)
			}
			var info struct {
				Name string `json:"name"`
			}
			if err := json.Unmarshal(msg.Data, &info); err != nil {
				t.Fatalf("decode INFO: %v", err)
			}
			if info.Name != "agents" {
				t.Errorf("service name: got %q want agents", info.Name)
			}
		},
	},
	{
		name: "§3.2 metadata has agent/owner/protocol_version",
		run: func(t *testing.T, nc *nats.Conn, _, _, _ string) {
			t.Helper()
			msg, err := nc.Request("$SRV.INFO.agents", nil, time.Second)
			if err != nil {
				t.Fatalf("$SRV.INFO.agents: %v", err)
			}
			var info struct {
				Metadata map[string]string `json:"metadata"`
			}
			if err := json.Unmarshal(msg.Data, &info); err != nil {
				t.Fatalf("decode INFO: %v", err)
			}
			for _, key := range []string{"agent", "owner", "protocol_version"} {
				if info.Metadata[key] == "" {
					t.Errorf("metadata.%s is empty", key)
				}
			}
		},
	},
	{
		name: "§12 prompt and status endpoints registered with queue group agents",
		run: func(t *testing.T, nc *nats.Conn, _, _, _ string) {
			t.Helper()
			msg, err := nc.Request("$SRV.INFO.agents", nil, time.Second)
			if err != nil {
				t.Fatalf("$SRV.INFO.agents: %v", err)
			}
			var info struct {
				Endpoints []struct {
					Name       string `json:"name"`
					QueueGroup string `json:"queue_group"`
				} `json:"endpoints"`
			}
			if err := json.Unmarshal(msg.Data, &info); err != nil {
				t.Fatalf("decode INFO: %v", err)
			}
			found := map[string]bool{}
			for _, ep := range info.Endpoints {
				found[ep.Name] = true
				if ep.QueueGroup != "agents" {
					t.Errorf("endpoint %s queue_group: got %q want agents", ep.Name, ep.QueueGroup)
				}
			}
			for _, name := range []string{"prompt", "status"} {
				if !found[name] {
					t.Errorf("%s endpoint not registered", name)
				}
			}
		},
	},
	{
		name:        "§6.4/§6.2/§6.5 prompt stream: ack-first, response chunks, zero-byte terminator",
		needsChunks: true,
		run: func(t *testing.T, nc *nats.Conn, agent, owner, pane string) {
			t.Helper()
			token := agentToken(agent)
			subject := "agents.prompt." + token + "." + owner + "." + encodePane(pane)
			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(inbox)
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.PublishRequest(subject, inbox, []byte("hello")); err != nil {
				t.Fatalf("publish: %v", err)
			}

			msgs := readStream(t, sub, 3, 3*time.Second)

			var firstChunk struct {
				Type string `json:"type"`
				Data any    `json:"data"`
			}
			if err := json.Unmarshal(msgs[0].Data, &firstChunk); err != nil {
				t.Fatalf("decode first chunk: %v\nraw: %s", err, msgs[0].Data)
			}
			if firstChunk.Type != "status" {
				t.Errorf("§6.4: first chunk type: got %q want status", firstChunk.Type)
			}
			if firstChunk.Data != "ack" {
				t.Errorf("§6.4: first chunk data: got %v want ack", firstChunk.Data)
			}

			var secondChunk struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(msgs[1].Data, &secondChunk); err != nil {
				t.Fatalf("decode second chunk: %v\nraw: %s", err, msgs[1].Data)
			}
			if secondChunk.Type != "response" {
				t.Errorf("§6.2: expected response chunk, got type %q", secondChunk.Type)
			}

			last := msgs[len(msgs)-1]
			if len(last.Data) != 0 {
				t.Errorf("§6.5: terminator body not empty: %d bytes: %s", len(last.Data), last.Data)
			}
			if len(last.Header) != 0 {
				t.Errorf("§6.5: terminator should have no headers, got %v", last.Header)
			}
		},
	},
	{
		name: "§8.7 status endpoint replies with heartbeat payload",
		run: func(t *testing.T, nc *nats.Conn, agent, owner, pane string) {
			t.Helper()
			token := agentToken(agent)
			subject := "agents.status." + token + "." + owner + "." + encodePane(pane)
			msg, err := nc.Request(subject, nil, time.Second)
			if err != nil {
				t.Fatalf("status request: %v", err)
			}
			var hb struct {
				Agent      string `json:"agent"`
				Owner      string `json:"owner"`
				InstanceID string `json:"instance_id"`
				TS         string `json:"ts"`
				IntervalS  int    `json:"interval_s"`
			}
			if err := json.Unmarshal(msg.Data, &hb); err != nil {
				t.Fatalf("decode heartbeat: %v", err)
			}
			if hb.Agent == "" {
				t.Error("§8.3: agent field is empty")
			}
			if hb.Owner == "" {
				t.Error("§8.3: owner field is empty")
			}
			if hb.InstanceID == "" {
				t.Error("§8.3: instance_id is empty")
			}
			if hb.TS == "" {
				t.Error("§8.3: ts is empty")
			}
			if hb.IntervalS == 0 {
				t.Error("§8.3: interval_s is zero")
			}
		},
	},
	{
		name: "§8.1/§8.2 heartbeat published on agents.hb subject",
		run: func(t *testing.T, nc *nats.Conn, agent, owner, pane string) {
			t.Helper()
			token := agentToken(agent)
			hbSubject := "agents.hb." + token + "." + owner + "." + encodePane(pane)
			sub, err := nc.SubscribeSync(hbSubject)
			if err != nil {
				t.Fatalf("subscribe hb: %v", err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			msg, err := sub.NextMsg(3 * time.Second)
			if err != nil {
				t.Fatalf("no heartbeat on %s: %v", hbSubject, err)
			}
			var hb struct {
				Agent string `json:"agent"`
				Owner string `json:"owner"`
			}
			if err := json.Unmarshal(msg.Data, &hb); err != nil {
				t.Fatalf("decode hb: %v", err)
			}
			if hb.Agent == "" || hb.Owner == "" {
				t.Errorf("hb missing identity: agent=%q owner=%q", hb.Agent, hb.Owner)
			}
		},
	},
	{
		name: "§9.2/§9.3 empty prompt rejected with 400 + terminator",
		run: func(t *testing.T, nc *nats.Conn, agent, owner, pane string) {
			t.Helper()
			token := agentToken(agent)
			subject := "agents.prompt." + token + "." + owner + "." + encodePane(pane)
			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(inbox)
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.PublishRequest(subject, inbox, []byte("")); err != nil {
				t.Fatalf("publish: %v", err)
			}
			msg, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("error message: %v", err)
			}
			if got := msg.Header.Get("Nats-Service-Error-Code"); got != "400" {
				t.Errorf("§9.2: error code: got %q want 400", got)
			}
			term, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("§9.3: terminator missing: %v", err)
			}
			if len(term.Data) != 0 {
				t.Errorf("§9.3: terminator body not empty: %d bytes", len(term.Data))
			}
			if len(term.Header) != 0 {
				t.Errorf("§9.3: terminator should have no headers, got %v", term.Header)
			}
		},
	},
}

// RunAdapterConformance exercises all §12 conformance cases against
// the adapter produced by factory. Each sub-test gets a fresh adapter
// and a fresh in-process NATS server.
//
// skipChunkTests controls whether the prompt-stream test (which
// requires the adapter to emit response+terminator) is included.
// Pass false for adapters that emit chunks; pass true for stub adapters
// that do not (e.g. a nopAdapter used only for protocol tests).
func RunAdapterConformance(t *testing.T, factory AdapterFactory, cfg ConformanceConfig, skipChunkTests bool) {
	t.Helper()
	for _, tc := range cases {
		tc := tc
		if skipChunkTests && tc.needsChunks {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			url := startEmbeddedNATS(t)
			shimCfg := shim.Config{
				Agent:    cfg.Agent,
				Pane:     cfg.Pane,
				Owner:    cfg.Owner,
				Adapter:  factory(),
				Interval: time.Second, // fast heartbeat so §8 tests don't wait 30s
			}
			nc, cleanup := runShimInBackground(t, url, shimCfg)
			defer cleanup()
			tc.run(t, nc, cfg.Agent, cfg.Owner, cfg.Pane)
		})
	}
}

// startEmbeddedNATS runs an in-process NATS server on a random port.
func startEmbeddedNATS(t *testing.T) string {
	t.Helper()
	opts := test.DefaultTestOptions
	opts.Port = -1
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

// runShimInBackground starts the shim and waits for service registration.
func runShimInBackground(t *testing.T, url string, cfg shim.Config) (*nats.Conn, func()) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = shim.RunWithConn(ctx, nc, cfg)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := nc.Request("$SRV.PING.agents", nil, 200*time.Millisecond)
		if err == nil && msg != nil {
			break
		}
	}
	return nc, func() {
		cancel()
		<-done
		_ = nc.Drain()
	}
}

// readStream collects exactly n messages from sub within total duration.
func readStream(t *testing.T, sub *nats.Subscription, n int, total time.Duration) []*nats.Msg {
	t.Helper()
	out := make([]*nats.Msg, 0, n)
	deadline := time.Now().Add(total)
	for len(out) < n {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("readStream: timeout after %d msgs (want %d)", len(out), n)
		}
		msg, err := sub.NextMsg(remaining)
		if err != nil {
			t.Fatalf("readStream: %v after %d msgs", err, len(out))
		}
		out = append(out, msg)
	}
	return out
}

// agentToken mirrors shim.withDefaults subject-token abbreviation.
func agentToken(agent string) string {
	if agent == "claude-code" {
		return "cc"
	}
	return agent
}

// encodePane mirrors shim.encodePane.
func encodePane(pane string) string {
	return "pct" + strings.TrimPrefix(pane, "%")
}
