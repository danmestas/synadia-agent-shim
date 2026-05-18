package shim

// conformance_test.go exercises the §12 agent-checklist requirements
// using shim-internal types (nopAdapter, heartbeatPayload, serviceInfo).
//
// The full generic conformance framework (usable by any adapter package)
// lives in internal/shimtest/conformance.go.
//
// Requirements exercised (§12 agent checklist):
//
//	§3.1   Service registered as `agents`
//	§3.2   metadata.agent / owner / protocol_version present
//	§6.4   First reply chunk is `{"type":"status","data":"ack"}`
//	§6.2   Response chunks have {"type":"response","data":…} shape
//	§6.5   Stream ends with zero-byte terminator (no headers)
//	§8.7   `status` endpoint replies with §8.3 heartbeat payload
//	§8.1   Heartbeat published on agents.hb.<token>.<owner>.<enc-pane>
//	§9.2   Empty payload rejected with 400 + terminator
//	§9.3   Error stream: error-header message followed by terminator

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// conformanceCase is one §12 requirement tested end-to-end.
type conformanceCase struct {
	name        string
	needsChunks bool // skip for nopAdapter (it never emits chunks)
	run         func(t *testing.T, nc *nats.Conn, cfg Config)
}

var conformanceCases = []conformanceCase{
	{
		name: "§3.1 service name is agents",
		run: func(t *testing.T, nc *nats.Conn, _ Config) {
			t.Helper()
			msg, err := nc.Request("$SRV.INFO.agents", nil, time.Second)
			if err != nil {
				t.Fatalf("$SRV.INFO.agents: %v", err)
			}
			var info serviceInfo
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
		run: func(t *testing.T, nc *nats.Conn, _ Config) {
			t.Helper()
			msg, err := nc.Request("$SRV.INFO.agents", nil, time.Second)
			if err != nil {
				t.Fatalf("$SRV.INFO.agents: %v", err)
			}
			var info serviceInfo
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
		name: "§12 prompt and status endpoints with queue group agents",
		run: func(t *testing.T, nc *nats.Conn, _ Config) {
			t.Helper()
			msg, err := nc.Request("$SRV.INFO.agents", nil, time.Second)
			if err != nil {
				t.Fatalf("$SRV.INFO.agents: %v", err)
			}
			var info serviceInfo
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
			if !found["prompt"] {
				t.Error("prompt endpoint not registered")
			}
			if !found["status"] {
				t.Error("status endpoint not registered")
			}
		},
	},
	{
		name:        "§6.4/§6.2/§6.5 prompt stream: ack-first, response chunks, zero-byte terminator",
		needsChunks: true,
		run: func(t *testing.T, nc *nats.Conn, cfg Config) {
			t.Helper()
			token := cfg.AgentToken
			if token == "" {
				if cfg.Agent == "claude-code" {
					token = "cc"
				} else {
					token = cfg.Agent
				}
			}
			subject := "agents.prompt." + token + "." + cfg.Owner + "." + encodePane(cfg.Pane)
			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(inbox)
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			defer sub.Unsubscribe()
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
		run: func(t *testing.T, nc *nats.Conn, cfg Config) {
			t.Helper()
			token := cfg.AgentToken
			if token == "" {
				if cfg.Agent == "claude-code" {
					token = "cc"
				} else {
					token = cfg.Agent
				}
			}
			subject := "agents.status." + token + "." + cfg.Owner + "." + encodePane(cfg.Pane)
			msg, err := nc.Request(subject, nil, time.Second)
			if err != nil {
				t.Fatalf("status request: %v", err)
			}
			var hb heartbeatPayload
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
		run: func(t *testing.T, nc *nats.Conn, cfg Config) {
			t.Helper()
			token := cfg.AgentToken
			if token == "" {
				if cfg.Agent == "claude-code" {
					token = "cc"
				} else {
					token = cfg.Agent
				}
			}
			hbSubject := "agents.hb." + token + "." + cfg.Owner + "." + encodePane(cfg.Pane)
			sub, err := nc.SubscribeSync(hbSubject)
			if err != nil {
				t.Fatalf("subscribe hb: %v", err)
			}
			defer sub.Unsubscribe()
			msg, err := sub.NextMsg(3 * time.Second)
			if err != nil {
				t.Fatalf("no heartbeat on %s: %v", hbSubject, err)
			}
			var hb heartbeatPayload
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
		run: func(t *testing.T, nc *nats.Conn, cfg Config) {
			t.Helper()
			token := cfg.AgentToken
			if token == "" {
				if cfg.Agent == "claude-code" {
					token = "cc"
				} else {
					token = cfg.Agent
				}
			}
			subject := "agents.prompt." + token + "." + cfg.Owner + "." + encodePane(cfg.Pane)
			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(inbox)
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			defer sub.Unsubscribe()
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

// TestAdapterConformance_Nop runs all §12 protocol-infrastructure cases
// against nopAdapter. Cases that require chunk emission are skipped.
//
// For a full end-to-end run (including chunk emission), see
// TestAdapterConformance in internal/adapter/echo/echo_test.go.
func TestAdapterConformance_Nop(t *testing.T) {
	for _, tc := range conformanceCases {
		tc := tc
		if tc.needsChunks {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			url := startEmbeddedNATS(t)
			cfg := Config{
				Agent:    "nop",
				Pane:     "%1",
				Owner:    "conformance-runner",
				Adapter:  &nopAdapter{},
				Interval: time.Second, // fast heartbeat so §8 tests don't wait 30s
			}
			nc, cleanup := runShimInBackground(t, url, cfg)
			defer cleanup()
			tc.run(t, nc, cfg)
		})
	}
}
