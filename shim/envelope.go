// envelope.go — sesh message-envelope headers for outbound NATS publishes.
//
// Spec: ~/projects/sesh/docs/message-envelope.md. Every outbound publish
// the shim makes carries:
//
//   - traceparent  (W3C standard, REQUIRED — https://www.w3.org/TR/trace-context/)
//   - Sesh-Envelope: 1
//   - Sesh-Role         (configurable; defaults to "worker")
//   - Sesh-Task-Id      (optional; omitted when unset)
//   - Sesh-Attempt      (retry counter; 1 on first publish)
//
// When an inbound message carries a traceparent, the shim propagates the
// trace_id portion onto every outbound publish in that stream's lifetime
// and mints a fresh span_id per hop. When no parent is present, the shim
// mints a fresh trace per publish.
//
// Adding envelope headers to the §6.5 terminator: the Synadia spec
// describes the terminator as "zero-body". It does NOT forbid arbitrary
// headers, only the Nats-Service-Error-* family (which would signal an
// error reply). Sesh envelope headers are orthogonal to terminator
// detection — receivers test `len(msg.Data) == 0`, not header absence.
package shim

import (
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strconv"

	"github.com/nats-io/nats.go"
)

const (
	HeaderTraceparent = "traceparent"
	HeaderEnvelope    = "Sesh-Envelope"
	HeaderRole        = "Sesh-Role"
	HeaderTaskID      = "Sesh-Task-Id"
	HeaderAttempt     = "Sesh-Attempt"

	envelopeVersion = "1"

	invalidTraceID = "00000000000000000000000000000000"
	invalidSpanID  = "0000000000000000"
)

var traceparentRE = regexp.MustCompile(`^[0-9a-f]{2}-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$`)

func newTraceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func newSpanID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// parseTraceparent extracts the trace_id from a W3C traceparent header.
// Returns "" if the header is malformed or carries the reserved zero
// trace/span ids (which the spec marks as invalid).
func parseTraceparent(s string) string {
	m := traceparentRE.FindStringSubmatch(s)
	if m == nil {
		return ""
	}
	if m[1] == invalidTraceID || m[2] == invalidSpanID {
		return ""
	}
	return m[1]
}

// newTraceparent builds a W3C-compliant traceparent. If parentTraceID is
// a valid 32-hex string the new header reuses it (making this hop a
// child span on the same trace). Otherwise a fresh trace is minted.
func newTraceparent(parentTraceID string) string {
	tid := parentTraceID
	if len(tid) != 32 {
		tid = newTraceID()
	}
	return "00-" + tid + "-" + newSpanID() + "-01"
}

// envelopeHeaders returns the sesh envelope headers for an outbound
// publish. parentTraceID is the trace-id from an inbound traceparent if
// any; pass "" to mint fresh. attempt is the retry counter (1 on first
// publish, increment on retries). taskID empty omits the header.
func envelopeHeaders(role, taskID, parentTraceID string, attempt int) nats.Header {
	hdr := nats.Header{}
	hdr.Set(HeaderTraceparent, newTraceparent(parentTraceID))
	hdr.Set(HeaderEnvelope, envelopeVersion)
	if role == "" {
		role = "worker"
	}
	hdr.Set(HeaderRole, role)
	if taskID != "" {
		hdr.Set(HeaderTaskID, taskID)
	}
	if attempt < 1 {
		attempt = 1
	}
	hdr.Set(HeaderAttempt, strconv.Itoa(attempt))
	return hdr
}

// traceFromHeaders extracts the trace_id from an inbound message's
// headers. Empty result means "no parent — mint fresh".
func traceFromHeaders(h nats.Header) string {
	if h == nil {
		return ""
	}
	return parseTraceparent(h.Get(HeaderTraceparent))
}
