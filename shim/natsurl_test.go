package shim

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nats-io/nats.go"
)

// withFakeHome sets HOME to a fresh temp dir and creates the .sesh
// subdirectory inside it. t.Setenv handles cleanup automatically.
func withFakeHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	// Unset NATS_URL so the env tier in ReadNATSURL doesn't short-circuit
	// the disk-resolution path that each test cares about. Individual
	// tests that need NATS_URL set will Setenv it back.
	t.Setenv("NATS_URL", "")
	if err := os.MkdirAll(filepath.Join(home, ".sesh"), 0o755); err != nil {
		t.Fatalf("mkdir .sesh: %v", err)
	}
	return home
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestNATSURL_ReadsHubNATSURL: when ~/.sesh/hub.nats.url exists, the
// shim must prefer it over the legacy hub.url file. This is the
// substantive fix for issue #4 — hub.url is the leaf-node port and
// rejects regular NATS clients.
func TestNATSURL_ReadsHubNATSURL(t *testing.T) {
	home := withFakeHome(t)
	want := "nats://hub.example:4222"
	writeFile(t, filepath.Join(home, ".sesh", "hub.nats.url"), want+"\n")
	// Also write the leaf URL to prove we do NOT pick it up when both
	// exist — this is the exact misconfiguration shim was hitting.
	writeFile(t, filepath.Join(home, ".sesh", "hub.url"), "nats://hub.example:7422\n")

	got := ReadNATSURL("")
	if got != want {
		t.Fatalf("ReadNATSURL: want %q (client URL), got %q", want, got)
	}
}

// TestNATSURL_FallbackToHubURL: when hub.nats.url is missing but the
// legacy hub.url exists, we read it (preserving backwards-compat with
// pre-sesh#65 hub installs) and emit a deprecation warning on stderr.
func TestNATSURL_FallbackToHubURL(t *testing.T) {
	home := withFakeHome(t)
	want := "nats://legacy.example:4222"
	writeFile(t, filepath.Join(home, ".sesh", "hub.url"), want+"\n")

	// Capture stderr so we can assert the deprecation warning fires.
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	defer func() { os.Stderr = oldStderr }()

	got := ReadNATSURL("")

	if err := w.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	stderr := string(buf[:n])

	if got != want {
		t.Fatalf("ReadNATSURL: want %q (legacy URL), got %q", want, got)
	}
	wantSubstr := "shim: reading legacy ~/.sesh/hub.url (deprecated"
	if !contains(stderr, wantSubstr) {
		t.Fatalf("expected deprecation warning on stderr containing %q, got %q", wantSubstr, stderr)
	}
}

// TestNATSURL_BothMissing: when neither file exists, fall back to the
// nats.go default localhost URL. This preserves the pre-fix behavior
// for operators running a bare local nats-server.
func TestNATSURL_BothMissing(t *testing.T) {
	withFakeHome(t)
	got := ReadNATSURL("")
	if got != nats.DefaultURL {
		t.Fatalf("ReadNATSURL with no files: want %q, got %q", nats.DefaultURL, got)
	}
}

// TestNATSURL_EnvOverride: $NATS_URL trumps both files. This is the
// existing operator workaround documented in issue #4 and must remain
// functional post-fix.
func TestNATSURL_EnvOverride(t *testing.T) {
	home := withFakeHome(t)
	writeFile(t, filepath.Join(home, ".sesh", "hub.nats.url"), "nats://from-file:4222\n")

	want := "nats://from-env:4222"
	t.Setenv("NATS_URL", want)

	got := ReadNATSURL("")
	if got != want {
		t.Fatalf("ReadNATSURL: $NATS_URL should win, want %q, got %q", want, got)
	}
}

// TestNATSURL_ExplicitOverride: the override argument trumps env and
// files alike. Sanity check on the top of the precedence chain.
func TestNATSURL_ExplicitOverride(t *testing.T) {
	home := withFakeHome(t)
	writeFile(t, filepath.Join(home, ".sesh", "hub.nats.url"), "nats://from-file:4222\n")
	t.Setenv("NATS_URL", "nats://from-env:4222")

	want := "nats://from-flag:4222"
	got := ReadNATSURL(want)
	if got != want {
		t.Fatalf("ReadNATSURL: explicit override should win, want %q, got %q", want, got)
	}
}

// contains is a tiny substring helper to avoid pulling in strings just
// for one call — keeps the test file's imports tight.
func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
