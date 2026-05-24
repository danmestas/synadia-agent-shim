package shim

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// TestDetectEngine covers the engine-detection precedence table from
// engine.go's header comment. Each row pins one env-var permutation to
// the expected Engine + Value to lock the precedence rule in place —
// if a future commit reorders the lookup chain, this table is what
// catches it.
func TestDetectEngine(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		wantEng  Engine
		wantVal  string
		wantOK   bool
		valEmpty bool // for the tmux-without-pane edge
	}{
		{
			name:    "cmux wins when CMUX_SURFACE_ID present",
			env:     map[string]string{"CMUX_SURFACE_ID": "surface:30", "TMUX": "/tmp/sock,1,0", "TMUX_PANE": "%37"},
			wantEng: EngineCmux,
			wantVal: "surface:30",
			wantOK:  true,
		},
		{
			name:    "zmx wins over tmux when ZMX_SESSION present",
			env:     map[string]string{"ZMX_SESSION": "engineer-a", "TMUX": "/tmp/sock,1,0"},
			wantEng: EngineZmx,
			wantVal: "engineer-a",
			wantOK:  true,
		},
		{
			name:    "tmux pane id when only TMUX_PANE set",
			env:     map[string]string{"TMUX_PANE": "%42"},
			wantEng: EngineTmux,
			wantVal: "%42",
			wantOK:  true,
		},
		{
			name:     "tmux engine with no pane when only TMUX set",
			env:      map[string]string{"TMUX": "/tmp/sock,1,0"},
			wantEng:  EngineTmux,
			wantVal:  "",
			wantOK:   true,
			valEmpty: true,
		},
		{
			name:    "no engine markers → not detected",
			env:     map[string]string{},
			wantEng: EngineUnknown,
			wantVal: "",
			wantOK:  false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			getenv := func(k string) string { return c.env[k] }
			loc, ok := DetectEngine(getenv)
			if ok != c.wantOK {
				t.Fatalf("DetectEngine ok: got %v want %v", ok, c.wantOK)
			}
			if loc.Engine != c.wantEng {
				t.Errorf("Engine: got %q want %q", loc.Engine, c.wantEng)
			}
			if loc.Value != c.wantVal {
				t.Errorf("Value: got %q want %q", loc.Value, c.wantVal)
			}
		})
	}
}

// TestDetectEngine_NilGetenvUsesOSGetenv asserts the documented default
// fallback: passing nil for getenv uses os.Getenv. We don't manipulate
// the OS environment in this test (would be racy with the other
// parallel tests) — we only confirm the nil path doesn't panic.
func TestDetectEngine_NilGetenvUsesOSGetenv(t *testing.T) {
	// Just exercise the code path. Result depends on the test runner's
	// env, which we don't control — but the call itself must not panic.
	_, _ = DetectEngine(nil)
}

// TestParseLocator pins the wire format of the --locator flag plus the
// legacy --pane back-compat path.
func TestParseLocator(t *testing.T) {
	cases := []struct {
		in      string
		want    Locator
		wantErr bool
	}{
		{in: "tmux:%37", want: Locator{Engine: EngineTmux, Value: "%37"}},
		{in: "cmux:surface:30", want: Locator{Engine: EngineCmux, Value: "surface:30"}},
		{in: "cmux:EE919252-94D9-4F53-AC92-C5122AC2C579", want: Locator{Engine: EngineCmux, Value: "EE919252-94D9-4F53-AC92-C5122AC2C579"}},
		{in: "zmx:engineer-a", want: Locator{Engine: EngineZmx, Value: "engineer-a"}},
		{in: "  zmx:engineer-a  ", want: Locator{Engine: EngineZmx, Value: "engineer-a"}}, // trims whitespace

		// Legacy bare-pane back-compat.
		{in: "%64", want: Locator{Engine: EngineTmux, Value: "%64"}},

		// Failures.
		{in: "", wantErr: true},
		{in: ":foo", wantErr: true},
		{in: "tmux:", wantErr: true},
		{in: "foo:bar", wantErr: true}, // unknown engine
		{in: "tmux", wantErr: true},    // no colon, not a pane id
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := ParseLocator(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("ParseLocator(%q): want error, got %v", c.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseLocator(%q): unexpected error: %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("ParseLocator(%q): got %+v want %+v", c.in, got, c.want)
			}
		})
	}
}

// TestLocator_IsValid pins the "is this usable for sending?" predicate.
func TestLocator_IsValid(t *testing.T) {
	cases := []struct {
		loc  Locator
		want bool
	}{
		{loc: Locator{Engine: EngineTmux, Value: "%1"}, want: true},
		{loc: Locator{Engine: EngineCmux, Value: "surface:30"}, want: true},
		{loc: Locator{Engine: EngineZmx, Value: "dev"}, want: true},
		{loc: Locator{Engine: EngineTmux, Value: ""}, want: false},
		{loc: Locator{Engine: EngineUnknown, Value: "x"}, want: false},
		{loc: Locator{}, want: false},
	}
	for _, c := range cases {
		if got := c.loc.IsValid(); got != c.want {
			t.Errorf("Locator{%s, %q}.IsValid(): got %v want %v", c.loc.Engine, c.loc.Value, got, c.want)
		}
	}
}

// recordedCommand captures one runCmd invocation for the recorder.
type recordedCommand struct {
	name string
	args []string
}

// commandRecorder is a testing helper: implements commandRunner, stashes
// every invocation it sees, and can be configured to return a specific
// error from the Nth call (for error-path tests).
type commandRecorder struct {
	mu      sync.Mutex
	calls   []recordedCommand
	errAt   int // 1-based call index that returns errStub; 0 = never error
	errStub error
}

func (r *commandRecorder) run(_ context.Context, name string, args ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recordedCommand{name: name, args: append([]string(nil), args...)})
	if r.errAt != 0 && len(r.calls) == r.errAt {
		return r.errStub
	}
	return nil
}

func (r *commandRecorder) snapshot() []recordedCommand {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedCommand, len(r.calls))
	copy(out, r.calls)
	return out
}

// TestLocatorSend_PerEngine confirms each engine maps to the correct
// command + args. This is the heart of the engine-aware-send PR — if
// any of these change unexpectedly, an upstream operator sees their
// prompts vanish or land in the wrong surface.
func TestLocatorSend_PerEngine(t *testing.T) {
	cases := []struct {
		name string
		loc  Locator
		text string
		want []recordedCommand
	}{
		{
			name: "tmux: two-step literal text + Enter",
			loc:  Locator{Engine: EngineTmux, Value: "%37"},
			text: "hello world",
			want: []recordedCommand{
				{name: "tmux", args: []string{"send-keys", "-l", "-t", "%37", "hello world"}},
				{name: "tmux", args: []string{"send-keys", "-t", "%37", "Enter"}},
			},
		},
		{
			name: "cmux: single invocation with -- and \\n",
			loc:  Locator{Engine: EngineCmux, Value: "surface:30"},
			text: "hello world",
			want: []recordedCommand{
				{name: "cmux", args: []string{"send", "--surface", "surface:30", "--", "hello world\n"}},
			},
		},
		{
			name: "zmx: single invocation with trailing \\r",
			loc:  Locator{Engine: EngineZmx, Value: "engineer-a"},
			text: "hello world",
			want: []recordedCommand{
				{name: "zmx", args: []string{"send", "engineer-a", "hello world\r"}},
			},
		},
		// Prompt body starting with "-" is the classic flag-injection
		// hazard; cmux's "--" sentinel must shield us.
		{
			name: "cmux: prompt starting with - is shielded by --",
			loc:  Locator{Engine: EngineCmux, Value: "surface:30"},
			text: "--rm -rf /tmp",
			want: []recordedCommand{
				{name: "cmux", args: []string{"send", "--surface", "surface:30", "--", "--rm -rf /tmp\n"}},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &commandRecorder{}
			if err := locatorSend(context.Background(), c.loc, c.text, rec.run); err != nil {
				t.Fatalf("locatorSend: %v", err)
			}
			got := rec.snapshot()
			if len(got) != len(c.want) {
				t.Fatalf("calls: got %d want %d (%+v)", len(got), len(c.want), got)
			}
			for i := range got {
				if got[i].name != c.want[i].name {
					t.Errorf("call[%d].name: got %q want %q", i, got[i].name, c.want[i].name)
				}
				if strings.Join(got[i].args, "|") != strings.Join(c.want[i].args, "|") {
					t.Errorf("call[%d].args: got %v want %v", i, got[i].args, c.want[i].args)
				}
			}
		})
	}
}

// TestLocatorSend_InvalidLocatorRejected confirms we fail loud on an
// uninitialised Locator rather than no-op-and-confuse-the-operator.
func TestLocatorSend_InvalidLocatorRejected(t *testing.T) {
	rec := &commandRecorder{}
	err := locatorSend(context.Background(), Locator{}, "hi", rec.run)
	if err == nil {
		t.Fatal("locatorSend with zero Locator: want error, got nil")
	}
	if len(rec.snapshot()) != 0 {
		t.Errorf("invalid locator should not have invoked any command, got %v", rec.snapshot())
	}
}

// TestLocatorSend_TmuxFirstCallErrorAborts pins the documented tmux
// two-step semantics: if the literal text call fails, the Enter call
// MUST NOT be issued (otherwise we'd submit an empty line into the
// REPL on a broken pipe).
func TestLocatorSend_TmuxFirstCallErrorAborts(t *testing.T) {
	rec := &commandRecorder{errAt: 1, errStub: errors.New("send-keys failed")}
	err := locatorSend(context.Background(), Locator{Engine: EngineTmux, Value: "%37"}, "hi", rec.run)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if calls := rec.snapshot(); len(calls) != 1 {
		t.Fatalf("expected 1 call before abort, got %d (%+v)", len(calls), calls)
	}
}

// TestLocatorInterrupt_PerEngine pins each engine's interrupt verb.
func TestLocatorInterrupt_PerEngine(t *testing.T) {
	cases := []struct {
		name string
		loc  Locator
		want recordedCommand
	}{
		{
			name: "tmux: send-keys C-c",
			loc:  Locator{Engine: EngineTmux, Value: "%37"},
			want: recordedCommand{name: "tmux", args: []string{"send-keys", "-t", "%37", "C-c"}},
		},
		{
			name: "cmux: send-key ctrl+c",
			loc:  Locator{Engine: EngineCmux, Value: "surface:30"},
			want: recordedCommand{name: "cmux", args: []string{"send-key", "--surface", "surface:30", "ctrl+c"}},
		},
		{
			name: "zmx: send raw 0x03",
			loc:  Locator{Engine: EngineZmx, Value: "engineer-a"},
			want: recordedCommand{name: "zmx", args: []string{"send", "engineer-a", "\x03"}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &commandRecorder{}
			if err := locatorInterrupt(context.Background(), c.loc, rec.run); err != nil {
				t.Fatalf("locatorInterrupt: %v", err)
			}
			got := rec.snapshot()
			if len(got) != 1 {
				t.Fatalf("want 1 call, got %d (%+v)", len(got), got)
			}
			if got[0].name != c.want.name {
				t.Errorf("name: got %q want %q", got[0].name, c.want.name)
			}
			if strings.Join(got[0].args, "|") != strings.Join(c.want.args, "|") {
				t.Errorf("args: got %v want %v", got[0].args, c.want.args)
			}
		})
	}
}

// TestLocatorInterrupt_InvalidLocator confirms the invalid-locator
// guard mirrors locatorSend.
func TestLocatorInterrupt_InvalidLocator(t *testing.T) {
	rec := &commandRecorder{}
	if err := locatorInterrupt(context.Background(), Locator{}, rec.run); err == nil {
		t.Fatal("want error, got nil")
	}
	if len(rec.snapshot()) != 0 {
		t.Error("invalid locator must not invoke commandRunner")
	}
}

// TestLocator_String pins the wire form used by the heartbeat metadata
// `locator` field and the --locator flag round-trip.
func TestLocator_String(t *testing.T) {
	cases := []struct {
		loc  Locator
		want string
	}{
		{loc: Locator{Engine: EngineTmux, Value: "%37"}, want: "tmux:%37"},
		{loc: Locator{Engine: EngineCmux, Value: "surface:30"}, want: "cmux:surface:30"},
		{loc: Locator{Engine: EngineZmx, Value: "engineer-a"}, want: "zmx:engineer-a"},
	}
	for _, c := range cases {
		if got := c.loc.String(); got != c.want {
			t.Errorf("Locator{%s,%q}.String(): got %q want %q", c.loc.Engine, c.loc.Value, got, c.want)
		}
	}
}

// TestParseLocator_RoundTrip_String asserts ParseLocator and Locator.String
// are inverses for the canonical forms. (Bare-pane back-compat is excluded
// — it expands to "tmux:%N" on the way back, which is the desired
// canonicalisation, not a round-trip violation.)
func TestParseLocator_RoundTrip_String(t *testing.T) {
	for _, s := range []string{"tmux:%37", "cmux:surface:30", "cmux:abc-uuid", "zmx:engineer-a"} {
		got, err := ParseLocator(s)
		if err != nil {
			t.Fatalf("ParseLocator(%q): %v", s, err)
		}
		if got.String() != s {
			t.Errorf("round-trip: ParseLocator(%q).String() = %q, want %q", s, got.String(), s)
		}
	}
}
