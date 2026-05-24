# Changelog

All notable changes to `synadia-agent-shim` are documented here. The
format is [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Engine-aware send dispatch. The shim now detects whether it is
  running under tmux, cmux (orch#207), or zmx (orch#210), and routes
  the inbound-prompt delivery + interrupt verbs to the engine-native
  CLI:
  - tmux → `tmux send-keys -l` + `Enter` (existing behaviour).
  - cmux → `cmux send --surface <ref> -- <text>\n`.
  - zmx → `zmx send <session> <text>\r`.
- `--locator TYPE:VALUE` CLI flag. Examples:
  `--locator tmux:%37`, `--locator cmux:surface:30`,
  `--locator zmx:engineer-a`. When omitted, the shim autodetects from
  `$CMUX_SURFACE_ID`, `$ZMX_SESSION`, or `$TMUX_PANE`.
- `$SRV.INFO.agents` metadata now carries `engine` and `locator`
  fields alongside the back-compat `pane_id`.
- Heartbeat payload (`agents.hb.*`) now carries `engine` and
  `locator` fields (omitempty; back-compat with pre-PR subscribers).

### Deprecated

- `--pane VALUE` is deprecated in favour of `--locator tmux:VALUE`. It
  continues to work this release with a stderr warning on startup and
  will be **removed in the next shim release**.

### Notes

- The pane-watchdog (orch#167 backstop) currently only runs under the
  tmux engine; cmux and zmx manage surface lifetime themselves and
  don't expose a `tmux display-message`-equivalent today.
- The `pane_id` metadata field stays for back-compat with orch's
  current registry reader. Once orch adopts the typed `locator`
  field, `pane_id` will be retired in a future shim release (tracked
  separately).
