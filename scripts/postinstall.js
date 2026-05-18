#!/usr/bin/env node
// postinstall — fetch the platform-specific synadia-agent-shim binary
// from the matching GitHub Release and place it at vendor/synadia-agent-shim.
//
// Resolution:
//   1. version = package.json.version → tag = `v${version}`.
//   2. asset name = `synadia-agent-shim_${version}_${os}_${arch}.tar.gz`.
//   3. download from https://github.com/danmestas/synadia-agent-shim/releases/download/<tag>/<asset>.
//   4. extract the binary into vendor/synadia-agent-shim and chmod +x.
//
// Skipped when:
//   - $SYNADIA_AGENT_SHIM_SKIP_DOWNLOAD=1 (CI / offline / vendored installs).
//   - The vendor binary already exists and the package version matches
//     (re-runs of `npm install` shouldn't redownload).

const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const zlib = require("node:zlib");
const { pipeline } = require("node:stream/promises");
const { spawnSync } = require("node:child_process");

const ROOT = path.resolve(__dirname, "..");
const VENDOR_DIR = path.join(ROOT, "vendor");
const VENDOR_BIN = path.join(VENDOR_DIR, "synadia-agent-shim");

const log = (m) => process.stderr.write(`synadia-agent-shim postinstall: ${m}\n`);
const die = (m) => { log(`ERROR ${m}`); process.exit(1); };

async function main() {
    if (process.env.SYNADIA_AGENT_SHIM_SKIP_DOWNLOAD === "1") {
        log("SYNADIA_AGENT_SHIM_SKIP_DOWNLOAD=1, skipping download");
        return;
    }

    const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));
    const version = pkg.version;
    const tag = `v${version}`;

    const goos = mapOS(process.platform);
    const goarch = mapArch(process.arch);
    if (!goos || !goarch) {
        log(`unsupported platform ${process.platform}/${process.arch} — skipping`);
        log("the wrapper scripts will fall back to a clear error at runtime");
        return;
    }

    const asset = `synadia-agent-shim_${version}_${goos}_${goarch}.tar.gz`;
    const url = `https://github.com/danmestas/synadia-agent-shim/releases/download/${tag}/${asset}`;

    fs.mkdirSync(VENDOR_DIR, { recursive: true });

    log(`fetching ${url}`);
    try {
        await download(url, asset);
    } catch (err) {
        log(`download failed: ${err.message}`);
        log("the wrapper scripts will fall back to a clear error at runtime");
        return;
    }

    log(`extracted to ${VENDOR_BIN}`);
}

function mapOS(p) {
    if (p === "darwin") return "darwin";
    if (p === "linux") return "linux";
    return null;
}

function mapArch(a) {
    if (a === "x64") return "amd64";
    if (a === "arm64") return "arm64";
    return null;
}

async function download(url, asset) {
    // node's fetch + tar — but to avoid a dependency on `tar`, shell out
    // to curl + tar which both ship on macOS and most Linux. Fall back to
    // a clear error if curl is missing.
    if (!hasCmd("curl") || !hasCmd("tar")) {
        throw new Error("curl + tar required on PATH");
    }
    const tmp = path.join(VENDOR_DIR, asset);
    let r = spawnSync("curl", ["-fsSL", "-o", tmp, url], { stdio: "inherit" });
    if (r.status !== 0) throw new Error(`curl exited ${r.status}`);

    r = spawnSync("tar", ["-xzf", tmp, "-C", VENDOR_DIR, "synadia-agent-shim"], { stdio: "inherit" });
    if (r.status !== 0) throw new Error(`tar exited ${r.status}`);

    fs.unlinkSync(tmp);
    fs.chmodSync(VENDOR_BIN, 0o755);
}

function hasCmd(cmd) {
    const r = spawnSync(process.platform === "win32" ? "where" : "which", [cmd], { stdio: "ignore" });
    return r.status === 0;
}

main().catch((err) => die(err.message));
