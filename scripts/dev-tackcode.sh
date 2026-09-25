#!/usr/bin/env bash
# Dev launcher for TackCode: starts the ZCode desktop dev environment with
# the tack-agent bridge as the agent backend (instead of apps/zcode-cli).
#
# Usage: scripts/dev-tackcode.sh [workspace-dir-to-open]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_DIR="$REPO_ROOT/.toolchains/node-v24.14.0-darwin-arm64/bin"

# Toolchain / runtime env (all caches stay inside the repo or /tmp so the
# sandboxed dev shell can write them).
export PATH="$NODE_DIR:$PATH"
export HOME="${TACKCODE_HOME:-/tmp/tackcode-home}"
mkdir -p "$HOME"
export COREPACK_HOME="$REPO_ROOT/.toolchains/corepack-home"
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
export npm_config_cache="$REPO_ROOT/.toolchains/cache/pnpm"

# ZCode data (settings, sessions index DB, logs) lives here in dev.
export ZCODE_DATA_BASE_DIR="$HOME/zcode-data"
# pi-rs state (sessions, auth.json, settings) lives here in dev.
export PI_RS_AGENT_DIR="${PI_RS_AGENT_DIR:-$HOME/tack-agent}"
mkdir -p "$PI_RS_AGENT_DIR"

# The pi-rs binary the bridge should spawn.
export TACK_AGENT_PI_BINARY="${TACK_AGENT_PI_BINARY:-pi-rs}"

# Agent backend: tackAgentDefaults (desktop main) resolves the bundled bridge
# automatically; these remain available as explicit overrides when debugging.
# export ZCODE_AGENT_SERVER_COMMAND=...
# export ZCODE_AGENT_SERVER_ARGS_JSON=...
# export ZCODE_AGENT_SERVER_STORAGE_PREPARATION_ENTRY=...
export TACK_AGENT_STORAGE_STARTUP=1
# Bridge diagnostics (stdout is the protocol channel, stderr may be swallowed).
export TACK_AGENT_LOG_FILE="${TACK_AGENT_LOG_FILE:-$HOME/tack-agent.log}"
# Host startup checkpoints (temporary instrumentation; see hostDatabaseStartup).
export TACKCODE_DEBUG_HOST="${TACKCODE_DEBUG_HOST:-1}"

export ZCODE_ENV="${ZCODE_ENV:-production}"

# The harness sandboxes child processes; Chromium's own sandbox cannot
# initialize underneath it, and Chromium caches under ~/Library are
# read-only here. Both are dev-only accommodations.
export ELECTRON_DISABLE_SANDBOX=1
export ELECTRON_ENABLE_LOGGING="${ELECTRON_ENABLE_LOGGING:-}"
# Official runtime overrides (desktopRuntimeEnv.ts): keep Electron's
# home/userData/sessionData inside the writable dev sandbox as well.
export ZCODE_DESKTOP_HOME_DIR="$HOME"
export ZCODE_DESKTOP_USER_DATA_DIR="$HOME/electron-user-data"
export ZCODE_DESKTOP_SESSION_DATA_DIR="$HOME/electron-user-data/session"

cd "$REPO_ROOT"
corepack pnpm --filter @zcode/desktop pre-dev
exec corepack pnpm --filter @zcode/desktop dev:runtime
