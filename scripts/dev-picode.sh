#!/usr/bin/env bash
# Dev launcher for pi-rs-code: starts the ZCode desktop dev environment with
# the pi-agent bridge as the agent backend (instead of apps/zcode-cli).
#
# Usage: scripts/dev-picode.sh [workspace-dir-to-open]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_DIR="$REPO_ROOT/.toolchains/node-v24.14.0-darwin-arm64/bin"

# Toolchain / runtime env (all caches stay inside the repo or /tmp so the
# sandboxed dev shell can write them).
export PATH="$NODE_DIR:$PATH"
export HOME="${PICODE_HOME:-/tmp/picode-home}"
mkdir -p "$HOME"
export COREPACK_HOME="$REPO_ROOT/.toolchains/corepack-home"
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
export npm_config_cache="$REPO_ROOT/.toolchains/cache/pnpm"

# ZCode data (settings, sessions index DB, logs) lives here in dev.
export ZCODE_DATA_BASE_DIR="$HOME/zcode-data"
# pi-rs state (sessions, auth.json, settings) lives here in dev.
export PI_RS_AGENT_DIR="${PI_RS_AGENT_DIR:-$HOME/pi-agent}"
mkdir -p "$PI_RS_AGENT_DIR"

# The pi-rs binary the bridge should spawn.
export PI_AGENT_PI_BINARY="${PI_AGENT_PI_BINARY:-pi-rs}"

# Point the host's agent resolution at the bridge (official override point).
export ZCODE_AGENT_SERVER_COMMAND="$NODE_DIR/node"
export ZCODE_AGENT_SERVER_ARGS_JSON="[\"$REPO_ROOT/packages/pi-agent/bin/pi-agent.mjs\"]"
# The bridge also implements the agent-owned storage startup protocol; declare
# its preparation entrypoint so the desktop startup gate accepts it.
export ZCODE_AGENT_SERVER_STORAGE_PREPARATION_ENTRY="$REPO_ROOT/packages/pi-agent/bin/pi-agent.mjs"
export PI_AGENT_STORAGE_STARTUP=1

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
