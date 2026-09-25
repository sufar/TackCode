// TackCode: default the host's agent to the bundled pi-agent bridge when no
// explicit ZCODE_AGENT_SERVER_COMMAND override is present. Upstream resolves
// to its own zcode-cli agent bundle; the fork ships the pi-rs bridge instead.
import { existsSync } from "node:fs";
import { join } from "node:path";
import { app } from "electron";

function resolveBundledPiAgentEntry(): string | null {
  const candidates = app.isPackaged
    ? [join(process.resourcesPath, "pi-agent", "bin", "pi-agent.mjs")]
    : [join(import.meta.dirname, "../../../pi-agent/bin/pi-agent.mjs")];
  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }
  return null;
}

export function applyPiAgentDefaults(): void {
  if (process.env.ZCODE_AGENT_SERVER_COMMAND?.trim()) return;
  const entry = resolveBundledPiAgentEntry();
  if (!entry) return;
  // The Electron binary itself is the Node runtime (ELECTRON_RUN_AS_NODE is
  // injected by the services-layer env-override handling).
  process.env.ZCODE_AGENT_SERVER_COMMAND = process.execPath;
  process.env.ZCODE_AGENT_SERVER_ARGS_JSON = JSON.stringify([entry]);
  process.env.ZCODE_AGENT_SERVER_STORAGE_PREPARATION_ENTRY = entry;
  process.env.PI_AGENT_STORAGE_STARTUP = "1";
}
