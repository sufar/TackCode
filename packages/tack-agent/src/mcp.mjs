// MCP support for the tack-agent bridge:
// - translate ZCode Protocol `mcpServers` (createSession payload / mcp/list
//   params) into pi-rs `set_mcp_servers` shape (Claude Code mcp.json entries);
// - translate pi-rs `get_mcp_status` results into ZCode status snapshots;
// - own the shared probe pi-rs processes used by `mcp/list` (workspace-level
//   status checks have no session of their own).
import fs from "node:fs";
import path from "node:path";

import { spawnPiRpc } from "./piRpc.mjs";

function entriesToObject(entries) {
  // Protocol carries env/headers as [{name, value}] arrays; pi-rs wants maps.
  const out = {};
  if (!Array.isArray(entries)) return out;
  for (const entry of entries) {
    if (entry && typeof entry.name === "string" && typeof entry.value === "string") {
      out[entry.name] = entry.value;
    }
  }
  return out;
}

function translateOAuth(oauth) {
  if (!oauth || typeof oauth !== "object") return undefined;
  const out = {};
  if (typeof oauth.clientId === "string" && oauth.clientId) out.clientId = oauth.clientId;
  if (typeof oauth.scope === "string" && oauth.scope.trim()) {
    out.scopes = oauth.scope.split(/\s+/u).filter(Boolean);
  }
  // pi-rs MCP OAuth is dynamic-registration + browser flow: clientSecret /
  // clientName / redirectPath have no pi-side field, and M2M
  // client_credentials is not supported — `true` = authorize on 401.
  return Object.keys(out).length > 0 ? out : true;
}

/**
 * ZCodeProtocolMcpServer[] → pi-rs servers map (mcp.json entry shapes).
 * Returns null when `servers` is not an array (host did not override the
 * agent's own config). timeoutMs / isolation / protocolVersion have no
 * pi-rs equivalent and are dropped.
 */
export function protocolMcpServersToPi(servers) {
  if (!Array.isArray(servers)) return null;
  const out = {};
  for (const server of servers) {
    if (!server || typeof server.name !== "string" || !server.name) continue;
    if (typeof server.command === "string" && server.command) {
      out[server.name] = {
        command: server.command,
        args: Array.isArray(server.args) ? server.args.filter((a) => typeof a === "string") : [],
        env: entriesToObject(server.env),
      };
    } else if (typeof server.url === "string" && server.url) {
      const entry = {
        type: server.type === "sse" ? "sse" : "http",
        url: server.url,
        headers: entriesToObject(server.headers),
      };
      const oauth = translateOAuth(server.oauth);
      if (oauth !== undefined) entry.oauth = oauth;
      out[server.name] = entry;
    }
  }
  return out;
}

/** pi-rs get_mcp_status servers[] → ZCode mcp/list statuses record. */
export function piStatusToZCodeStatuses(servers) {
  const statuses = {};
  const updatedAt = new Date().toISOString();
  for (const entry of Array.isArray(servers) ? servers : []) {
    if (!entry || typeof entry.name !== "string" || !entry.name) continue;
    const failed = entry.status === "failed";
    statuses[entry.name] = {
      status: failed ? "failed" : entry.status === "connected" ? "connected" : "disconnected",
      transport: ["stdio", "http", "sse"].includes(entry.transport) ? entry.transport : "stdio",
      toolCount: Number.isInteger(entry.toolCount) && entry.toolCount >= 0 ? entry.toolCount : 0,
      updatedAt,
      ...(failed
        ? {
            error: typeof entry.error === "string" && entry.error ? entry.error : "connection failed",
            failureKind: "connection_failed",
          }
        : {}),
    };
  }
  return statuses;
}

/**
 * Write-through 持久化 host 各会话的 MCP 配置（agentDir/tack-mcp-servers.json）。
 * ZCode 协议口径：MCP 是 runtime 启动期配置，随 createSession 一次性进入 record；
 * bridge 重启/桌面 App 重启后的冷恢复（resume）按留档重放——host 不会在 v4
 * resubscribe 时重发。文件损坏/读取失败一律按空档处理（会话仍有 pi 文件配置兜底）。
 */
const SESSION_MCP_STORE_LIMIT = 512;

export class SessionMcpStore {
  #file;
  #map;

  constructor(file) {
    this.#file = file;
    this.#map = new Map();
    try {
      const raw = JSON.parse(fs.readFileSync(file, "utf8"));
      if (raw && typeof raw === "object") {
        for (const [sessionId, servers] of Object.entries(raw)) {
          if (typeof sessionId === "string" && servers && typeof servers === "object") {
            this.#map.set(sessionId, servers);
          }
        }
      }
    } catch {
      /* 缺文件/损坏 → 空档 */
    }
  }

  get(sessionId) {
    return this.#map.get(sessionId);
  }

  set(sessionId, servers) {
    // Map 保持插入序；超限淘汰最老条目（正常路径 deleteSession 已清理）。
    this.#map.delete(sessionId);
    this.#map.set(sessionId, servers);
    while (this.#map.size > SESSION_MCP_STORE_LIMIT) {
      const oldest = this.#map.keys().next().value;
      this.#map.delete(oldest);
    }
    this.#persist();
  }

  rekey(oldSessionId, newSessionId) {
    const servers = this.#map.get(oldSessionId);
    if (servers === undefined) return;
    this.set(newSessionId, servers);
  }

  delete(sessionId) {
    if (this.#map.delete(sessionId)) this.#persist();
  }

  #persist() {
    try {
      fs.mkdirSync(path.dirname(this.#file), { recursive: true });
      const tmp = `${this.#file}.tmp`;
      fs.writeFileSync(tmp, JSON.stringify(Object.fromEntries(this.#map)));
      fs.renameSync(tmp, this.#file);
    } catch {
      /* 留档失败不致命：内存态仍有效，仅本次 bridge 生命周期内可重放 */
    }
  }
}

/**
 * Lazily-spawned probe pi-rs rpc processes (one per workspace path) serving
 * `mcp/list`: the settings page asks the agent to really connect and report
 * per-server status. Probes are long-lived so repeated refreshes and OAuth
 * polling reuse pi-rs's connection pool instead of re-spawning servers.
 */
export class McpProbePool {
  #probes = new Map(); // workspacePath -> { rpc, queue }
  #piBinary;
  #env;
  #log;

  constructor({ piBinary, env, log }) {
    this.#piBinary = piBinary;
    this.#env = env;
    this.#log = log ?? (() => {});
  }

  /**
   * Run one mcp/list round against the workspace probe. `servers` = pi-shape
   * map (explicit host override; null = keep whatever the probe already has,
   * i.e. file-configured servers only). Calls are serialized per probe so a
   * concurrent set_mcp_servers/get_mcp_status pair cannot interleave.
   */
  status({ workspacePath, servers, connect }) {
    const probe = this.#probe(workspacePath);
    const run = probe.queue
      .catch(() => {})
      .then(() => this.#query(probe.rpc, { servers, connect }));
    probe.queue = run;
    return run;
  }

  async #query(rpc, { servers, connect }) {
    if (servers) {
      await rpc.request("set_mcp_servers", { servers });
    }
    const result = await rpc.request("get_mcp_status", { connect }, { timeoutMs: 150_000 });
    return piStatusToZCodeStatuses(result?.servers);
  }

  #probe(workspacePath) {
    const key = workspacePath || process.cwd();
    const existing = this.#probes.get(key);
    if (existing && !existing.rpc.closed) return existing;
    const rpc = spawnPiRpc({
      piBinary: this.#piBinary,
      cwd: key,
      env: this.#env,
      onEvent: () => {},
      onExit: () => {
        if (this.#probes.get(key)?.rpc === rpc) this.#probes.delete(key);
      },
      onStderr: (line) => this.#log(`[pi-rs mcp-probe] ${line}`),
    });
    const probe = { rpc, queue: Promise.resolve() };
    this.#probes.set(key, probe);
    return probe;
  }

  dispose() {
    for (const probe of this.#probes.values()) probe.rpc.kill();
    this.#probes.clear();
  }
}
