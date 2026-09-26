/* oxlint-disable max-lines -- bridge dispatch co-locates the whole host-facing method surface; splitting would scatter protocol handlers from their shared subscription state. */
// WorkspaceBridge: the ZCode Protocol endpoint. One bridge process serves one
// workspace (the host spawns one agent process per workspaceKey); it owns the
// sessions-index / workspace-config topics, routes conversation topics to
// SessionActors, and brokers provider credentials from host to pi-rs.
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  deleteSessionFile,
  findSessionFile,
  listModels,
  readAuthJson,
  readSessionFileMeta,
  scanSessionFiles,
  writeAuthApiKey,
} from "./piHome.mjs";
import { previewText } from "./projection.mjs";
import { SessionActor } from "./sessionActor.mjs";
import { encodeTopicWireFrames } from "./wire.mjs";
import { AttachmentError, AttachmentStore } from "./attachments.mjs";
import { agentDir } from "./piHome.mjs";
import { loadSkills } from "./skills.mjs";
import { McpProbePool, SessionMcpStore, loadFileMcpServers, protocolMcpServersToPi } from "./mcp.mjs";
import { grantWorkspaceHookTrustFlow, revokeWorkspaceHookTrustFlow } from "./hooks.mjs";

const AGENT_VERSION = JSON.parse(
  fs.readFileSync(new URL("../package.json", import.meta.url), "utf8"),
).version;

const piProviders = JSON.parse(
  fs.readFileSync(    path.join(path.dirname(fileURLToPath(import.meta.url)), "piProviders.generated.json"),
    "utf8",
  ),
);

const ERROR_METHOD_NOT_FOUND = -32601;
const ERROR_INVALID_PARAMS = -32602;
const ERROR_INTERNAL = -32603;
const SESSION_INDEX_META_LIMIT = 100;

/** ZCode template/provider ids that differ from pi-rs's builtin provider ids. */
const PI_PROVIDER_ALIASES = {
  "moonshot-kimi": "moonshotai",
  "qwen-alibaba-model-studio-cn": "qwen-token-plan-cn",
  "qwen-alibaba-model-studio-intl": "qwen-token-plan",
  "xiaomi-mimo": "xiaomi",
  "opencode-go-chat": "opencode-go",
  "opencode-go-messages": "opencode-go",
  "opencode-go-responses": "opencode-go",
  "opencode-zen-chat": "opencode",
  "opencode-zen-messages": "opencode",
  "opencode-zen-responses": "opencode",
};

export class ProtocolError extends Error {
  constructor(code, message, data) {
    super(message);
    this.code = code;
    this.data = data;
  }
}

function methodNotFound(method) {
  return new ProtocolError(ERROR_METHOD_NOT_FOUND, `Method not found: ${method}`);
}

function flattenGenerateMessages(messages) {
  if (!Array.isArray(messages) || messages.length === 0) return "";
  const system = [];
  const turns = [];
  for (const message of messages) {
    if (message.role === "system") system.push(message.content ?? "");
    else if (message.role === "user") turns.push(`User: ${message.content ?? ""}`);
    else if (message.role === "assistant") turns.push(`Assistant: ${message.content ?? ""}`);
    else if (message.role === "tool") turns.push(`Tool (${message.toolName ?? "result"}): ${message.content ?? ""}`);
  }
  return [...(system.length ? [system.join("\n\n")] : []), turns.join("\n\n")]
    .filter((part) => part.length > 0)
    .join("\n\n");
}

function assistantContentText(content) {
  if (!Array.isArray(content)) return "";
  return content
    .filter((block) => block && block.type === "text" && typeof block.text === "string")
    .map((block) => block.text)
    .join("");
}

/** Drain a `pi-rs -p ... --mode json` child into {text, usage, finishReason, errorMessage}. */
function collectPrintModeResult(child, timeoutMs) {
  return new Promise((resolve, reject) => {
    let buffer = "";
    let stderr = "";
    let text = "";
    let usage = null;
    let finishReason;
    let errorMessage;
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error("generateText timed out"));
    }, timeoutMs);
    child.stdout.on("data", (chunk) => {
      buffer += chunk;
      let index;
      while ((index = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, index);
        buffer = buffer.slice(index + 1);
        if (!line.trim()) continue;
        try {
          const event = JSON.parse(line);
          if (event.type === "message_end" && event.message?.role === "assistant") {
            const next = assistantContentText(event.message.content);
            if (next) text = next;
            usage = event.message.usage ?? usage;
            finishReason = event.message.rawStopReason ?? event.message.stopReason ?? finishReason;
            if (event.message.errorMessage) errorMessage = event.message.errorMessage;
          }
        } catch {
          /* non-json line */
        }
      }
    });
    child.stderr.on("data", (chunk) => {
      stderr = `${stderr}${chunk}`.slice(-4096);
    });
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      if (code !== 0 && !text && !errorMessage) {
        reject(new Error(`pi-rs print mode exited ${code}: ${stderr.slice(-400)}`));
        return;
      }
      resolve({
        text,
        usage: usage
          ? {
              inputTokens: usage.input ?? 0,
              outputTokens: usage.output ?? 0,
              totalTokens: usage.totalTokens ?? 0,
              cacheReadTokens: usage.cacheRead ?? 0,
              cacheWriteTokens: usage.cacheWrite ?? 0,
            }
          : undefined,
        finishReason,
        errorMessage,
      });
    });
  });
}

export class WorkspaceBridge {
  #send;
  #log;
  #cwd;
  #env;
  #subscriptions = new Map(); // subscriptionId -> {topic, connectionId, kind}
  #subscriptionsByTopic = new Map(); // topic -> Set<subscriptionId>
  #topicStates = new Map(); // bridge-owned topic -> {logEpoch, seq}
  #actors = new Map(); // sessionId -> SessionActor
  #pendingActors = new Map(); // sessionId -> Promise<SessionActor>
  #recentAcks = new Map(); // `${sessionId}|${commandId}` -> ack
  #accountConfig = null;
  #modelCatalog = null; // parsed `pi-rs models` output
  #modelInfoCache = new Map(); // provider -> Map(modelId -> full Model json)
  #providerKeys = new Map(); // providerId -> {apiKey} (from host reverse channel)
  #authPending = new Map(); // providerId -> Promise
  #reversePending = new Map();
  #reverseNextId = 1;
  #disposed = false;
  #hostWorkspacePath = null;
  #generateTextProcesses = new Map(); // operationId -> one-shot print-mode child
  #attachments;
  #childParentMap = new Map(); // childSessionId -> parentSessionId
  #skillsCache = null; // {at, entries} (30s TTL)
  #sessionMcpServers; // SessionMcpStore（磁盘留档，bridge 重启后冷恢复仍能重放）
  #mcpProbePool = null; // lazy McpProbePool for mcp/list

  constructor({ send, cwd, env, log }) {
    this.#send = send;
    this.#cwd = cwd;
    this.#env = env;
    this.#log = log;
    this.#attachments = new AttachmentStore(agentDir(env));
    this.#sessionMcpServers = new SessionMcpStore(
      path.join(agentDir(env), "tack-mcp-servers.json"),
    );
  }

  get attachments() {
    return this.#attachments;
  }

  #skills() {
    if (this.#skillsCache && Date.now() - this.#skillsCache.at < 30_000) {
      return this.#skillsCache.entries;
    }
    const entries = loadSkills({ cwd: this.#cwd, agentDir: agentDir(this.#env) });
    this.#skillsCache = { at: Date.now(), entries };
    return entries;
  }

  #mcpProbes() {
    if (!this.#mcpProbePool) {
      this.#mcpProbePool = new McpProbePool({
        piBinary: this.piBinary,
        env: this.piSpawnEnv(),
        log: (line) => this.#log(line),
      });
    }
    return this.#mcpProbePool;
  }

  /**
   * 会话级生效 MCP 集 = zcode 配置文件解析（镜像 zcode-cli，每次新读，配置改动即生效）
   * + host createSession.mcpServers 显式下发（同名覆盖，对应 CLI merge 的 cli 优先级）。
   */
  #effectiveSessionMcpServers(hostServers) {
    const fileServers = loadFileMcpServers({
      homeDir: os.homedir(),
      workspacePath: this.#hostWorkspacePath ?? this.#cwd,
    });
    if (!hostServers) return fileServers;
    return { ...fileServers, ...hostServers };
  }

  registerChildSession(parentSessionId, childSessionId) {
    this.#childParentMap.set(childSessionId, parentSessionId);
  }

  #rebindActorToForkedSession(actor, newSessionId) {
    const oldSessionId = actor.sessionId;
    this.#actors.delete(oldSessionId);
    // fork 出来的新会话继承同一 pi-rs 进程（MCP 规格仍在进程内），但 bridge
    // 侧的留档要换键，否则 fork 后的冷恢复会丢 MCP 重放。
    this.#sessionMcpServers.rekey(oldSessionId, newSessionId);
    for (const subscriptionId of [...actor.subscriptionIds()]) {
      this.#dropSubscription(subscriptionId);
    }
    actor.detachAllSubscriptions();
    actor.sessionId = newSessionId;
    this.#actors.set(newSessionId, actor);
    actor
      .rebindAfterFork()
      .catch((error) => this.#log(`[tack-agent] fork rebuild failed: ${error.message}`));
    this.notifySessionChanged(actor);
  }

  #withAttachmentError(run) {
    try {
      return run();
    } catch (error) {
      if (error instanceof AttachmentError) {
        throw new ProtocolError(ERROR_INVALID_PARAMS, error.message);
      }
      throw error;
    }
  }

  get piBinary() {
    return this.#env.TACK_AGENT_PI_BINARY || "pi-rs";
  }

  /** Map a ZCode-side provider id to pi-rs's builtin provider id. */
  toPiProviderId(providerId) {
    if (!providerId) return providerId;
    const direct = PI_PROVIDER_ALIASES[providerId];
    if (direct) return direct;
    if (piProviders.some((p) => p.id === providerId)) return providerId;
    // Personal providers created from a template get "<templateId>-N" ids.
    const suffixMatch = /^(.*?)-\d+$/.exec(providerId);
    if (suffixMatch) {
      const base = suffixMatch[1];
      if (PI_PROVIDER_ALIASES[base]) return PI_PROVIDER_ALIASES[base];
      if (piProviders.some((p) => p.id === base)) return base;
    }
    // Custom personal providers: resolve via the account config's baseUrl.
    const api = this.#accountConfig?.providers?.[providerId]?.api;
    if (api?.baseUrl) {
      try {
        const host = new URL(api.baseUrl).host;
        const match = piProviders.find((p) => p.baseUrl && new URL(p.baseUrl).host === host);
        if (match) return match.id;
      } catch {
        /* malformed baseUrl */
      }
    }
    return providerId;
  }

  log(message) {
    this.#log(message);
  }

  /** hooks 发现/信任存储都以宿主 HOME 为根（与 services 层同 HOME 才能读到同一 store）。 */
  get homeDir() {
    return this.#env.HOME ?? this.#env.USERPROFILE;
  }

  // ── workspace / catalog helpers ─────────────────────────────────────────

  workspaceRef(workspaceId) {
    return {
      workspacePath: this.#hostWorkspacePath ?? this.#cwd,
      workspaceKey: workspaceId ?? this.#cwd,
    };
  }

  piSpawnEnv() {
    const env = { ...this.#env };
    for (const [providerId, key] of this.#providerKeys) {
      const def = piProviders.find((p) => p.id === providerId);
      const envName = def?.envKeys?.[def.envKeys.length - 1];
      if (envName && !env[envName]) env[envName] = key.apiKey;
    }
    return env;
  }

  #catalog() {
    if (this.#modelCatalog) return this.#modelCatalog;
    try {
      this.#modelCatalog = listModels(this.piBinary, this.piSpawnEnv());
    } catch (error) {
      this.#log(`[tack-agent] pi-rs models failed: ${error.message}`);
      this.#modelCatalog = [];
    }
    return this.#modelCatalog;
  }

  refreshCatalog() {
    this.#modelCatalog = null;
    this.#catalog();
  }

  findModelInfo(providerId, modelId) {
    return this.#modelInfoCache.get(providerId)?.get(modelId) ?? null;
  }

  registerModelInfo(providerId, models) {
    if (!Array.isArray(models)) return;
    let map = this.#modelInfoCache.get(providerId);
    if (!map) {
      map = new Map();
      this.#modelInfoCache.set(providerId, map);
    }
    for (const model of models) {
      if (model?.id) map.set(model.id, model);
    }
  }

  mapThinkingLevel(level) {
    return level || "off";
  }

  // ── provider credentials ────────────────────────────────────────────────

  async ensureProviderAuth(zcodeProviderId, { sessionId, workspace, modelSelection }) {
    const providerId = this.toPiProviderId(zcodeProviderId);
    if (!providerId) return;
    if (this.#providerKeys.has(providerId)) return;
    const auth = readAuthJson(this.#env);
    if (auth[providerId]) {
      this.#providerKeys.set(providerId, { apiKey: null, source: "pi-auth.json" });
      return;
    }
    const def = piProviders.find((p) => p.id === providerId);
    if (def?.envKeys?.some((name) => this.#env[name])) {
      this.#providerKeys.set(providerId, { apiKey: null, source: "env" });
      return;
    }
    if (this.#authPending.has(providerId)) return this.#authPending.get(providerId);
    const task = this.#fetchProviderAuth(providerId, { sessionId, workspace, modelSelection })
      .catch((error) => {
        this.#log(`[tack-agent] provider auth fetch failed for ${providerId}: ${error.message}`);
      })
      .finally(() => this.#authPending.delete(providerId));
    this.#authPending.set(providerId, task);
    return task;
  }

  async #fetchProviderAuth(providerId, { sessionId, workspace, modelSelection }) {
    const result = await this.reverseRequest(
      "interaction/requestProviderRuntimeHeaders",
      {
        requestId: randomUUID(),
        sessionId: sessionId ?? "unknown",
        workspace: this.workspaceRef(workspace?.workspaceKey),
        modelSelection: modelSelection ?? { providerId, modelId: "default" },
        providerId,
        reason: "model-request",
      },
      30_000,
    );
    if (result?.headersApplied === true && result.requestAuth?.apiKey) {
      const apiKey = result.requestAuth.apiKey;
      this.#providerKeys.set(providerId, { apiKey, source: "host" });
      try {
        writeAuthApiKey(providerId, apiKey, this.#env);
      } catch (error) {
        this.#log(`[tack-agent] writing auth.json for ${providerId} failed: ${error.message}`);
      }
      return;
    }
    this.#providerKeys.set(providerId, { apiKey: null, source: "unavailable" });
    if (result?.errorMessage) {
      this.#log(`[tack-agent] host declined headers for ${providerId}: ${result.errorMessage}`);
    }
  }

  // ── reverse (agent -> host) requests ────────────────────────────────────

  reverseRequest(method, params, timeoutMs = 30_000) {
    const id = `pibridge-${this.#reverseNextId++}`;
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.#reversePending.delete(id);
        reject(new Error(`reverse request ${method} timed out`));
      }, timeoutMs);
      this.#reversePending.set(id, { resolve, reject, timeout, method });
      this.#send({ id, method, params });
    });
  }

  handleResponse(message) {
    const id = String(message.id);
    const pending = this.#reversePending.get(id);
    if (!pending) return false;
    this.#reversePending.delete(id);
    clearTimeout(pending.timeout);
    if (message.error) {
      pending.reject(
        new ProtocolError(message.error.code ?? ERROR_INTERNAL, message.error.message ?? "error"),
      );
    } else {
      pending.resolve(message.result);
    }
    return true;
  }

  // ── frame plumbing ──────────────────────────────────────────────────────

  #topicState(topic) {
    let state = this.#topicStates.get(topic);
    if (!state) {
      state = { logEpoch: randomUUID(), seq: 0 };
      this.#topicStates.set(topic, state);
    }
    return state;
  }

  #registerSubscription(topic, connectionId, kind) {
    // Resubscribe replaces only the SAME (connectionId, topic) registration —
    // the host legitimately runs multiple concurrent consumers of one topic
    // (task-index syncer AND UI task service both subscribe sessions-index).
    const previous = this.#subscriptionsByTopic.get(topic);
    if (previous) {
      for (const subId of previous) {
        const sub = this.#subscriptions.get(subId);
        if (sub && sub.connectionId === connectionId) this.#subscriptions.delete(subId);
      }
    }
    const subscriptionId = randomUUID();
    this.#subscriptions.set(subscriptionId, { topic, connectionId, kind });
    let set = this.#subscriptionsByTopic.get(topic);
    if (!set) {
      set = new Set();
      this.#subscriptionsByTopic.set(topic, set);
    }
    set.add(subscriptionId);
    return subscriptionId;
  }

  #dropSubscription(subscriptionId) {
    const sub = this.#subscriptions.get(subscriptionId);
    if (!sub) return;
    this.#subscriptions.delete(subscriptionId);
    this.#subscriptionsByTopic.get(sub.topic)?.delete(subscriptionId);
  }

  #sendFrame(topic, subscriptionId, { fromSeq, toSeq, payload }, deliveryKind = "online") {
    const sub = this.#subscriptions.get(subscriptionId);
    const ordinal = sub ? (sub.ordinal = (sub.ordinal ?? 0) + 1) : 1;
    const logical = { topic, subscriptionId, fromSeq, toSeq, sentAt: Date.now(), payload };
    const wires = encodeTopicWireFrames(logical, {
      deliveryKind,
      topic,
      subscriptionId,
      logicalFrameOrdinal: ordinal,
    });
    for (const wire of wires) {
      this.#send({ method: "v4/conversation/frame", params: wire });
    }
  }

  sendConversationFrame(sessionId, subscriptionIds, { fromSeq, toSeq, payload }) {
    const topic = `conversation/${sessionId}`;
    for (const subscriptionId of subscriptionIds) {
      if (!this.#subscriptions.has(subscriptionId)) continue;
      this.#sendFrame(topic, subscriptionId, { fromSeq, toSeq, payload }, "online");
    }
  }

  #emitTopicDelta(topic, deltaOps) {
    const subs = this.#subscriptionsByTopic.get(topic);
    if (!subs || subs.size === 0) return;
    const state = this.#topicState(topic);
    const fromSeq = state.seq;
    state.seq += deltaOps.length;
    for (const subscriptionId of subs) {
      this.#sendFrame(
        topic,
        subscriptionId,
        { fromSeq, toSeq: state.seq, payload: { kind: "deltas", deltas: deltaOps } },
        "online",
      );
    }
  }

  #sendTopicSnapshot(topic, subscriptionId, snapshot, deliveryKind = "initial") {
    const state = this.#topicState(topic);
    this.#sendFrame(
      topic,
      subscriptionId,
      { fromSeq: 0, toSeq: state.seq, payload: { kind: "snapshot", snapshot } },
      deliveryKind,
    );
  }

  // ── sessions-index ──────────────────────────────────────────────────────

  #sessionSummaryFor(actor, workspaceId) {
    const parentSessionId = this.#childParentMap.get(actor.sessionId);
    return {
      sessionId: actor.sessionId,
      workspaceId,
      ...(parentSessionId ? { parentSessionId } : {}),
      title: actor.meta.title || previewText(actor.firstUserText) || "New session",
      titleSource: actor.meta.titleSource,
      phase: actor.control.phase,
      sessionEnded: false,
      hasBackgroundWork: false,
      lastActivityAt: actor.lastActivityAt,
      createdAt: actor.createdAt,
      ...(actor.lastAssistantPreview ? { lastAssistantPreview: actor.lastAssistantPreview } : {}),
    };
  }

  #sessionsIndexSnapshot(workspaceId) {
    const state = this.#topicState(`sessions-index/${workspaceId}`);
    const byDisk = scanSessionFiles(this.#cwd, this.#env).slice(0, SESSION_INDEX_META_LIMIT);
    const sessions = [];
    const seen = new Set();
    for (const actor of this.#actors.values()) {
      seen.add(actor.sessionId);
      sessions.push(this.#sessionSummaryFor(actor, workspaceId));
    }
    for (const entry of byDisk) {
      if (seen.has(entry.sessionId)) continue;
      const meta = readSessionFileMeta(entry.file);
      const parentSessionId = this.#childParentMap.get(entry.sessionId);
      sessions.push({
        sessionId: entry.sessionId,
        workspaceId,
        ...(parentSessionId ? { parentSessionId } : {}),
        title:
          meta.name ||
          previewText(meta.firstUserText) ||
          `Session ${new Date(entry.createdAt).toLocaleString()}`,
        titleSource: meta.name ? "custom" : "generated",
        phase: meta.messageCount > 0 ? "completedSuccess" : "draft",
        sessionEnded: false,
        hasBackgroundWork: false,
        lastActivityAt: meta.lastTimestamp ?? entry.lastActivityAt,
        createdAt: meta.createdAt ?? entry.createdAt,
        ...(meta.lastAssistantText
          ? { lastAssistantPreview: previewText(meta.lastAssistantText) }
          : {}),
      });
    }
    return {
      protocolVersion: 1,
      workspaceId,
      logEpoch: state.logEpoch,
      sessions,
    };
  }

  notifySessionChanged(actor) {
    const workspaceId = actor.workspace.workspaceKey;
    this.#emitTopicDelta(`sessions-index/${workspaceId}`, [
      { op: "session.upserted", session: this.#sessionSummaryFor(actor, workspaceId) },
    ]);
  }

  #notifySessionRemoved(workspaceId, sessionId) {
    this.#emitTopicDelta(`sessions-index/${workspaceId}`, [
      { op: "session.removed", sessionId },
    ]);
  }

  // ── workspace-config ────────────────────────────────────────────────────

  #workspaceConfigSnapshot(workspaceId) {
    const state = this.#topicState(`workspace-config/${workspaceId}`);
    const catalog = this.#catalog();
    const modelOptions = [];
    for (const provider of catalog) {
      if (!provider.hasAuth && !this.#accountConfig?.providers?.[provider.id]) continue;
      for (const model of provider.models) {
        modelOptions.push({
          value: `${provider.id}/${model.id}`,
          name: model.name,
          origin: "native",
          modelProviderId: provider.id,
          modelProviderName: provider.name,
        });
      }
    }
    const anyActor = this.#actors.values().next().value;
    const currentModel = anyActor
      ? `${anyActor.config.provider}/${anyActor.config.model}`
      : (modelOptions[0]?.value ?? "");
    const configOptions = [
      {
        id: "model",
        name: "Model",
        type: "select",
        currentValue: currentModel,
        options: modelOptions,
      },
      {
        id: "mode",
        name: "Mode",
        type: "select",
        currentValue: anyActor?.config.mode ?? "build",
        options: [
          { value: "build", name: "Build" },
          { value: "edit", name: "Edit" },
          { value: "plan", name: "Plan" },
          { value: "yolo", name: "Yolo" },
        ],
      },
      {
        id: "thought_level",
        name: "Thinking",
        type: "select",
        currentValue: anyActor?.config.thought ?? "off",
        options: (anyActor?.config.thoughtLevels?.length
          ? anyActor.config.thoughtLevels
          : ["off", "minimal", "low", "medium", "high"]
        ).map((level) => ({ value: level, name: level })),
      },
    ];
    return {
      protocolVersion: 1,
      workspaceId,
      logEpoch: state.logEpoch,
      config: {
        configOptions,
        slashCommands: this.#skills().map((skill) => ({
          name: skill.name,
          description: skill.description ?? "",
          source: "custom",
        })),
      },
    };
  }

  #publishWorkspaceConfig(workspaceId) {
    const topic = `workspace-config/${workspaceId}`;
    const subs = this.#subscriptionsByTopic.get(topic);
    if (!subs || subs.size === 0) return;
    const snapshot = this.#workspaceConfigSnapshot(workspaceId);
    this.#emitTopicDelta(topic, [{ op: "config.updated", config: snapshot.config }]);
  }

  // ── actors ──────────────────────────────────────────────────────────────

  async #ensureActor(sessionId, workspaceRef) {
    const existing = this.#actors.get(sessionId);
    if (existing) {
      // Late-arriving workspace facts (first seen via subscribe params) still
      // upgrade the actor's ref so legacy snapshots group correctly.
      if (workspaceRef?.workspaceIdentity && !existing.workspace.workspaceIdentity) {
        existing.workspace.workspaceIdentity = workspaceRef.workspaceIdentity;
      }
      if (workspaceRef?.workspacePath && existing.workspace.workspacePath !== workspaceRef.workspacePath) {
        existing.workspace.workspacePath = workspaceRef.workspacePath;
      }
      return existing;
    }
    if (this.#pendingActors.has(sessionId)) return this.#pendingActors.get(sessionId);
    const workspace = {
      workspacePath: workspaceRef?.workspacePath ?? this.#cwd,
      workspaceKey: workspaceRef?.workspaceKey ?? workspaceRef ?? this.#cwd,
      ...(workspaceRef?.workspaceIdentity
        ? { workspaceIdentity: workspaceRef.workspaceIdentity }
        : {}),
    };
    const file = findSessionFile(this.#cwd, sessionId, this.#env);
    if (!file) {
      throw new ProtocolError(ERROR_INVALID_PARAMS, `unknown session: ${sessionId}`);
    }
    const task = SessionActor.resume({ bridge: this, workspace, sessionId, file })
      .then(async (actor) => {
        // 冷恢复（resubscribe / 超时重建）也要恢复会话的 MCP 工具面：host
        // 显式下发的部分按留档重放，文件配置部分重新解析（与 zcode-cli resume 同语义）。
        const mcpServers = this.#effectiveSessionMcpServers(
          this.#sessionMcpServers.get(actor.sessionId),
        );
        if (Object.keys(mcpServers).length > 0) await actor.applyMcpServers(mcpServers);
        this.#actors.set(actor.sessionId, actor);
        this.#pendingActors.delete(sessionId);
        return actor;
      })
      .catch((error) => {
        this.#pendingActors.delete(sessionId);
        throw error;
      });
    this.#pendingActors.set(sessionId, task);
    return task;
  }

  // ── request dispatch ────────────────────────────────────────────────────

  async handleRequest(method, params) {
    switch (method) {
      // ── v4 subscription family ──
      case "v4/conversation/subscribe":
        return this.#handleSubscribe(params ?? {});
      case "v4/conversation/resync":
        return this.#handleResync(params ?? {});
      case "v4/conversation/unsubscribe":
        this.#dropSubscription(params?.subscriptionId ?? "");
        return {};
      case "v4/command":
        return this.#handleCommand(params ?? {});
      case "v4/commands/query":
        return this.#handleCommandsQuery(params ?? {});
      case "v4/conversation/rowsRange":
        return this.#handleRowsRange(params ?? {});
      case "v4/conversation/usage":
        return this.#handleConversationUsage(params ?? {});
      case "v4/conversation/plans":
        return { plans: [], atSeq: 0, atLogEpoch: "0" };
      case "v4/conversation/workflowRuns":
        return { runs: [] };
      case "v4/conversation/workflowRunEvents":
        return { events: [], hasMore: false };
      case "v4/conversation/backgroundBashOutput": {
        const actor = await this.#ensureActor(params?.sessionId, undefined);
        return actor.backgroundBashOutput(params?.workId ?? "");
      }
      case "v4/connection/flow":
        return {};
      case "v4/attachment/begin":
        return this.#withAttachmentError(() => this.#attachments.begin(params ?? {}));
      case "v4/attachment/chunk":
        return this.#withAttachmentError(() => this.#attachments.chunk(params ?? {}));
      case "v4/attachment/commit":
        return this.#withAttachmentError(() => this.#attachments.commit(params ?? {}));
      case "v4/attachment/abort":
        return this.#withAttachmentError(() => this.#attachments.abort(params ?? {}));
      case "v4/attachment/read":
      case "v4/conversation/attachmentRead":
        return this.#withAttachmentError(() => this.#attachments.read(params ?? {}));
      case "v4/conversation/attachmentStat":
        return this.#withAttachmentError(() => this.#attachments.stat(params ?? {}));
      case "v4/attachment/previewSource":
        return { kind: "chunked" };

      // ── legacy workspace/config family ──
      case "workspace/readPresentation":
        return {
          workspace: this.workspaceRef(params?.workspace?.workspaceKey),
          mode: "build",
          slashCommands: [],
        };
      case "provider/updateAccountConfig":
        return this.#handleAccountConfig(params ?? {});
      case "workspace/updateInteractionPreferences":
        return {
          workspace: params?.workspace ?? this.workspaceRef(params?.workspace?.workspaceKey),
          askUserQuestionAutoResolutionEnabled:
            params?.preferences?.askUserQuestionAutoResolutionEnabled ?? true,
          snoozedInteractionCount: 0,
        };
      case "workspace/updateModelIoPreferences":
        return {
          workspace: params?.workspace ?? this.workspaceRef(params?.workspace?.workspaceKey),
          fullRetentionEnabled: params?.preferences?.fullRetentionEnabled === true,
          updatedSessionCount: 0,
        };
      case "workspace/updateOffPeakToolPolicy":
        return {
          workspace: params?.workspace ?? this.workspaceRef(params?.workspace?.workspaceKey),
          enabled: params?.enabled === true,
        };
      case "workspace/updateDynamicWorkflowPolicy":
        return {
          workspace: params?.workspace ?? this.workspaceRef(params?.workspace?.workspaceKey),
          enabled: params?.enabled === true,
        };
      case "workspace/hooks/trustGrant":
        return this.#handleHookTrustGrant(params ?? {});
      case "provider/testModelConnectivity":
        return { success: true };
      case "session/setModel":
        return this.#handleLegacySetModel(params ?? {});
      case "session/setThoughtLevel":
        return this.#handleLegacySetThoughtLevel(params ?? {});
      case "session/setMode": {
        const actor = await this.#ensureActor(params?.sessionId, undefined);
        await actor.switchCollaborationMode(params?.mode ?? "build");
        return {};
      }
      case "mcp/list":
        return this.#handleMcpList(params ?? {});
      case "skills/referenceCatalog": {
        const entries = this.#skills().map((skill) => ({
          // `glm:` 前缀是 UI provider 过滤契约（skillSourceFilter），path 使同名
          // 不同来源仍有稳定行身份——与 zcode-cli toReferenceCatalogEntry 逐字对齐。
          id: `glm:${skill.scope}:${skill.path}`,
          name: skill.name,
          description: skill.description ?? "",
          path: skill.path,
          scope: skill.scope,
          enabled: true,
        }));
        return {
          authority: params?.sessionId && this.#actors.has(params.sessionId) ? "session" : "workspace",
          skills: entries,
        };
      }
      case "plugins/referenceCatalog":
      case "plugins/referenceCatalogWithCategory":
        return { authority: "workspace", plugins: [] };
      case "workspace/generateText":
        return this.#handleGenerateText(params ?? {});
      case "workspace/cancelGenerateText":
        return this.#handleCancelGenerateText(params ?? {});
      case "session/read": {
        const actor = await this.#ensureActor(params?.sessionId, undefined);
        return actor.legacySnapshot({ messageLimit: params?.messageLimit });
      }
      case "session/close":
      case "session/goal":
        return {};

      default:
        throw methodNotFound(method);
    }
  }

  handleNotification(method) {
    // v4/connection/flow backpressure hints and telemetry frames require no action.
    void method;
  }

  /**
   * mcp/list（设置页状态检查）：host 把 UI 归并后的 enabled MCP 全量下发，bridge
   * 经共享 probe pi-rs 进程真实连接并回写 per-server 状态快照。mode=status 是
   * OAuth 轮询的只读路径（不触发连接/断开）。
   */
  /**
   * workspace/hooks/trustGrant（Settings 页无会话预信任）：bridge 作为 Agent
   * authority 重新发现 canonical snapshot、校验 bundle/declaration digest 后
   * 写信任存储（UI 只提交 digest，记录字段全部由 bridge 重算）。落盘后
   * 该 workspace 的 live 会话立即重跑 provision（pi-rs 下一轮 prompt 生效）。
   */
  async #handleHookTrustGrant(params) {
    const workspacePath = params?.workspace?.workspacePath ?? this.#cwd;
    try {
      const result = await grantWorkspaceHookTrustFlow({
        workspacePath,
        workspaceIdentity: params?.workspace?.workspaceIdentity,
        bundleDigest: params?.bundleDigest,
        hookDeclarationDigest: params?.hookDeclarationDigest,
        homeDir: this.homeDir,
        appVersion: AGENT_VERSION,
        log: (line) => this.#log(line),
      });
      if (result.accepted) {
        this.#reprovisionWorkspaceHooks(result.workspaceIdentity ?? workspacePath);
        return { accepted: true };
      }
      return { accepted: false, reasonCode: result.reasonCode };
    } catch (error) {
      this.#log(`[tack-agent] trustGrant failed: ${error.message}`);
      return { accepted: false, reasonCode: "workspace_hooks_config_unreadable" };
    }
  }

  /** 信任变化后重跑同 workspace 全部 live 会话的 hook provision。 */
  #reprovisionWorkspaceHooks(workspacePath) {
    const target = path.resolve(workspacePath);
    for (const actor of this.#actors.values()) {
      if (path.resolve(actor.workspace.workspacePath) !== target) continue;
      actor
        .provisionHooks()
        .catch((error) => this.#log(`[tack-agent] reprovision hooks failed: ${error.message}`));
    }
  }

  async #handleMcpList(params) {
    const workspacePath = params.workspace?.workspacePath ?? this.#cwd;
    // host 显式下发优先；缺省回落 zcode 配置文件解析（zcode-cli mcp/list 的
    // createConfig 语义：agent 自己读 mcp.servers）。
    const servers =
      protocolMcpServersToPi(params.mcpServers) ??
      loadFileMcpServers({ homeDir: os.homedir(), workspacePath });
    const connect = (params.mode ?? "connect") !== "status";
    try {
      const statuses = await this.#mcpProbes().status({ workspacePath, servers, connect });
      return { statuses };
    } catch (error) {
      this.#log(`[tack-agent] mcp/list failed: ${error.message}`);
      throw new ProtocolError(ERROR_INTERNAL, `mcp/list failed: ${error.message}`);
    }
  }

  async #handleSubscribe(params) {
    const topic = params.topic ?? "";
    const connectionId = params.connectionId ?? "default";
    if (topic.startsWith("conversation/")) {
      const sessionId = topic.slice("conversation/".length);
      if (params.workspace?.workspacePath && !this.#hostWorkspacePath) {
        this.#hostWorkspacePath = params.workspace.workspacePath;
      }
      const actor = await this.#ensureActor(sessionId, {
        workspacePath: params.workspace?.workspacePath,
        workspaceKey: params.workspace?.workspaceKey ?? this.#cwd,
        workspaceIdentity: params.workspace?.workspaceIdentity,
      });
      const subscriptionId = this.#registerSubscription(topic, connectionId, "conversation");
      actor.attach(subscriptionId, connectionId);
      const { frame } = actor.snapshot(subscriptionId);
      // ACK first; the initial snapshot travels as a post-response owned
      // notification (protocol §3.1 ordering rule).
      setImmediate(() => {
        this.#sendFrame(topic, subscriptionId, frame, "initial");
      });
      return { ack: { subscriptionId, mode: "snapshot", logEpoch: actor.logEpoch } };
    }
    if (topic.startsWith("sessions-index/")) {
      const workspaceId = topic.slice("sessions-index/".length);
      const subscriptionId = this.#registerSubscription(topic, connectionId, "sessions-index");
      const snapshot = this.#sessionsIndexSnapshot(workspaceId);
      const state = this.#topicState(topic);
      setImmediate(() => {
        this.#sendTopicSnapshot(topic, subscriptionId, snapshot);
      });
      return { ack: { subscriptionId, mode: "snapshot", logEpoch: state.logEpoch } };
    }
    if (topic.startsWith("workspace-config/")) {
      const workspaceId = topic.slice("workspace-config/".length);
      const subscriptionId = this.#registerSubscription(topic, connectionId, "workspace-config");
      const snapshot = this.#workspaceConfigSnapshot(workspaceId);
      const state = this.#topicState(topic);
      setImmediate(() => {
        this.#sendTopicSnapshot(topic, subscriptionId, snapshot);
      });
      return { ack: { subscriptionId, mode: "snapshot", logEpoch: state.logEpoch } };
    }
    throw new ProtocolError(ERROR_INVALID_PARAMS, `unknown topic: ${topic}`);
  }

  async #handleResync(params) {
    const subscriptionId = params.subscriptionId ?? "";
    const sub = this.#subscriptions.get(subscriptionId);
    if (!sub) throw new ProtocolError(ERROR_INVALID_PARAMS, "unknown subscription");
    if (sub.kind === "conversation") {
      const sessionId = sub.topic.slice("conversation/".length);
      const actor = await this.#ensureActor(sessionId, sub.workspaceId);
      const { frame } = actor.snapshot(subscriptionId);
      setImmediate(() => this.#sendFrame(sub.topic, subscriptionId, frame, "recovery"));
      return { ack: { subscriptionId, mode: "snapshot", logEpoch: actor.logEpoch } };
    }
    const workspaceId = sub.topic.slice(sub.topic.indexOf("/") + 1);
    const snapshot =
      sub.kind === "sessions-index"
        ? this.#sessionsIndexSnapshot(workspaceId)
        : this.#workspaceConfigSnapshot(workspaceId);
    const state = this.#topicState(sub.topic);
    setImmediate(() => this.#sendTopicSnapshot(sub.topic, subscriptionId, snapshot, "recovery"));
    return { ack: { subscriptionId, mode: "snapshot", logEpoch: state.logEpoch } };
  }

  #recordAck(sessionId, ack) {
    const key = `${sessionId ?? "null"}|${ack.commandId}`;
    this.#recentAcks.set(key, ack);
    if (this.#recentAcks.size > 512) {
      const oldest = this.#recentAcks.keys().next().value;
      this.#recentAcks.delete(oldest);
    }
    return ack;
  }

  async #handleCommand(envelope) {
    const { commandId, type, payload = {}, sessionId } = envelope ?? {};
    if (!commandId || !type) {
      throw new ProtocolError(ERROR_INVALID_PARAMS, "invalid command envelope");
    }
    const workspaceId = payload.workspaceId ?? this.#cwd;
    try {
      switch (type) {
        case "createSession": {
          const workspace = {
            workspacePath: this.#cwd,
            workspaceKey: workspaceId,
          };
          // MCP 是 runtime 启动期配置（ZCode 协议注释）：必须随 create 一次性
          // 进入会话，首发 prompt 之前注入 pi-rs。host 显式下发的部分留档
          // （冷恢复重放）；文件配置部分每次恢复时重新解析，不冻结。
          const hostMcpServers = protocolMcpServersToPi(payload.mcpServers);
          const mcpServers = this.#effectiveSessionMcpServers(hostMcpServers);
          const actor = await SessionActor.spawnNew({
            bridge: this,
            workspace,
            config: payload.config ?? {},
            mcpServers: Object.keys(mcpServers).length > 0 ? mcpServers : null,
          });
          this.#actors.set(actor.sessionId, actor);
          if (hostMcpServers) this.#sessionMcpServers.set(actor.sessionId, hostMcpServers);
          this.notifySessionChanged(actor);
          let input;
          if (payload.firstInput?.text) {
            input = { delivery: "startNow", inputId: randomUUID() };
            // Fire and forget: the ACK must not wait for the turn.
            actor
              .sendText({
                text: payload.firstInput.text,
                modelSelection: payload.firstInput.modelSelection,
                attachments: payload.firstInput.attachments,
                commandId,
                clientId: envelope.clientId,
              })
              .catch((error) => this.#log(`[tack-agent] firstInput failed: ${error.message}`));
          }
          return this.#recordAck(null, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
            result: { type: "createSession", sessionId: actor.sessionId, ...(input ? { input } : {}) },
          });
        }
        case "sendText": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          const inputId = randomUUID();
          const sendResult = actor
            .sendText({
              text: payload.text ?? "",
              modelSelection: payload.modelSelection,
              attachments: payload.attachments,
              requestedDelivery: payload.requestedDelivery,
              commandId,
              clientId: envelope.clientId,
            })
            .catch((error) => this.#log(`[tack-agent] sendText failed: ${error.message}`));
          const { delivery = "startNow" } = (await sendResult) ?? {};
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
            result: { type: "inputAccepted", delivery, inputId },
          });
        }
        case "stop": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          actor.stop().catch((error) => this.#log(`[tack-agent] stop failed: ${error.message}`));
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "compact": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          actor
            .compact()
            .catch((error) => this.#log(`[tack-agent] compact failed: ${error.message}`));
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "renameSession": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          await actor.renameSession(payload.title ?? "");
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "forkAssistant": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          try {
            const { sessionId: newSessionId } = await actor.forkAssistant(payload.target ?? {});
            this.#rebindActorToForkedSession(actor, newSessionId);
            this.registerChildSession(sessionId, newSessionId);
            return this.#recordAck(sessionId, {
              commandId,
              status: "accepted",
              revisionAtDecision: actor.revision,
              result: { type: "forkAssistant", sessionId: newSessionId },
            });
          } catch (error) {
            return this.#recordAck(sessionId, {
              commandId,
              status: "failed",
              reasonCode: "fork.rejected",
              message: error.message,
              revisionAtDecision: actor.revision,
            });
          }
        }
        case "editUserQuery": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          try {
            const outcome = await actor.editUserQuery(payload.target ?? {}, payload.newText ?? "");
            return this.#recordAck(sessionId, {
              commandId,
              status: "accepted",
              revisionAtDecision: actor.revision,
              result: {
                type: "editUserQuery",
                disposition: outcome.disposition,
                sessionId: outcome.sessionId,
              },
            });
          } catch (error) {
            return this.#recordAck(sessionId, {
              commandId,
              status: "failed",
              reasonCode: "edit.rejected",
              message: error.message,
              revisionAtDecision: actor.revision,
            });
          }
        }
        case "retryTurn": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          try {
            await actor.retryTurn(payload.target ?? {});
            return this.#recordAck(sessionId, {
              commandId,
              status: "accepted",
              revisionAtDecision: actor.revision,
              result: { type: "inputAccepted", delivery: "startNow", inputId: randomUUID() },
            });
          } catch (error) {
            return this.#recordAck(sessionId, {
              commandId,
              status: "failed",
              reasonCode: "retry.rejected",
              message: error.message,
              revisionAtDecision: actor.revision,
            });
          }
        }
        case "deleteSession": {
          const actor = this.#actors.get(sessionId);
          if (actor) {
            await actor.dispose();
            this.#actors.delete(sessionId);
          }
          this.#sessionMcpServers.delete(sessionId);
          deleteSessionFile(this.#cwd, sessionId, this.#env);
          this.#notifySessionRemoved(workspaceId, sessionId);
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: 0,
          });
        }
        case "switchModelConfig": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          actor
            .applyModelSelection(
              { providerId: payload.provider, modelId: payload.model },
              payload.thought,
            )
            .catch((error) => this.#log(`[tack-agent] switchModelConfig failed: ${error.message}`));
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "switchCollaborationMode": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          await actor.switchCollaborationMode(payload.mode ?? "build");
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "setFollowupMode":
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: 0,
          });
        case "deleteQueueItem": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          actor
            .deleteQueueItem(payload.queueItemId ?? "")
            .catch((error) => this.#log(`[tack-agent] deleteQueueItem failed: ${error.message}`));
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "editQueueItem": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          actor
            .editQueueItem(payload.queueItemId ?? "", payload.newText ?? "")
            .catch((error) => this.#log(`[tack-agent] editQueueItem failed: ${error.message}`));
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "reorderQueueItem": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          actor
            .reorderQueueItem(payload.queueItemId ?? "", payload.beforeQueueItemId ?? null)
            .catch((error) => this.#log(`[tack-agent] reorderQueueItem failed: ${error.message}`));
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "setAutoDrain": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          actor
            .setAutoDrain(payload.autoDrain === true)
            .catch((error) => this.#log(`[tack-agent] setAutoDrain failed: ${error.message}`));
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "sendQueuedNow": {
          // pi runs follow_up at turn end; nothing extra to trigger.
          const actor = await this.#ensureActor(sessionId, workspaceId);
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "cancelBackgroundWork": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          actor
            .cancelBackgroundWork(payload.workId ?? "")
            .catch((error) => this.#log(`[tack-agent] cancelBackgroundWork failed: ${error.message}`));
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "requestWorkspaceHookReview": {
          // 软门禁 fire-and-forget：banner 点击已自行打开设置页 Hooks 分区，
          // 完整 review interaction（workspaceHookReview payload）未实现，
          // 这里只需让命令通道不报错。
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: 0,
          });
        }
        case "revokeWorkspaceHookTrust": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          const result = await revokeWorkspaceHookTrustFlow({
            workspacePath: actor.workspace.workspacePath,
            workspaceIdentity: payload.workspaceIdentity,
            hookDeclarationDigests: payload.hookDeclarationDigests,
            homeDir: this.homeDir,
          });
          if (!result.accepted) {
            return this.#recordAck(sessionId, {
              commandId,
              status: "failed",
              reasonCode: result.reasonCode,
              revisionAtDecision: actor.revision,
            });
          }
          // 撤销即刻生效：重跑 provision，下一轮 prompt 不再下发。
          await actor.provisionHooks();
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
          });
        }
        case "resolveInteraction": {
          const actor = await this.#ensureActor(sessionId, workspaceId);
          const outcome = await actor.resolveInteractionCommand(
            payload.interactionId ?? "",
            payload.answer ?? {},
            envelope.clientId ?? "unknown",
          );
          if (!outcome.resolved) {
            return this.#recordAck(sessionId, {
              commandId,
              status: "noop",
              reasonCode: outcome.reasonCode ?? "proto.alreadyResolved",
              revisionAtDecision: actor.revision,
            });
          }
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
            result: { type: "resolveInteraction", resolvedBy: outcome.resolvedBy },
          });
        }
        case "snoozeInteractionAutoResolution":
          return this.#recordAck(sessionId, {
            commandId,
            status: "noop",
            reasonCode: "proto.noPendingInteraction",
            revisionAtDecision: 0,
          });
        default:
          return this.#recordAck(sessionId, {
            commandId,
            status: "failed",
            reasonCode: "unsupported.command",
            message: `tack-agent does not support command ${type}`,
            revisionAtDecision: 0,
          });
      }
    } catch (error) {
      return this.#recordAck(sessionId, {
        commandId,
        status: "failed",
        reasonCode: "internal.error",
        message: error.message,
        revisionAtDecision: 0,
      });
    }
  }

  #handleCommandsQuery(params) {
    const results = (params.commands ?? []).map((key) => {
      const ack = this.#recentAcks.get(`${key.sessionId ?? "null"}|${key.commandId}`);
      return { key, result: ack ?? "unknown" };
    });
    return { results };
  }

  async #handleRowsRange(params) {
    const actor = await this.#ensureActor(params.sessionId, undefined);
    return actor.rowsRange({ beforeRowId: params.beforeRowId, limit: params.limit ?? 60 });
  }

  async #handleConversationUsage(params) {
    const actor = await this.#ensureActor(params.sessionId, undefined);
    const cumulative = actor.usage.cumulative;
    return {
      sessionId: actor.sessionId,
      totalTokens: cumulative.inputTokens + cumulative.outputTokens,
      inputTokens: cumulative.inputTokens,
      outputTokens: cumulative.outputTokens,
      reasoningTokens: 0,
      cacheCreationTokens: cumulative.cacheWriteTokens,
      cacheReadTokens: cumulative.cacheReadTokens,
      modelRequestCount: 0,
      modelErrorCount: 0,
      inputBaselineBySource: {},
    };
  }

  #handleAccountConfig(params) {
    this.#accountConfig = params;
    const providerCount = params.providers ? Object.keys(params.providers).length : 0;
    this.refreshCatalog();
    for (const topic of this.#subscriptionsByTopic.keys()) {
      if (topic.startsWith("workspace-config/")) {
        this.#publishWorkspaceConfig(topic.slice("workspace-config/".length));
      }
    }
    return {
      receivedRevision: params.revision ?? "",
      providerCount,
      status: "received",
    };
  }

  // ── workspace/generateText: one-shot pi-rs print-mode call ──────────────

  async #handleGenerateText(params) {
    const selection = params.selection;
    if (!selection?.providerId || !selection?.modelId) {
      throw new ProtocolError(ERROR_INVALID_PARAMS, "selection is required");
    }
    const promptText = params.prompt ?? flattenGenerateMessages(params.messages);
    if (!promptText) {
      throw new ProtocolError(ERROR_INVALID_PARAMS, "prompt or messages is required");
    }
    const providerId = this.toPiProviderId(selection.providerId);
    await this.ensureProviderAuth(providerId, {
      workspace: params.workspace,
      modelSelection: { providerId, modelId: selection.modelId },
    });
    const args = ["-p", promptText, "--no-tools", "--mode", "json"];
    args.push("--provider", providerId, "--model", selection.modelId);
    if (selection.options?.reasoningLevel) {
      args.push("--thinking", selection.options.reasoningLevel);
    }
    const child = spawn(this.piBinary, args, {
      cwd: params.workspace?.workspacePath ?? this.#cwd,
      env: this.piSpawnEnv(),
      stdio: ["ignore", "pipe", "pipe"],
    });
    const operationId = params.operationId ?? randomUUID();
    if (params.operationId) this.#generateTextProcesses.set(operationId, child);
    try {
      const result = await collectPrintModeResult(child, 180_000);
      if (result.errorMessage) {
        throw new ProtocolError(ERROR_INTERNAL, result.errorMessage);
      }
      if (!result.text) {
        throw new ProtocolError(ERROR_INTERNAL, "empty generation result");
      }
      return {
        text: result.text,
        selection,
        ...(result.finishReason ? { finishReason: result.finishReason } : {}),
        usage: result.usage,
      };
    } finally {
      this.#generateTextProcesses.delete(operationId);
    }
  }

  #handleCancelGenerateText(params) {
    const child = this.#generateTextProcesses.get(params?.operationId);
    if (child) {
      this.#generateTextProcesses.delete(params.operationId);
      child.kill("SIGTERM");
      return { operationId: params.operationId, cancelled: true };
    }
    return { operationId: params?.operationId ?? "", cancelled: false };
  }

  async #handleLegacySetModel(params) {
    const actor = await this.#ensureActor(params.sessionId, undefined);
    const selection = params.model ?? {};
    await actor.applyModelSelection(
      { providerId: selection.providerId, modelId: selection.modelId },
      selection.options?.reasoningLevel,
    );
    return {};
  }

  async #handleLegacySetThoughtLevel(params) {
    const actor = await this.#ensureActor(params.sessionId, undefined);
    await actor.applyModelSelection(
      { providerId: actor.config.provider, modelId: actor.config.model },
      params.thoughtLevel,
    );
    return {};
  }

  async dispose() {
    this.#disposed = true;
    for (const pending of this.#reversePending.values()) {
      clearTimeout(pending.timeout);
      pending.reject(new Error("bridge disposed"));
    }
    this.#reversePending.clear();
    this.#mcpProbePool?.dispose();
    this.#mcpProbePool = null;
    await Promise.all([...this.#actors.values()].map((actor) => actor.dispose()));
  }
}
