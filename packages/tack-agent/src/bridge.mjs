/* oxlint-disable max-lines -- bridge dispatch co-locates the whole host-facing method surface; splitting would scatter protocol handlers from their shared subscription state. */
// WorkspaceBridge: the ZCode Protocol endpoint. One bridge process serves one
// workspace (the host spawns one agent process per workspaceKey); it owns the
// sessions-index / workspace-config topics, routes conversation topics to
// SessionActors, and brokers provider credentials from host to pi-rs.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
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

const piProviders = JSON.parse(
  fs.readFileSync(
    path.join(path.dirname(fileURLToPath(import.meta.url)), "piProviders.generated.json"),
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

  constructor({ send, cwd, env, log }) {
    this.#send = send;
    this.#cwd = cwd;
    this.#env = env;
    this.#log = log;
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

  // ── workspace / catalog helpers ─────────────────────────────────────────

  workspaceRef(workspaceId) {
    return {
      workspacePath: this.#cwd,
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
    // Same-topic resubscribe replaces the previous registration (protocol rule).
    const previous = this.#subscriptionsByTopic.get(topic);
    if (previous) {
      for (const subId of previous) this.#subscriptions.delete(subId);
      previous.clear();
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
    return {
      sessionId: actor.sessionId,
      workspaceId,
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
      sessions.push({
        sessionId: entry.sessionId,
        workspaceId,
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
        slashCommands: [],
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

  async #ensureActor(sessionId, workspaceId) {
    const existing = this.#actors.get(sessionId);
    if (existing) return existing;
    if (this.#pendingActors.has(sessionId)) return this.#pendingActors.get(sessionId);
    const workspace = {
      workspacePath: this.#cwd,
      workspaceKey: workspaceId ?? this.#cwd,
    };
    const file = findSessionFile(this.#cwd, sessionId, this.#env);
    if (!file) {
      throw new ProtocolError(ERROR_INVALID_PARAMS, `unknown session: ${sessionId}`);
    }
    const task = SessionActor.resume({ bridge: this, workspace, sessionId, file })
      .then((actor) => {
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
      case "v4/connection/flow":
        return {};

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
        return { accepted: true };
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
        return { statuses: {} };
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

  async #handleSubscribe(params) {
    const topic = params.topic ?? "";
    const connectionId = params.connectionId ?? "default";
    if (topic.startsWith("conversation/")) {
      const sessionId = topic.slice("conversation/".length);
      const workspaceId = params.workspace?.workspaceKey ?? this.#cwd;
      const actor = await this.#ensureActor(sessionId, workspaceId);
      const subscriptionId = this.#registerSubscription(topic, connectionId, "conversation");
      actor.attach(subscriptionId, connectionId);
      const { frame } = actor.snapshot(subscriptionId);
      // ACK first; the initial snapshot travels as a post-response owned
      // notification (protocol §3.1 ordering rule).
      queueMicrotask(() => {
        this.#sendFrame(topic, subscriptionId, frame, "initial");
      });
      return { ack: { subscriptionId, mode: "snapshot", logEpoch: actor.logEpoch } };
    }
    if (topic.startsWith("sessions-index/")) {
      const workspaceId = topic.slice("sessions-index/".length);
      const subscriptionId = this.#registerSubscription(topic, connectionId, "sessions-index");
      const snapshot = this.#sessionsIndexSnapshot(workspaceId);
      const state = this.#topicState(topic);
      queueMicrotask(() => {
        this.#sendTopicSnapshot(topic, subscriptionId, snapshot);
      });
      return { ack: { subscriptionId, mode: "snapshot", logEpoch: state.logEpoch } };
    }
    if (topic.startsWith("workspace-config/")) {
      const workspaceId = topic.slice("workspace-config/".length);
      const subscriptionId = this.#registerSubscription(topic, connectionId, "workspace-config");
      const snapshot = this.#workspaceConfigSnapshot(workspaceId);
      const state = this.#topicState(topic);
      queueMicrotask(() => {
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
      queueMicrotask(() => this.#sendFrame(sub.topic, subscriptionId, frame, "recovery"));
      return { ack: { subscriptionId, mode: "snapshot", logEpoch: actor.logEpoch } };
    }
    const workspaceId = sub.topic.slice(sub.topic.indexOf("/") + 1);
    const snapshot =
      sub.kind === "sessions-index"
        ? this.#sessionsIndexSnapshot(workspaceId)
        : this.#workspaceConfigSnapshot(workspaceId);
    const state = this.#topicState(sub.topic);
    queueMicrotask(() => this.#sendTopicSnapshot(sub.topic, subscriptionId, snapshot, "recovery"));
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
          const actor = await SessionActor.spawnNew({
            bridge: this,
            workspace,
            config: payload.config ?? {},
          });
          this.#actors.set(actor.sessionId, actor);
          this.notifySessionChanged(actor);
          let input;
          if (payload.firstInput?.text) {
            input = { delivery: "startNow", inputId: randomUUID() };
            // Fire and forget: the ACK must not wait for the turn.
            actor
              .sendText({
                text: payload.firstInput.text,
                modelSelection: payload.firstInput.modelSelection,
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
          actor
            .sendText({
              text: payload.text ?? "",
              modelSelection: payload.modelSelection,
              commandId,
              clientId: envelope.clientId,
            })
            .catch((error) => this.#log(`[tack-agent] sendText failed: ${error.message}`));
          return this.#recordAck(sessionId, {
            commandId,
            status: "accepted",
            revisionAtDecision: actor.revision,
            result: { type: "inputAccepted", delivery: "startNow", inputId },
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
        case "deleteSession": {
          const actor = this.#actors.get(sessionId);
          if (actor) {
            await actor.dispose();
            this.#actors.delete(sessionId);
          }
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
        case "resolveInteraction":
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
    await Promise.all([...this.#actors.values()].map((actor) => actor.dispose()));
  }
}
