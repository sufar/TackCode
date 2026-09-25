/* oxlint-disable max-lines -- the session projection state machine is one cohesive unit (rows, streaming, commands, snapshot); splitting it would duplicate the shared private state it operates on. */
// SessionActor: one ZCode conversation <-> one `pi-rs --mode rpc` child.
// Owns the conversation topic's projection: rows, seq/revision accounting,
// control/config/usage state, and the pi-rs event -> v4 delta state machine.
import { randomUUID } from "node:crypto";
import { spawnPiRpc } from "./piRpc.mjs";
import {
  RowFactory,
  contentBlocksText as textOf,
  previewText,
  toolInputText,
  toolResultText,
} from "./projection.mjs";
import { readSessionFileMeta } from "./piHome.mjs";

const SNAPSHOT_TAIL_ROWS = 60;
const FLUSH_WINDOW_MS = 30;
const IDLE_CONTROL = {
  canStop: false,
  stopState: "idle",
  stopTargetKind: "unknown",
  activeWorks: [],
  lastError: null,
  apiRetry: null,
};

/** ZCode collaboration mode -> pi SessionMode. */
export function zcodeModeToPi(mode) {
  switch (mode) {
    case "plan":
      return "plan";
    case "edit":
      return "acceptEdits";
    case "yolo":
      return "bypass";
    default:
      return "ask"; // build / auto
  }
}

const PERMISSION_DENIED_CONTENT =
  "The user doesn't want to proceed with this tool use. The tool use was rejected (eg. if it was a file edit, the new_string was NOT written to the file). STOP what you are doing and wait for the user to tell you how to proceed.";

/**
 * Permission options come in two vocabularies the host validates strictly:
 * legacy reverse-request options (zcodePermissionOptionSchema:
 * optionId/kind/name/response) and v4 snapshot options (optionId/label/
 * kind: allowOnce|allowAlways|deny). Declared responses mirror the real
 * CLI's buildProtocolPermissionOptions.
 */
function buildPermissionOptions(toolName) {
  const legacy = [
    {
      optionId: "allow_once",
      kind: "allow_once",
      name: "Allow once",
      response: { decision: "allow", reason: "Approved once" },
    },
    {
      optionId: "allow_project",
      kind: "allow_always",
      name: "Always allow in this project",
      description: "Do not ask again for matching requests in this project",
      response: {
        decision: "allow",
        reason: "Approved for this project",
        permissionUpdates: [{ type: "addRules", behavior: "allow", rules: [{ toolName }] }],
      },
    },
    {
      optionId: "deny",
      kind: "deny",
      name: "Deny",
      response: { decision: "deny", reason: PERMISSION_DENIED_CONTENT },
    },
  ];
  const v4 = [
    { optionId: "allowOnce", label: "Allow once", kind: "allowOnce", response: legacy[0].response },
    {
      optionId: "allowAlways",
      label: "Always allow in this project",
      kind: "allowAlways",
      response: legacy[1].response,
    },
    { optionId: "deny", label: "Deny", kind: "deny", response: legacy[2].response },
  ];
  return { legacy, v4 };
}

function riskLevelForTool(toolName) {
  if (toolName === "bash") return "high";
  if (toolName === "write" || toolName === "edit") return "medium";
  return "low";
}

/** ZCodePermissionResponse -> pi permission_response payload. */
function zcodeResponseToPiDecision(response) {
  if (
    !response ||
    response.decision === "deny" ||
    response.decision === "escalate" ||
    response.decision === "modify"
  ) {
    return { decision: "deny", ...(response?.reason ? { reason: response.reason } : {}) };
  }
  const hasAlways = (response.permissionUpdates ?? []).some(
    (update) => update?.type === "addRules" && update.behavior === "allow",
  );
  return { decision: hasAlways ? "allowAlways" : "allow" };
}

/** Map a v4 toolCall row to a legacy zcodeToolStateSchema state. */
function legacyToolState(row) {
  const startedAt = row.startedAt ?? row.createdAt;
  if (row.status === "success") {
    return {
      status: "completed",
      input: row.input ?? {},
      output: row.output?.text ?? "",
      title: row.toolName,
      metadata: {},
      startedAt,
      completedAt: row.endedAt ?? startedAt,
    };
  }
  if (row.status === "error" || row.status === "cancelled") {
    return {
      status: "error",
      input: row.input ?? {},
      error: row.error?.message ?? row.output?.text ?? "tool failed",
      startedAt,
      completedAt: row.endedAt ?? startedAt,
    };
  }
  if (row.status === "running") {
    return { status: "running", input: row.input ?? {}, title: row.toolName, startedAt };
  }
  return { status: "pending", input: row.input ?? {}, raw: row.inputText ?? "" };
}

function allAvailability() {
  const allowed = { allowed: true };
  return {
    fork: allowed,
    compact: allowed,
    switchModelConfig: allowed,
    setFollowupMode: allowed,
    queueEdit: allowed,
    sendQueuedNow: allowed,
    pauseGoal: { allowed: false, reasonCode: "unsupported.goals" },
    resumeGoal: { allowed: false, reasonCode: "unsupported.goals" },
  };
}

export class SessionActor {
  #bridge;
  #workspace;
  #rpc = null;
  #rowFactory = new RowFactory();
  #subscriptions = new Map(); // subscriptionId -> {connectionId}
  #outbox = [];
  #flushTimer = null;
  #disposed = false;
  #stopTimer = null;

  sessionId;
  logEpoch = randomUUID();
  seq = 0;
  revision = 0;
  rows = [];
  #rowById = new Map();
  #contentRows = new Map(); // streaming contentIndex -> row
  #toolRows = new Map(); // toolCallId -> row
  #turnId = null;
  #turnHeaderRow = null;
  #turnOpen = false;
  #turnHadError = false;
  #responseId = null;
  #assistantUsageSeen = null;
  lastActivityAt;
  meta = { title: "", titleSource: "default" };
  config = {
    provider: "",
    model: "",
    thought: "off",
    thoughtLevels: [],
    followupMode: "queue",
    mode: "build",
  };
  usage = {
    contextWindow: null,
    cumulative: { inputTokens: 0, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
  };
  control = { phase: "draft", sessionEnded: false, ...IDLE_CONTROL };
  modelContextWindow = 0;
  streaming = false;
  compacting = false;
  firstUserText = null;
  pendingInteractions = [];
  #pendingPermissions = new Map(); // interactionId -> pending permission

  constructor({ bridge, workspace, sessionId }) {
    this.#bridge = bridge;
    this.#workspace = workspace;
    this.sessionId = sessionId;
    this.createdAt = Date.now();
    this.lastActivityAt = this.createdAt;
  }

  get workspace() {
    return this.#workspace;
  }

  // ── lifecycle ──────────────────────────────────────────────────────────

  static async spawnNew({ bridge, workspace, config }) {
    const actor = new SessionActor({ bridge, workspace, sessionId: null });
    await actor.#spawn();
    await actor.#readState();
    if (config?.provider && config?.model) {
      await actor.applyModelSelection(
        { providerId: config.provider, modelId: config.model },
        config.thought,
        { persistMarker: false },
      );
    } else if (config?.modelSelection) {
      await actor.applyModelSelection(config.modelSelection, config.thought, {
        persistMarker: false,
      });
    }
    if (config?.mode) actor.config.mode = config.mode;
    return actor;
  }

  static async resume({ bridge, workspace, sessionId, file }) {
    const actor = new SessionActor({ bridge, workspace, sessionId });
    await actor.#spawn();
    if (file) {
      await actor.#rpc.request("switch_session", { path: file }, { timeoutMs: 30_000 });
    }
    const meta = file ? readSessionFileMeta(file) : null;
    await actor.#readState();
    await actor.#rebuildFromHistory(meta);
    return actor;
  }

  async #spawn() {
    this.#rpc = spawnPiRpc({
      piBinary: this.#bridge.piBinary,
      cwd: this.#workspace.workspacePath,
      env: this.#bridge.piSpawnEnv(),
      onEvent: (event) => this.#handlePiEvent(event),
      onExit: () => this.#handlePiExit(),
      onStderr: (line) => this.#bridge.log(`[pi-rs ${this.sessionId ?? "new"}] ${line}`),
    });
  }

  async #readState() {
    const state = await this.#rpc.request("get_state");
    this.sessionId = state.sessionId;
    this.streaming = state.isStreaming === true;
    this.compacting = state.isCompacting === true;
    const model = state.model;
    if (model && typeof model === "object") {
      this.config.provider = model.provider ?? "";
      this.config.model = model.id ?? "";
      this.modelContextWindow = model.contextWindow ?? 0;
    }
    this.config.thought = state.thinkingLevel ?? "off";
    try {
      const levels = await this.#rpc.request("get_available_thinking_levels");
      this.config.thoughtLevels = levels?.levels ?? [];
    } catch {
      this.config.thoughtLevels = [];
    }
    // Apply the collaboration mode's permission mode (build -> ask, ...).
    try {
      await this.#rpc.request("set_mode", { mode: zcodeModeToPi(this.config.mode) });
    } catch (error) {
      this.#bridge.log(`[tack-agent] set_mode failed: ${error.message}`);
    }
    this.control.phase = this.streaming ? "running" : "draft";
    if (this.streaming) {
      this.control = {
        ...this.control,
        canStop: true,
        stopState: "stoppable",
        activeWorks: [{ kind: "primaryTurn", startedAt: this.lastActivityAt }],
      };
    }
  }

  async #rebuildFromHistory(fileMeta) {
    const { messages } = await this.#rpc.request("get_messages", {}, { timeoutMs: 30_000 });
    const list = Array.isArray(messages) ? messages : [];
    let turnCounter = 0;
    let lastSeq = 0;
    const bumpSeq = () => ++lastSeq;
    let currentTurnId = null;
    for (const message of list) {
      const at = typeof message?.timestamp === "number" ? message.timestamp : this.createdAt;
      if (message.role === "system") continue;
      if (message.role === "user") {
        const text = textOf(message.content);
        if (!text) continue;
        turnCounter += 1;
        currentTurnId = `turn-${turnCounter}`;
        this.#pushRow(
          this.#rowFactory.turnHeader({
            turnId: currentTurnId,
            state: "completedSuccess",
            at,
            seq: bumpSeq(),
          }),
        );
        const row = this.#rowFactory.userInput({
          turnId: currentTurnId,
          text,
          at,
          seq: bumpSeq(),
          origin: "realUser",
        });
        this.#pushRow(row);
        if (!this.firstUserText) this.firstUserText = text;
        continue;
      }
      if (message.role === "assistant") {
        if (!currentTurnId) {
          turnCounter += 1;
          currentTurnId = `turn-${turnCounter}`;
          this.#pushRow(
            this.#rowFactory.turnHeader({
              turnId: currentTurnId,
              state: "completedSuccess",
              at,
              seq: bumpSeq(),
            }),
          );
        }
        const responseId = message.responseId ?? `resp-${at}`;
        const failed = typeof message.errorMessage === "string" && message.errorMessage;
        for (const block of Array.isArray(message.content) ? message.content : []) {
          if (block.type === "text" && block.text) {
            this.#pushRow(
              this.#rowFactory.assistantText({
                turnId: currentTurnId,
                at,
                seq: bumpSeq(),
                state: failed ? "failed" : "complete",
                text: block.text,
                model: message.model,
                responseId,
              }),
            );
          } else if (block.type === "thinking" && block.thinking) {
            this.#pushRow(
              this.#rowFactory.reasoning({
                turnId: currentTurnId,
                at,
                seq: bumpSeq(),
                state: "complete",
                text: block.thinking,
                responseId,
              }),
            );
          } else if (block.type === "toolCall") {
            const row = this.#rowFactory.toolCall({
              turnId: currentTurnId,
              at,
              seq: bumpSeq(),
              toolCallId: block.id,
              toolName: block.name,
              status: "success",
              responseId,
            });
            row.input = block.arguments;
            row.inputText = toolInputText(block.arguments);
            this.#pushRow(row);
            this.#toolRows.set(block.id, row);
          }
        }
        this.#accumulateUsage(message.usage);
        continue;
      }
      if (message.role === "toolResult") {
        const row = this.#toolRows.get(message.toolCallId);
        if (row) {
          row.output = { text: toolResultText({ content: message.content }) };
          row.status = message.isError ? "error" : "success";
          row.endedAt = typeof message.timestamp === "number" ? message.timestamp : at;
          if (message.isError) {
            row.error = { code: "tool_error", message: previewText(row.output.text) ?? "error" };
          }
        }
        continue;
      }
      if (message.role === "compactionSummary") {
        this.#pushRow(
          this.#rowFactory.timelineMarker({
            turnId: currentTurnId ?? "turn-0",
            at,
            seq: bumpSeq(),
            marker: {
              type: "compact",
              origin: "auto",
              status: "success",
              ...(message.tokensBefore ? { tokensBefore: message.tokensBefore } : {}),
            },
          }),
        );
      }
    }
    this.seq = lastSeq;
    const hasRows = this.rows.length > 0;
    this.control.phase = this.streaming ? "running" : hasRows ? "completedSuccess" : "draft";
    if (fileMeta) {
      this.createdAt = fileMeta.createdAt ?? this.createdAt;
      if (fileMeta.name) {
        this.meta = { title: fileMeta.name, titleSource: "custom" };
      } else if (fileMeta.firstUserText) {
        this.meta = { title: previewText(fileMeta.firstUserText) ?? "", titleSource: "generated" };
      }
    } else if (this.firstUserText) {
      this.meta = { title: previewText(this.firstUserText) ?? "", titleSource: "generated" };
    }
    if (this.modelContextWindow && this.#assistantUsageSeen) {
      this.usage.contextWindow = {
        usedTokens: this.#assistantUsageSeen.totalTokens ?? 0,
        maxTokens: this.modelContextWindow,
        autoCompactThresholdTokens: null,
      };
    }
  }

  #pushRow(row) {
    row.productTurnId = row.turnId;
    this.rows.push(row);
    this.#rowById.set(row.rowId, row);
  }

  #accumulateUsage(usage) {
    if (!usage || typeof usage !== "object") return;
    this.#assistantUsageSeen = usage;
    this.usage.cumulative.inputTokens += usage.input ?? 0;
    this.usage.cumulative.outputTokens += usage.output ?? 0;
    this.usage.cumulative.cacheReadTokens += usage.cacheRead ?? 0;
    this.usage.cumulative.cacheWriteTokens += usage.cacheWrite ?? 0;
    if (this.modelContextWindow) {
      this.usage.contextWindow = {
        usedTokens: usage.totalTokens ?? 0,
        maxTokens: this.modelContextWindow,
        autoCompactThresholdTokens: null,
      };
    }
  }

  // ── subscription plumbing ──────────────────────────────────────────────

  attach(subscriptionId, connectionId) {
    this.#subscriptions.set(subscriptionId, { connectionId });
  }

  detach(subscriptionId) {
    this.#subscriptions.delete(subscriptionId);
  }

  get subscriptionCount() {
    return this.#subscriptions.size;
  }

  #emit(ops) {
    if (this.#disposed || ops.length === 0) return;
    this.#outbox.push(...ops);
    if (this.#flushTimer) return;
    this.#flushTimer = setTimeout(() => this.#flush(), FLUSH_WINDOW_MS);
    this.#flushTimer.unref?.();
  }

  #flush() {
    this.#flushTimer = null;
    if (this.#outbox.length === 0 || this.#disposed) return;
    const ops = this.#outbox;
    this.#outbox = [];
    const fromSeq = this.seq;
    this.seq += ops.length;
    this.#bridge.sendConversationFrame(this.sessionId, [...this.#subscriptions.keys()], {
      fromSeq,
      toSeq: this.seq,
      payload: { kind: "deltas", deltas: ops },
    });
    this.#bridge.notifySessionChanged(this);
  }

  #statePatch(patch) {
    this.revision += 1;
    return { op: "state.updated", patch: { revision: this.revision, ...patch } };
  }

  snapshot(subscriptionId) {
    this.#flush();
    const window = this.rows.slice(-SNAPSHOT_TAIL_ROWS);
    return {
      frame: {
        fromSeq: 0,
        toSeq: this.seq,
        payload: {
          kind: "snapshot",
          snapshot: {
            protocolVersion: 1,
            sessionId: this.sessionId,
            logEpoch: this.logEpoch,
            seq: this.seq,
            revision: this.revision,
            control: this.control,
            availability: allAvailability(),
            inputRouting: { mode: "startNow" },
            meta: this.meta,
            config: this.config,
            modelTransition: null,
            usage: this.usage,
            queue: { items: [], autoDrain: true },
            pendingInteractions: this.pendingInteractions,
            pendingCommands: [],
            backgroundWorks: [],
            subagents: { revision: 0, childSessionIds: [], running: [], endedTotal: 0 },
            goal: null,
            plan: null,
            workspaceHookAdmission: null,
            rows: {
              window,
              totalCount: this.rows.length,
              firstRowId: this.rows[0]?.rowId ?? null,
            },
          },
        },
      },
      subscriptionId,
    };
  }

  rowsRange({ beforeRowId, limit }) {
    const all = this.rows;
    let end = all.length;
    if (typeof beforeRowId === "number") {
      end = all.findIndex((row) => row.rowId >= beforeRowId);
      if (end === -1) end = all.length;
    }
    const start = Math.max(0, end - limit);
    return {
      rows: all.slice(start, end),
      atSeq: this.seq,
      atRevision: this.revision,
      atLogEpoch: this.logEpoch,
      hasMore: start > 0,
    };
  }

  /**
   * Legacy `session/read` snapshot (zcodeSessionStateSnapshotSchema). The
   * task-index syncer resyncs task rows from this shape; keep it faithful.
   */
  legacySnapshot({ messageLimit } = {}) {
    const statusMap = {
      draft: "idle",
      prewarming: "running",
      running: "running",
      completedSuccess: "completed",
      completedInterrupted: "completed",
      error: "error",
    };
    const status = statusMap[this.control.phase] ?? "idle";
    const modelSelection =
      this.config.provider && this.config.model
        ? { providerId: this.config.provider, modelId: this.config.model }
        : undefined;
    const contextUsed = this.usage.contextWindow?.usedTokens ?? 0;
    const contextWindow = this.usage.contextWindow?.maxTokens ?? this.modelContextWindow ?? 0;
    const mode = this.config.mode ?? "build";
    const titleSource =
      this.meta.titleSource === "custom"
        ? "custom"
        : this.meta.titleSource === "generated"
          ? "generated"
          : this.firstUserText
            ? "first_input"
            : "default";
    const lastError = this.control.lastError;
    return {
      protocol: { name: "ZCode Protocol", version: 1 },
      session: {
        sessionId: this.sessionId,
        workspace: {
          workspacePath: this.#workspace.workspacePath,
          ...(this.#workspace.workspaceIdentity
            ? { workspaceIdentity: this.#workspace.workspaceIdentity }
            : {}),
          workspaceKey: this.#workspace.workspaceKey,
        },
        sessionKind: "interactive",
        title: this.meta.title || "",
        titleSource,
        mode,
        status,
        ...(modelSelection ? { model: modelSelection } : {}),
        createdAt: this.createdAt,
        updatedAt: this.lastActivityAt,
      },
      settings: {
        model: {
          ...(modelSelection ? { current: modelSelection, lastUsed: modelSelection } : {}),
          available: [],
        },
        thoughtLevel: {
          enabled: this.config.thoughtLevels.length > 0,
          ...(this.config.thought ? { current: this.config.thought } : {}),
          available: this.config.thoughtLevels.map((level) => ({ value: level, label: level })),
        },
        mode: { current: mode },
      },
      projection: {
        sessionId: this.sessionId,
        status,
        mode,
        turnCount: this.rows.filter((row) => row.kind === "turnHeader").length,
        totalTokenCount:
          this.usage.cumulative.inputTokens + this.usage.cumulative.outputTokens,
        contextUsed,
        contextWindow,
        ...(this.streaming && this.#turnId ? { currentTurnId: this.#turnId } : {}),
        pendingPermissions: [],
        activeToolCalls: [],
        backgroundJobs: [],
        ...(lastError
          ? {
              lastError: {
                type: lastError.code ?? "error",
                ...(lastError.code ? { code: lastError.code } : {}),
                message: lastError.message,
              },
            }
          : {}),
      },
      runtime: {
        eventSeq: this.seq,
        stateRevision: this.revision,
        pendingRequestIds: [],
        contextUsage: { used: contextUsed, size: Math.max(1, contextWindow) },
      },
      messages: this.#buildLegacyMessages(messageLimit),
      slashCommands: [],
    };
  }

  #buildLegacyMessages(messageLimit) {
    const messages = [];
    let lastUserMessageId = null;
    let turnRows = [];
    const flushTurn = () => {
      if (turnRows.length === 0) return;
      const first = turnRows[0];
      const last = turnRows[turnRows.length - 1];
      const messageId = `a-${first.rowId}`;
      const parts = [];
      for (const row of turnRows) {
        const partBase = { partId: `p-${row.rowId}`, sessionId: this.sessionId, messageId };
        if (row.kind === "assistantText") {
          parts.push({ ...partBase, type: "text", text: row.text });
        } else if (row.kind === "reasoning") {
          parts.push({ ...partBase, type: "reasoning", text: row.text });
        } else if (row.kind === "toolCall") {
          parts.push({ ...partBase, type: "tool", callId: row.toolCallId, tool: row.toolName, state: legacyToolState(row) });
        }
      }
      const completed = turnRows.every((row) => row.kind !== "toolCall" || ["success", "error", "cancelled"].includes(row.status)) && turnRows.every((row) => row.state !== "streaming");
      messages.push({
        info: {
          messageId,
          sessionId: this.sessionId,
          role: "assistant",
          time: {
            created: first.createdAt,
            ...(completed ? { completed: last.createdAt } : {}),
          },
          parentMessageId: lastUserMessageId ?? `u-0`,
          agent: "pi-rs",
          path: { cwd: this.#workspace.workspacePath, root: this.#workspace.workspacePath },
          cost: 0,
          tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
          ...(completed ? { finish: "stop" } : {}),
        },
        parts,
      });
      turnRows = [];
    };
    for (const row of this.rows) {
      if (row.kind === "userInput") {
        flushTurn();
        const messageId = `u-${row.rowId}`;
        lastUserMessageId = messageId;
        messages.push({
          info: {
            messageId,
            sessionId: this.sessionId,
            role: "user",
            time: { created: row.createdAt },
            agent: "pi-rs",
          },
          parts: [
            { partId: `p-${row.rowId}`, sessionId: this.sessionId, messageId, type: "text", text: row.text },
          ],
        });
        continue;
      }
      if (row.kind === "assistantText" || row.kind === "reasoning" || row.kind === "toolCall") {
        if (turnRows.length > 0 && turnRows[0].turnId !== row.turnId) flushTurn();
        turnRows.push(row);
      }
    }
    flushTurn();
    if (typeof messageLimit === "number" && messageLimit > 0) {
      return messages.slice(-messageLimit);
    }
    return messages;
  }

  // ── commands ───────────────────────────────────────────────────────────

  async sendText({ text, modelSelection, commandId, clientId }) {
    if (this.#disposed) throw new Error("session is disposed");
    if (modelSelection) {
      await this.applyModelSelection(modelSelection, undefined, { persistMarker: false });
    }
    const at = Date.now();
    this.lastActivityAt = at;
    const guided = this.streaming;
    if (!this.#turnOpen) {
      this.#turnId = randomUUID();
      this.#turnHadError = false;
      this.#turnHeaderRow = this.#rowFactory.turnHeader({
        turnId: this.#turnId,
        state: "running",
        at,
        seq: this.seq + 1,
        sourceCommandId: commandId,
      });
      this.#pushRow(this.#turnHeaderRow);
      this.#turnOpen = true;
      this.streaming = true;
      this.control = {
        phase: "running",
        sessionEnded: false,
        canStop: true,
        stopState: "stoppable",
        stopTargetKind: "unknown",
        activeWorks: [{ kind: "primaryTurn", startedAt: at }],
        lastError: null,
        apiRetry: null,
      };
      this.#emit([
        { op: "row.appended", row: this.#turnHeaderRow },
        this.#statePatch({ control: this.control }),
      ]);
    }
    const userRow = this.#rowFactory.userInput({
      turnId: this.#turnId,
      text,
      at,
      seq: this.seq + 1,
      origin: "realUser",
      sourceCommandId: commandId,
      clientId,
      guided,
    });
    this.#pushRow(userRow);
    this.#emit([{ op: "row.appended", row: userRow }]);
    if (!this.firstUserText) {
      this.firstUserText = text;
      if (this.meta.titleSource === "default") {
        this.meta = { title: previewText(text) ?? "", titleSource: "generated" };
        this.#emit([this.#statePatch({ meta: this.meta })]);
      }
    }
    await this.#rpc.request("prompt", { message: text }, { timeoutMs: 15_000 });
    return { delivery: "startNow" };
  }

  async stop() {
    if (!this.streaming) return;
    this.control = { ...this.control, stopState: "stopping" };
    this.#emit([this.#statePatch({ control: this.control })]);
    try {
      await this.#rpc.request("abort", {}, { timeoutMs: 10_000 });
    } catch (error) {
      this.#bridge.log(`[tack-agent] abort failed: ${error.message}`);
    }
    // pi-rs normally winds the turn down with its own events; if it stays
    // silent, force the projection to interrupted so the UI never sticks.
    this.#stopTimer = setTimeout(() => this.#forceInterrupted(), 5_000);
    this.#stopTimer.unref?.();
  }

  #forceInterrupted() {
    if (!this.streaming || this.#disposed) return;
    const ops = [];
    for (const row of this.#contentRows.values()) {
      if (row.state === "streaming") {
        row.state = "interrupted";
        ops.push({ op: "row.upserted", row });
      }
    }
    this.#contentRows.clear();
    for (const row of this.#toolRows.values()) {
      if (["inputStreaming", "running", "pendingApproval"].includes(row.status)) {
        row.status = "cancelled";
        row.endedAt = Date.now();
        ops.push({ op: "row.upserted", row });
      }
    }
    this.#finishTurn("completedInterrupted", ops);
    this.#emit(ops);
  }

  async compact() {
    const at = Date.now();
    const turnId = this.#turnId ?? "turn-0";
    const marker = this.#rowFactory.timelineMarker({
      turnId,
      at,
      seq: this.seq + 1,
      lane: "assistantWork",
      marker: { type: "compact", origin: "manual", status: "running" },
    });
    this.#pushRow(marker);
    this.#emit([{ op: "row.appended", row: marker }]);
    try {
      await this.#rpc.request("compact", {}, { timeoutMs: 10 * 60_000 });
      marker.marker = { ...marker.marker, status: "success" };
    } catch (error) {
      marker.marker = { ...marker.marker, status: "failed" };
      this.#bridge.log(`[tack-agent] compact failed: ${error.message}`);
    }
    this.#emit([{ op: "row.upserted", row: marker }]);
  }

  async renameSession(title) {
    try {
      await this.#rpc.request("set_session_name", { name: title }, { timeoutMs: 15_000 });
    } catch (error) {
      this.#bridge.log(`[tack-agent] set_session_name failed: ${error.message}`);
    }
    this.meta = { title, titleSource: "custom" };
    this.#emit([this.#statePatch({ meta: this.meta })]);
    this.#bridge.notifySessionChanged(this);
  }

  async applyModelSelection(selection, thought, { persistMarker = true } = {}) {
    const provider = this.#bridge.toPiProviderId(selection.providerId);
    const model = selection.modelId;
    const thoughtLevel =
      thought ?? selection.options?.reasoningLevel ?? this.config.thought ?? "off";
    if (!provider || !model) return;
    if (provider === this.config.provider && model === this.config.model) {
      if (thoughtLevel !== this.config.thought) {
        await this.#applyThoughtLevel(thoughtLevel);
      }
      return;
    }
    await this.#bridge.ensureProviderAuth(provider, {
      sessionId: this.sessionId,
      workspace: this.#workspace,
      modelSelection: { providerId: provider, modelId: model },
    });
    const from = { provider: this.config.provider, model: this.config.model };
    await this.#rpc.request("set_model", { provider, modelId: model }, { timeoutMs: 30_000 });
    const previousThought = this.config.thought;
    this.config = { ...this.config, provider, model };
    try {
      const levels = await this.#rpc.request("get_available_thinking_levels");
      this.config.thoughtLevels = levels?.levels ?? [];
    } catch {
      /* keep previous levels */
    }
    const modelInfo = this.#bridge.findModelInfo(provider, model);
    if (modelInfo?.contextWindow) this.modelContextWindow = modelInfo.contextWindow;
    await this.#applyThoughtLevel(thoughtLevel, { emit: false });
    if (persistMarker) {
      const at = Date.now();
      const marker = this.#rowFactory.timelineMarker({
        turnId: this.#turnId ?? "turn-0",
        at,
        seq: this.seq + 1,
        lane: "lightBoundary",
        marker: {
          type: "modelChange",
          ...(from.provider ? { fromProvider: from.provider, fromModel: from.model } : {}),
          toProvider: provider,
          toModel: model,
          toThought: previousThought ?? "off",
        },
      });
      this.#pushRow(marker);
      this.#emit([{ op: "row.appended", row: marker }]);
    }
    this.#emit([this.#statePatch({ config: this.config })]);
  }

  async #applyThoughtLevel(level, { emit = true } = {}) {
    const mapped = this.#bridge.mapThinkingLevel(level, this.config.provider);
    try {
      await this.#rpc.request("set_thinking_level", { level: mapped }, { timeoutMs: 15_000 });
      this.config = { ...this.config, thought: level };
      if (emit) this.#emit([this.#statePatch({ config: this.config })]);
    } catch (error) {
      this.#bridge.log(`[tack-agent] set_thinking_level failed: ${error.message}`);
    }
  }

  async switchCollaborationMode(mode) {
    // Map to pi's SessionMode: permission prompts now gate tool calls.
    this.config = { ...this.config, mode };
    try {
      await this.#rpc.request("set_mode", { mode: zcodeModeToPi(mode) });
    } catch (error) {
      this.#bridge.log(`[tack-agent] set_mode failed: ${error.message}`);
    }
    this.#emit([this.#statePatch({ config: this.config })]);
  }

  /** v4 resolveInteraction command (permission answers from the UI). */
  async resolveInteractionCommand(interactionId, answer, clientId) {
    const pending = this.#pendingPermissions.get(interactionId);
    if (!pending) {
      return { resolved: false, reasonCode: "proto.alreadyResolved" };
    }
    const action = answer?.action;
    if (action === "decline" || action === "cancel") {
      await this.#settlePermission(pending, { decision: "deny", reason: undefined });
      return {
        resolved: true,
        resolvedBy: { clientId, ...(answer?.optionId ? { optionId: answer.optionId } : {}) },
      };
    }
    const option =
      pending.options.find((o) => o.optionId === answer?.optionId) ??
      pending.options.find((o) => o.optionId === "deny");
    const response = { ...option.response };
    if (answer?.freeText?.trim()) response.reason = answer.freeText.trim();
    await this.#settlePermission(pending, response);
    return {
      resolved: true,
      resolvedBy: { clientId, ...(answer?.optionId ? { optionId: answer.optionId } : {}) },
    };
  }

  // ── pi-rs event pump ───────────────────────────────────────────────────

  #handlePiEvent(event) {
    if (this.#disposed || !event || typeof event !== "object") return;
    this.lastActivityAt = Date.now();
    if (this.#stopTimer) {
      clearTimeout(this.#stopTimer);
      this.#stopTimer = null;
    }
    switch (event.type) {
      case "agent_start":
        break;
      case "turn_start":
        break;
      case "message_start":
        if (event.message?.role === "assistant") {
          this.#responseId = event.message.responseId ?? randomUUID();
        }
        break;
      case "message_update":
        this.#handleAssistantEvent(event);
        break;
      case "message_end":
        if (event.message?.role === "assistant") {
          this.#accumulateUsage(event.message.usage);
          this.#finalizeStreamingRows(event.message);
        }
        break;
      case "tool_execution_start":
        this.#handleToolExecutionStart(event);
        break;
      case "tool_execution_update":
        // v1: intermediate tool output is rendered on completion.
        break;
      case "tool_execution_end":
        this.#handleToolExecutionEnd(event);
        break;
      case "turn_end":
        this.#handleTurnEnd();
        break;
      case "agent_end":
        this.#handleAgentEnd();
        break;
      case "model_fallback":
        break;
      case "permission_request":
        void this.#handlePermissionRequest(event);
        break;
      case "permission_resolved":
        this.#handlePermissionResolved(event);
        break;
      default:
        break;
    }
  }

  /** pi permission_request -> ZCode permission interaction + host prompt. */
  async #handlePermissionRequest(event) {
    const interactionId = event.requestId;
    const toolCallId = event.toolCallId;
    if (!interactionId || this.#pendingPermissions.has(interactionId)) return;
    const at = Date.now();
    const row = this.#toolRows.get(toolCallId);
    const options = buildPermissionOptions(event.toolName);
    const interaction = {
      interactionId,
      kind: "permission",
      anchorRowId: row?.rowId ?? null,
      createdAt: at,
      payload: {
        kind: "permission",
        toolCallId: toolCallId ?? "",
        toolName: event.toolName,
        summary: event.title ?? event.toolName,
        detail: event.input ?? {},
        options: options.v4,
      },
    };
    const pending = { interactionId, toolCallId, row, options: options.v4, settled: false };
    this.#pendingPermissions.set(interactionId, pending);
    if (row) {
      row.status = "pendingApproval";
      row.approvalInteractionId = interactionId;
    }
    this.pendingInteractions = [...this.pendingInteractions, interaction];
    this.#emit([
      ...(row ? [{ op: "row.upserted", row }] : []),
      this.#statePatch({ pendingInteractions: this.pendingInteractions }),
    ]);
    try {
      // The host answers when the user resolves the dialog (its broker maps
      // the chosen option's declared response back to us).
      const response = await this.#bridge.reverseRequest(
        "interaction/requestPermission",
        {
          requestId: interactionId,
          sessionId: this.sessionId,
          ...(this.#turnId ? { turnId: this.#turnId } : {}),
          toolCallId: toolCallId ?? "",
          toolName: event.toolName,
          reason: event.title ?? event.toolName,
          riskLevel: riskLevelForTool(event.toolName),
          input: event.input ?? {},
          options: options.legacy,
        },
        10 * 60_000,
      );
      this.#bridge.log(
        `[tack-agent] permission host response: ${JSON.stringify(response)?.slice(0, 300)}`,
      );
      await this.#settlePermission(pending, response);
    } catch (error) {
      // Host unreachable / transport closed: fail closed (deny), unless the
      // v4 resolveInteraction path already settled it.
      this.#bridge.log(
        `[tack-agent] permission reverse request failed: ${error instanceof Error ? error.message : String(error)}`,
      );
      await this.#settlePermission(pending, {
        decision: "deny",
        reason: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /** Settle a pending permission: answer pi, clear the interaction. */
  async #settlePermission(pending, response) {
    if (pending.settled) return;
    pending.settled = true;
    this.#pendingPermissions.delete(pending.interactionId);
    const pi = zcodeResponseToPiDecision(response);
    this.#bridge.log(
      `[tack-agent] permission settle: interaction=${pending.interactionId.slice(0, 24)} decision=${pi.decision} response=${JSON.stringify(response)?.slice(0, 200)}`,
    );
    try {
      await this.#rpc.request(
        "permission_response",
        {
          requestId: pending.interactionId,
          decision: pi.decision,
          ...(pi.reason ? { reason: pi.reason } : {}),
        },
        { timeoutMs: 10_000 },
      );
    } catch (error) {
      this.#bridge.log(`[tack-agent] permission_response failed: ${error.message}`);
    }
    this.#clearPermissionInteraction(pending, pi.decision);
  }

  /** pi's own resolution (timeout/cancel): just clear the UI state. */
  #handlePermissionResolved(event) {
    const pending = this.#pendingPermissions.get(event.requestId);
    if (!pending) return;
    pending.settled = true;
    this.#pendingPermissions.delete(event.requestId);
    this.#clearPermissionInteraction(pending, event.decision ?? "deny");
  }

  #clearPermissionInteraction(pending, decision) {
    this.pendingInteractions = this.pendingInteractions.filter(
      (item) => item.interactionId !== pending.interactionId,
    );
    const row = pending.row;
    if (row && row.status === "pendingApproval") {
      row.status = decision === "deny" ? "cancelled" : "running";
      delete row.approvalInteractionId;
    }
    this.#emit([
      ...(row ? [{ op: "row.upserted", row }] : []),
      this.#statePatch({ pendingInteractions: this.pendingInteractions }),
    ]);
  }

  #handleAssistantEvent(event) {
    const slim = event.assistantMessageEvent;
    if (!slim || typeof slim !== "object") return;
    if (event.usage && typeof event.usage === "object") {
      this.#assistantUsageSeen = event.usage;
      if (this.modelContextWindow) {
        this.usage.contextWindow = {
          usedTokens: event.usage.totalTokens ?? this.usage.contextWindow?.usedTokens ?? 0,
          maxTokens: this.modelContextWindow,
          autoCompactThresholdTokens: null,
        };
      }
    }
    const at = Date.now();
    const turnId = this.#turnId ?? "turn-0";
    switch (slim.type) {
      case "start":
        break;
      case "text_start": {
        const row = this.#rowFactory.assistantText({
          turnId,
          at,
          seq: this.seq + 1,
          state: "streaming",
          model: this.config.model,
          responseId: this.#responseId,
        });
        this.#pushRow(row);
        this.#contentRows.set(slim.contentIndex, row);
        this.#emit([{ op: "row.appended", row }]);
        break;
      }
      case "text_delta": {
        const row = this.#contentRows.get(slim.contentIndex);
        if (row) {
          // Some providers never emit text_end (deepseek/anthropic-compat);
          // the row must own its text, not just stream it.
          row.text += slim.delta;
          this.#emit([{ op: "row.delta", rowId: row.rowId, path: "text", append: slim.delta }]);
        }
        break;
      }
      case "text_end": {
        const row = this.#contentRows.get(slim.contentIndex);
        if (row) {
          // Providers without text_end leave us the accumulated deltas;
          // when both exist, the fuller version wins.
          row.text = (slim.content?.length ?? 0) > row.text.length ? slim.content : row.text;
          row.state = "complete";
          this.#contentRows.delete(slim.contentIndex);
          this.#emit([{ op: "row.upserted", row }]);
        }
        break;
      }
      case "thinking_start": {
        const row = this.#rowFactory.reasoning({
          turnId,
          at,
          seq: this.seq + 1,
          state: "streaming",
          responseId: this.#responseId,
        });
        this.#pushRow(row);
        this.#contentRows.set(slim.contentIndex, row);
        this.#emit([{ op: "row.appended", row }]);
        break;
      }
      case "thinking_delta": {
        const row = this.#contentRows.get(slim.contentIndex);
        if (row) {
          row.text += slim.delta;
          this.#emit([{ op: "row.delta", rowId: row.rowId, path: "text", append: slim.delta }]);
        }
        break;
      }
      case "thinking_end": {
        const row = this.#contentRows.get(slim.contentIndex);
        if (row) {
          row.text = (slim.content?.length ?? 0) > row.text.length ? slim.content : row.text;
          row.state = "complete";
          this.#contentRows.delete(slim.contentIndex);
          this.#emit([{ op: "row.upserted", row }]);
        }
        break;
      }
      case "toolcall_start": {
        const row = this.#rowFactory.toolCall({
          turnId,
          at,
          seq: this.seq + 1,
          toolCallId: slim.id ?? `pending-${slim.contentIndex}`,
          toolName: slim.toolName ?? "tool",
          status: "inputStreaming",
          responseId: this.#responseId,
        });
        this.#pushRow(row);
        this.#contentRows.set(slim.contentIndex, row);
        if (slim.id) this.#toolRows.set(slim.id, row);
        this.#emit([{ op: "row.appended", row }]);
        break;
      }
      case "toolcall_delta": {
        const row = this.#contentRows.get(slim.contentIndex);
        if (row) {
          row.inputText += slim.delta;
          this.#emit([
            { op: "row.delta", rowId: row.rowId, path: "inputText", append: slim.delta },
          ]);
        }
        break;
      }
      case "toolcall_end": {
        const row = this.#contentRows.get(slim.contentIndex);
        const toolCall = slim.toolCall ?? {};
        if (row) {
          row.toolCallId = toolCall.id ?? row.toolCallId;
          row.toolName = toolCall.name ?? row.toolName;
          row.input = toolCall.arguments;
          row.inputText = toolInputText(toolCall.arguments ?? row.inputText);
          row.status = "running";
          row.startedAt = at;
          this.#contentRows.delete(slim.contentIndex);
          if (toolCall.id) this.#toolRows.set(toolCall.id, row);
          this.#emit([{ op: "row.upserted", row }]);
        }
        break;
      }
      case "done":
        break;
      case "error": {
        this.#turnHadError = true;
        const message =
          typeof slim.error === "string" ? slim.error : (slim.reason ?? "model request failed");
        const ops = [];
        for (const row of this.#contentRows.values()) {
          if (row.state === "streaming") {
            row.state = "failed";
            ops.push({ op: "row.upserted", row });
          }
        }
        this.#contentRows.clear();
        this.control = {
          ...this.control,
          lastError: {
            code: slim.reason ?? "provider_error",
            message,
            recoverable: true,
            at,
            source: "provider",
          },
        };
        ops.push(this.#statePatch({ control: this.control }));
        this.#emit(ops);
        break;
      }
      default:
        break;
    }
  }

  #finalizeStreamingRows(message) {
    const ops = [];
    // Fill any still-empty streaming rows from the final assistant message's
    // content blocks (providers that skip text_end/thinking_end entirely).
    const textBlocks = [];
    const thinkingBlocks = [];
    if (message && Array.isArray(message.content)) {
      for (const block of message.content) {
        if (block?.type === "text" && block.text) textBlocks.push(block.text);
        if (block?.type === "thinking" && block.thinking) thinkingBlocks.push(block.thinking);
      }
    }
    let textCursor = 0;
    let thinkingCursor = 0;
    for (const [index, row] of this.#contentRows) {
      if (row.state !== "streaming") {
        this.#contentRows.delete(index);
        continue;
      }
      if (!row.text) {
        if (row.kind === "assistantText" && textCursor < textBlocks.length) {
          row.text = textBlocks[textCursor++];
        } else if (row.kind === "reasoning" && thinkingCursor < thinkingBlocks.length) {
          row.text = thinkingBlocks[thinkingCursor++];
        }
      } else {
        if (row.kind === "assistantText") textCursor++;
        if (row.kind === "reasoning") thinkingCursor++;
      }
      row.state = "complete";
      ops.push({ op: "row.upserted", row });
      this.#contentRows.delete(index);
    }
    if (message?.errorMessage) {
      this.#turnHadError = true;
      this.control = {
        ...this.control,
        lastError: {
          code: "assistant_error",
          message: String(message.errorMessage).slice(0, 500),
          recoverable: true,
          at: Date.now(),
          source: "provider",
        },
      };
      ops.push(this.#statePatch({ control: this.control }));
    }
    if (this.usage.contextWindow || this.#assistantUsageSeen) {
      ops.push(this.#statePatch({ usage: this.usage }));
    }
    if (ops.length) this.#emit(ops);
  }

  #handleToolExecutionStart(event) {
    const row = this.#toolRows.get(event.toolCallId);
    if (!row) return;
    row.status = "running";
    row.startedAt = Date.now();
    if (event.args !== undefined) {
      row.input = event.args;
      if (!row.inputText) row.inputText = toolInputText(event.args);
    }
    this.#emit([{ op: "row.upserted", row }]);
  }

  #handleToolExecutionEnd(event) {
    const row = this.#toolRows.get(event.toolCallId);
    if (!row) return;
    row.status = event.isError ? "error" : "success";
    row.endedAt = Date.now();
    const text = toolResultText(event.result ?? {});
    if (text) row.output = { text };
    if (event.isError) {
      row.error = { code: "tool_error", message: previewText(text) ?? "tool failed" };
    }
    this.#emit([{ op: "row.upserted", row }]);
  }

  #finishTurn(state, ops = []) {
    const at = Date.now();
    if (this.#turnHeaderRow && this.#turnOpen) {
      this.#turnHeaderRow.state = state;
      this.#turnHeaderRow.endedAt = at;
      ops.push({ op: "row.upserted", row: this.#turnHeaderRow });
    }
    this.#turnOpen = false;
    this.streaming = false;
    this.control = {
      ...this.control,
      phase: state === "completedInterrupted" ? "completedInterrupted" : this.#turnHadError ? "error" : "completedSuccess",
      sessionEnded: false,
      canStop: false,
      stopState: "idle",
      activeWorks: [],
    };
    ops.push(this.#statePatch({ control: this.control, usage: this.usage }));
    this.#emit(ops);
    this.#bridge.notifySessionChanged(this);
  }

  #handleTurnEnd() {
    if (!this.#turnOpen) return;
    this.#finishTurn(this.#turnHadError ? "failed" : "completedSuccess");
  }

  #handleAgentEnd() {
    if (this.#turnOpen) {
      this.#finishTurn(this.#turnHadError ? "failed" : "completedSuccess");
      return;
    }
    this.streaming = false;
    this.control = {
      ...this.control,
      phase: this.rows.length ? this.control.phase === "draft" ? "completedSuccess" : this.control.phase : "draft",
      canStop: false,
      stopState: "idle",
      activeWorks: [],
    };
    this.#emit([this.#statePatch({ control: this.control })]);
  }

  #handlePiExit() {
    if (this.#disposed) return;
    this.control = {
      ...this.control,
      phase: "error",
      canStop: false,
      stopState: "idle",
      activeWorks: [],
      lastError: {
        code: "runtime_exit",
        message: "pi-rs process exited",
        recoverable: true,
        at: Date.now(),
        source: "runtime",
      },
    };
    this.streaming = false;
    this.#emit([this.#statePatch({ control: this.control })]);
  }

  async dispose() {
    this.#disposed = true;
    if (this.#flushTimer) clearTimeout(this.#flushTimer);
    if (this.#stopTimer) clearTimeout(this.#stopTimer);
    this.#rpc?.kill();
  }
}
