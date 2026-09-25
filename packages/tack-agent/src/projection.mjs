// Conversation row builders and pi-rs message -> v4 row conversion helpers.
// Row invariants honored here (see packages/shared/src/zcode-protocol-v4):
//   - rowId: number, monotonic per session (per logEpoch), never reused
//   - structural changes replace the whole row (row.upserted)
//   - text growth streams via row.delta on ["text" | "inputText" | "output.text"]
//   - frames stay well under the 1 MiB wire cap: tool output is head/tail-cut
const TOOL_TEXT_HEAD = 16 * 1024;
const TOOL_TEXT_TAIL = 16 * 1024;
const TOOL_INPUT_TEXT_MAX = 32 * 1024;
const PREVIEW_MAX = 240;

export function truncateHeadTail(text, head = TOOL_TEXT_HEAD, tail = TOOL_TEXT_TAIL) {
  if (typeof text !== "string") return "";
  if (text.length <= head + tail + 64) return text;
  const omitted = text.length - head - tail;
  return `${text.slice(0, head)}\n… [${omitted} chars omitted] …\n${text.slice(text.length - tail)}`;
}

export function stableStringify(value) {
  try {
    return JSON.stringify(value, null, 2) ?? "";
  } catch {
    return String(value);
  }
}

export function toolInputText(input) {
  const text = typeof input === "string" ? input : stableStringify(input);
  return text.length > TOOL_INPUT_TEXT_MAX ? `${text.slice(0, TOOL_INPUT_TEXT_MAX)}…` : text;
}

/** pi-rs tool result content ([{type:"text",text}|{type:"image"}]) -> text. */
export function contentBlocksText(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .filter((block) => block && block.type === "text" && typeof block.text === "string")
    .map((block) => block.text)
    .join("\n");
}

export function toolResultText(result) {
  if (!result || typeof result !== "object") return "";
  const text = contentBlocksText(result.content);
  return truncateHeadTail(text);
}

export function previewText(text) {
  if (!text) return undefined;
  const flat = text.replace(/\s+/g, " ").trim();
  if (!flat) return undefined;
  return flat.length > PREVIEW_MAX ? `${flat.slice(0, PREVIEW_MAX)}…` : flat;
}

export class RowFactory {
  #nextRowId = 1;

  reset() {
    this.#nextRowId = 1;
  }

  get nextRowId() {
    return this.#nextRowId;
  }

  #base(kind, turnId, createdAt, createdAtSeq) {
    const rowId = this.#nextRowId++;
    return {
      rowId,
      turnId,
      entityId: `row-${rowId}`,
      createdAt,
      createdAtSeq,
    };
  }

  turnHeader({ turnId, origin = "userInput", state = "running", at, seq, sourceCommandId }) {
    return {
      ...this.#base("turnHeader", turnId, at, seq),
      kind: "turnHeader",
      origin,
      executionKind: "agent",
      state,
      startedAt: at,
      ...(sourceCommandId ? { sourceCommandId } : {}),
    };
  }

  userInput({ turnId, text, at, seq, origin = "realUser", sourceCommandId, clientId, guided }) {
    return {
      ...this.#base("userInput", turnId, at, seq),
      kind: "userInput",
      text,
      origin,
      ...(guided ? { guided: true } : {}),
      ...(sourceCommandId ? { sourceCommandId } : {}),
      ...(clientId ? { clientId } : {}),
    };
  }

  assistantText({ turnId, at, seq, state = "streaming", text = "", model, responseId }) {
    return {
      ...this.#base("assistantText", turnId, at, seq),
      kind: "assistantText",
      ...(responseId ? { assistantResponseId: responseId } : {}),
      text,
      state,
      ...(model ? { model } : {}),
    };
  }

  reasoning({ turnId, at, seq, state = "streaming", text = "", responseId }) {
    return {
      ...this.#base("reasoning", turnId, at, seq),
      kind: "reasoning",
      ...(responseId ? { assistantResponseId: responseId } : {}),
      text,
      state,
    };
  }

  toolCall({ turnId, at, seq, toolCallId, toolName, status = "inputStreaming", responseId }) {
    return {
      ...this.#base("toolCall", turnId, at, seq),
      kind: "toolCall",
      ...(responseId ? { assistantResponseId: responseId } : {}),
      toolCallId,
      toolName,
      status,
      inputText: "",
    };
  }

  subagent({ turnId, at, seq, parentToolCallId, subagentType, summaryText, status = "running" }) {
    return {
      ...this.#base("subagent", turnId, at, seq),
      kind: "subagent",
      parentToolCallId,
      subagentType,
      status,
      summaryText,
    };
  }

  timelineMarker({ turnId, at, seq, marker, lane, sourceCommandId }) {
    return {
      ...this.#base("timelineMarker", turnId, at, seq),
      kind: "timelineMarker",
      ...(lane ? { lane } : {}),
      ...(sourceCommandId ? { sourceCommandId } : {}),
      marker,
    };
  }
}
