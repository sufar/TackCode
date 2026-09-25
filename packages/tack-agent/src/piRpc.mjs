// pi-rs `--mode rpc` child process client: JSONL commands in, responses and
// agent events out. Responses are matched by command id; every non-response
// line is an agent event forwarded to onEvent.
import { spawn } from "node:child_process";
import { JsonlReader, JsonlWriter, safeParse } from "./jsonl.mjs";

export class PiRpcClient {
  #child;
  #writer;
  #pending = new Map();
  #nextId = 1;
  #onEvent;
  #onExit;
  #stderrLog;
  #closed = false;

  constructor(child, { onEvent, onExit, onStderr }) {
    this.#child = child;
    this.#onEvent = onEvent;
    this.#onExit = onExit;
    this.#writer = new JsonlWriter(child.stdin);
    let stderrLines = 0;
    this.#stderrLog = (line) => {
      stderrLines += 1;
      if (stderrLines <= 50) onStderr?.(line);
    };
    new JsonlReader(child.stderr, { onLine: (line) => this.#stderrLog(line), onClose: () => {} });
    new JsonlReader(child.stdout, {
      onLine: (line) => this.#handleLine(line),
      onClose: () => this.#handleClose(),
    });
    child.once("error", () => this.#handleClose());
    child.once("exit", () => this.#handleClose());
  }

  get closed() {
    return this.#closed;
  }

  get pid() {
    return this.#child.pid;
  }

  #handleLine(line) {
    const parsed = safeParse(line);
    if (!parsed.ok) {
      this.#stderrLog(`[pi-rs stdout non-json] ${line.slice(0, 200)}`);
      return;
    }
    const message = parsed.value;
    if (message && typeof message === "object" && message.type === "response" && message.id) {
      const pending = this.#pending.get(String(message.id));
      if (pending) {
        this.#pending.delete(String(message.id));
        clearTimeout(pending.timeout);
        if (message.success) pending.resolve(message.data ?? null);
        else pending.reject(new Error(message.error || `pi-rs ${pending.command} failed`));
      }
      return;
    }
    this.#onEvent(message);
  }

  #handleClose() {
    if (this.#closed) return;
    this.#closed = true;
    for (const [id, pending] of this.#pending) {
      clearTimeout(pending.timeout);
      pending.reject(new Error(`pi-rs process exited (pending ${pending.command} ${id})`));
    }
    this.#pending.clear();
    this.#onExit?.();
  }

  request(command, params = {}, { timeoutMs = 120_000 } = {}) {
    if (this.#closed) return Promise.reject(new Error("pi-rs rpc is closed"));
    const id = `rpc-${this.#nextId++}`;
    const payload = { type: command, id, ...params };
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.#pending.delete(id);
        reject(new Error(`pi-rs ${command} timed out`));
      }, timeoutMs);
      this.#pending.set(id, { resolve, reject, timeout, command });
      if (!this.#writer.write(payload)) {
        clearTimeout(timeout);
        this.#pending.delete(id);
        reject(new Error("pi-rs rpc transport is closed"));
      }
    });
  }

  kill() {
    this.#closed = true;
    this.#writer.dispose();
    try {
      this.#child.kill("SIGTERM");
    } catch {
      /* already gone */
    }
    const child = this.#child;
    setTimeout(() => {
      try {
        child.kill("SIGKILL");
      } catch {
        /* already gone */
      }
    }, 3_000).unref();
  }
}

export function spawnPiRpc({
  piBinary,
  cwd,
  env,
  model,
  thinkingLevel,
  onEvent,
  onExit,
  onStderr,
}) {
  const args = ["--mode", "rpc"];
  if (model?.provider && model?.id) {
    args.push("--provider", model.provider, "--model", model.id);
  }
  if (thinkingLevel) {
    args.push("--thinking", thinkingLevel);
  }
  const child = spawn(piBinary, args, {
    cwd,
    env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  return new PiRpcClient(child, { onEvent, onExit, onStderr });
}
