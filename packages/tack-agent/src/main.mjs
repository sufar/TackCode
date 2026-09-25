// Entry: host-facing stdio JSONL loop. Requests are dispatched to the
// WorkspaceBridge; responses for bridge-originated reverse requests are
// routed back; everything is serialized through one writer.
import { JsonlReader, JsonlWriter, safeParse } from "./jsonl.mjs";
import { ProtocolError, WorkspaceBridge } from "./bridge.mjs";
import { reportBootStorageReady, runPrepareStorage } from "./storageStartup.mjs";
import fs from "node:fs";

const ERROR_PARSE = -32700;
const ERROR_INTERNAL = -32603;

export async function main(argv = []) {
  const logFile = process.env.TACK_AGENT_LOG_FILE;
  const log = (message) => {
    // Diagnostics go to stderr only; stdout is the protocol channel.
    console.error(message);
    if (logFile) {
      try {
        fs.appendFileSync(logFile, `${new Date().toISOString()} ${message}\n`);
      } catch {
        /* ignore */
      }
    }
  };

  // One-shot storage preparation mode (host worker: app-server --stdio
  // --prepare-storage --cwd <dir>).
  if (argv.includes("--prepare-storage")) {
    const cwdIndex = argv.indexOf("--cwd");
    const cwd = cwdIndex !== -1 && argv[cwdIndex + 1] ? argv[cwdIndex + 1] : process.cwd();
    await runPrepareStorage({
      cwd,
      env: { ...process.env },
      input: process.stdin,
      output: process.stdout,
      log,
    });
    // The host resolves preparation on our exit code; do not linger on stray
    // event-loop handles after the final frame has been flushed.
    process.exit(process.exitCode ?? 0);
  }

  const writer = new JsonlWriter(process.stdout);
  const send = (message) => writer.write(message);

  const bridge = new WorkspaceBridge({
    send,
    cwd: process.cwd(),
    env: { ...process.env },
    log,
  });

  // The host gates requests on agent-owned storage readiness when the command
  // declares supportsStorageStartup (mirrored via TACK_AGENT_STORAGE_STARTUP=1).
  if (process.env.TACK_AGENT_STORAGE_STARTUP === "1") {
    await reportBootStorageReady({
      cwd: process.cwd(),
      env: { ...process.env },
      output: process.stdout,
    });
  }

  const reader = new JsonlReader(process.stdin, {
    onLine: (line) => {
      const parsed = safeParse(line);
      if (!parsed.ok) {
        send({
          id: "unknown",
          error: { code: ERROR_PARSE, message: "Parse error" },
        });
        return;
      }
      handleMessage(parsed.value).catch((error) => {
        log(`[tack-agent] dispatch error: ${error?.stack ?? error}`);
      });
    },
    onClose: () => shutdown(0),
  });
  void reader;

  async function handleMessage(message) {
    if (!message || typeof message !== "object") return;
    const hasId = Object.hasOwn(message, "id");
    const hasMethod = typeof message.method === "string";
    if (hasId && !hasMethod) {
      // Response to one of our reverse requests.
      bridge.handleResponse(message);
      return;
    }
    if (hasId && hasMethod) {
      const { id, method, params } = message;
      try {
        const result = await bridge.handleRequest(method, params);
        send({ id, result: result ?? {} });
      } catch (error) {
        if (error instanceof ProtocolError) {
          send({ id, error: { code: error.code, message: error.message, ...(error.data !== undefined ? { data: error.data } : {}) } });
        } else {
          log(`[tack-agent] ${method} failed: ${error?.stack ?? error}`);
          send({
            id,
            error: {
              code: ERROR_INTERNAL,
              message: error instanceof Error ? error.message : String(error),
            },
          });
        }
      }
      return;
    }
    if (hasMethod) {
      bridge.handleNotification(message.method, message.params);
    }
  }

  let shuttingDown = false;
  async function shutdown(code) {
    if (shuttingDown) return;
    shuttingDown = true;
    await bridge.dispose().catch(() => {});
    process.exit(code);
  }

  process.once("SIGTERM", () => shutdown(0));
  process.once("SIGINT", () => shutdown(0));

  log(`[tack-agent] ready (cwd=${process.cwd()}, pi=${bridge.piBinary})`);
}
