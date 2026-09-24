// Entry: host-facing stdio JSONL loop. Requests are dispatched to the
// WorkspaceBridge; responses for bridge-originated reverse requests are
// routed back; everything is serialized through one writer.
import { JsonlReader, JsonlWriter, safeParse } from "./jsonl.mjs";
import { ProtocolError, WorkspaceBridge } from "./bridge.mjs";

const ERROR_PARSE = -32700;
const ERROR_INTERNAL = -32603;

export async function main() {
  const writer = new JsonlWriter(process.stdout);
  const send = (message) => writer.write(message);
  const log = (message) => {
    // Diagnostics go to stderr only; stdout is the protocol channel.
    console.error(message);
  };

  const bridge = new WorkspaceBridge({
    send,
    cwd: process.cwd(),
    env: { ...process.env },
    log,
  });

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
        log(`[pi-agent] dispatch error: ${error?.stack ?? error}`);
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
          log(`[pi-agent] ${method} failed: ${error?.stack ?? error}`);
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

  log(`[pi-agent] ready (cwd=${process.cwd()}, pi=${bridge.piBinary})`);
}
