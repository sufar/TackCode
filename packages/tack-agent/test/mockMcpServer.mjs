// Minimal stdio MCP server for the tack-agent smoke test: NDJSON JSON-RPC,
// one "echo" tool, empty resources/prompts. Speaks just enough of the
// protocol for pi-rs's connect probe (initialize → tools/list → capability
// probes degrade to empty lists).
import readline from "node:readline";

const serverName = process.argv.includes("--name")
  ? process.argv[process.argv.indexOf("--name") + 1]
  : "smoke-mcp";

const rl = readline.createInterface({ input: process.stdin });

function send(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

rl.on("line", (line) => {
  if (!line.trim()) return;
  let request;
  try {
    request = JSON.parse(line);
  } catch {
    return;
  }
  const { id, method, params } = request;
  // Notifications have no id → never respond.
  if (id === undefined || id === null) return;
  switch (method) {
    case "initialize":
      send({
        jsonrpc: "2.0",
        id,
        result: {
          protocolVersion: params?.protocolVersion ?? "2024-11-05",
          capabilities: { tools: {} },
          serverInfo: { name: serverName, version: "0.0.1" },
        },
      });
      return;
    case "ping":
      send({ jsonrpc: "2.0", id, result: {} });
      return;
    case "tools/list":
      send({
        jsonrpc: "2.0",
        id,
        result: {
          tools: [
            {
              name: "echo",
              description: "Echoes the input text",
              inputSchema: {
                type: "object",
                properties: { text: { type: "string" } },
              },
            },
          ],
        },
      });
      return;
    case "tools/call":
      send({
        jsonrpc: "2.0",
        id,
        result: {
          content: [{ type: "text", text: `echo: ${params?.arguments?.text ?? ""}` }],
          isError: false,
        },
      });
      return;
    case "resources/list":
      send({ jsonrpc: "2.0", id, result: { resources: [] } });
      return;
    case "resources/templates/list":
      send({ jsonrpc: "2.0", id, result: { resourceTemplates: [] } });
      return;
    case "prompts/list":
      send({ jsonrpc: "2.0", id, result: { prompts: [] } });
      return;
    default:
      send({
        jsonrpc: "2.0",
        id,
        error: { code: -32601, message: `method not found: ${method}` },
      });
  }
});
