// Smoke test: drives the bridge over stdio the same way the ZCode host does.
// Usage: node test/smoke.mjs [--prompt "say hi"]
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const bridgeBin = path.join(here, "..", "bin", "pi-agent.mjs");
const promptArg = process.argv.includes("--prompt")
  ? process.argv[process.argv.indexOf("--prompt") + 1]
  : null;
const providerArg = process.argv.includes("--provider")
  ? process.argv[process.argv.indexOf("--provider") + 1]
  : null;
const modelArg = process.argv.includes("--model")
  ? process.argv[process.argv.indexOf("--model") + 1]
  : null;
const modelSelection = providerArg && modelArg ? { providerId: providerArg, modelId: modelArg } : undefined;

const child = spawn(process.execPath, [bridgeBin], {
  cwd: process.cwd(),
  env: { ...process.env, PI_AGENT_PI_BINARY: process.env.PI_AGENT_PI_BINARY || "pi-rs" },
  stdio: ["pipe", "pipe", "inherit"],
});

let buffer = "";
const pending = new Map();
let nextId = 1;
const frames = [];

child.stdout.on("data", (chunk) => {
  buffer += chunk;
  let index;
  while ((index = buffer.indexOf("\n")) !== -1) {
    const line = buffer.slice(0, index);
    buffer = buffer.slice(index + 1);
    if (!line.trim()) continue;
    const message = JSON.parse(line);
    if (message.method === "v4/conversation/frame") {
      frames.push(message.params);
      const snapshot = message.params.payload.snapshot;
      const kinds = message.params.payload.kind === "snapshot"
        ? `snapshot(${(snapshot.rows?.window ?? snapshot.sessions ?? snapshot.config) ? "ok" : "empty"})`
        : `deltas(${message.params.payload.deltas.map((d) => d.op).join(",")})`;
      console.log(`  frame ${message.params.topic} seq=(${message.params.fromSeq},${message.params.toSeq}] ${kinds}`);
      continue;
    }
    if (message.id && pending.has(String(message.id))) {
      const { resolve } = pending.get(String(message.id));
      pending.delete(String(message.id));
      resolve(message);
    }
  }
});

function request(method, params) {
  const id = `smoke-${nextId++}`;
  child.stdin.write(`${JSON.stringify({ id, method, params })}\n`);
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    setTimeout(() => {
      if (pending.has(id)) {
        pending.delete(id);
        reject(new Error(`timeout waiting ${method}`));
      }
    }, 90_000);
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function assert(condition, message) {
  if (!condition) {
    console.error(`FAIL: ${message}`);
    process.exit(1);
  }
  console.log(`ok: ${message}`);
}

// 1. readPresentation
const presentation = await request("workspace/readPresentation", {
  workspace: { workspacePath: process.cwd(), workspaceKey: "smoke" },
});
assert(presentation.result?.workspace?.workspacePath === process.cwd(), "readPresentation");

// 2. provider/updateAccountConfig (host pushes this before everything else)
const account = await request("provider/updateAccountConfig", {
  revision: "smoke-rev-1",
  basedOnZCodeBuiltinRevision: "builtin",
  providers: {},
  states: {},
});
assert(account.result?.receivedRevision === "smoke-rev-1", "updateAccountConfig echoes revision");

// 3. sessions-index subscribe
const indexSub = await request("v4/conversation/subscribe", {
  topic: "sessions-index/smoke",
  connectionId: "conn-1",
  clientMode: "desktop-continuous",
});
assert(indexSub.result?.ack?.mode === "snapshot", "sessions-index subscribe ack");
await sleep(200);
assert(
  frames.some((f) => f.topic === "sessions-index/smoke" && f.payload.kind === "snapshot"),
  "sessions-index initial snapshot frame",
);

// 4. workspace-config subscribe
const configSub = await request("v4/conversation/subscribe", {
  topic: "workspace-config/smoke",
  connectionId: "conn-1",
  clientMode: "desktop-continuous",
});
assert(configSub.result?.ack?.subscriptionId, "workspace-config subscribe ack");
await sleep(200);

// 5. createSession (draft)
const create = await request("v4/command", {
  commandId: crypto.randomUUID(),
  clientId: "smoke-client",
  sessionId: null,
  type: "createSession",
  payload: { workspaceId: "smoke" },
  issuedAt: Date.now(),
});
const sessionId = create.result?.result?.sessionId;
assert(create.result?.status === "accepted" && sessionId, `createSession accepted (${sessionId})`);

// 6. conversation subscribe
const convSub = await request("v4/conversation/subscribe", {
  topic: `conversation/${sessionId}`,
  connectionId: "conn-1",
  clientMode: "desktop-continuous",
});
assert(convSub.result?.ack?.subscriptionId, "conversation subscribe ack");
await sleep(200);
const convSnapshot = frames.find(
  (f) => f.topic === `conversation/${sessionId}` && f.payload.kind === "snapshot",
);
assert(convSnapshot, "conversation initial snapshot frame");
assert(
  convSnapshot.payload.snapshot.seq === convSnapshot.toSeq && convSnapshot.fromSeq === 0,
  "snapshot seq invariant (snapshot.seq == frame.toSeq, fromSeq == 0)",
);

// 7. optional live prompt
if (promptArg) {
  const framesBefore = frames.length;
  const send = await request("v4/command", {
    commandId: crypto.randomUUID(),
    clientId: "smoke-client",
    sessionId,
    type: "sendText",
    payload: { text: promptArg, ...(modelSelection ? { modelSelection } : {}) },
    issuedAt: Date.now(),
  });
  assert(send.result?.status === "accepted", "sendText accepted");
  console.log("  …waiting for the turn to finish (up to 80s)…");
  const deadline = Date.now() + 80_000;
  let done = false;
  while (Date.now() < deadline && !done) {
    await sleep(500);
    done = frames.some(
      (f) =>
        f.topic === `conversation/${sessionId}` &&
        f.payload.kind === "deltas" &&
        f.payload.deltas.some(
          (d) =>
            d.op === "state.updated" &&
            d.patch.control &&
            (d.patch.control.phase === "completedSuccess" ||
              d.patch.control.phase === "error" ||
              d.patch.control.phase === "completedInterrupted"),
        ),
    );
  }
  assert(done, "turn reached a terminal phase");
  const newFrames = frames.slice(framesBefore);
  const ops = newFrames.flatMap((f) =>
    f.payload.kind === "deltas" ? f.payload.deltas.map((d) => d.op) : [],
  );
  console.log(`  ops seen: ${[...new Set(ops)].join(", ")}`);
  assert(ops.includes("row.appended"), "stream emitted row.appended");
  const textDeltas = newFrames.flatMap((f) =>
    f.payload.kind === "deltas"
      ? f.payload.deltas.filter((d) => d.op === "row.delta" && d.path === "text")
      : [],
  );
  const streamedText = textDeltas.map((d) => d.append).join("");
  if (modelSelection) {
    assert(textDeltas.length > 0, "stream emitted assistant text deltas");
    console.log(`  streamed text: ${JSON.stringify(streamedText.slice(0, 120))}`);
  }
  // seq continuity per frame
  let expected;
  for (const f of newFrames.filter((f) => f.topic === `conversation/${sessionId}`)) {
    if (expected === undefined) {
      expected = f.toSeq;
      continue;
    }
    assert(f.fromSeq === expected, `frame seq continuity (fromSeq=${f.fromSeq} == ${expected})`);
    expected = f.toSeq;
  }
}

console.log("\nSMOKE PASS");
child.kill("SIGTERM");
process.exit(0);
