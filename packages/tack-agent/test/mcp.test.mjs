// Unit tests for the MCP translation layer (node --test test/mcp.test.mjs).
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  SessionMcpStore,
  piStatusToZCodeStatuses,
  protocolMcpServersToPi,
} from "../src/mcp.mjs";

test("protocolMcpServersToPi: non-array → null (host did not override)", () => {
  assert.equal(protocolMcpServersToPi(undefined), null);
  assert.equal(protocolMcpServersToPi(null), null);
  assert.equal(protocolMcpServersToPi("x"), null);
});

test("protocolMcpServersToPi: stdio entry translates env array → map", () => {
  const out = protocolMcpServersToPi([
    {
      name: "local",
      command: "npx",
      args: ["-y", "srv"],
      env: [
        { name: "A", value: "1" },
        { name: "B", value: "2" },
      ],
      timeoutMs: 5000, // no pi equivalent — dropped
    },
  ]);
  assert.deepEqual(out, {
    local: { command: "npx", args: ["-y", "srv"], env: { A: "1", B: "2" } },
  });
});

test("protocolMcpServersToPi: http/sse entries translate headers + oauth", () => {
  const out = protocolMcpServersToPi([
    {
      name: "remote",
      type: "http",
      url: "http://localhost:3000/mcp",
      headers: [{ name: "authorization", value: "Bearer x" }],
      oauth: { type: "authorization_code", clientId: "cid", scope: "read write" },
    },
    { name: "legacy", type: "sse", url: "http://localhost:3001/sse", headers: [] },
    { name: "oauth-true", type: "http", url: "http://localhost:3002/mcp", headers: [], oauth: { type: "client_credentials", clientId: "c", clientSecret: "s" } },
  ]);
  assert.deepEqual(out.remote, {
    type: "http",
    url: "http://localhost:3000/mcp",
    headers: { authorization: "Bearer x" },
    oauth: { clientId: "cid", scopes: ["read", "write"] },
  });
  assert.deepEqual(out.legacy, { type: "sse", url: "http://localhost:3001/sse", headers: {} });
  // client_credentials: secret has no pi-side field; degrade to authorize-on-401.
  assert.deepEqual(out["oauth-true"], {
    type: "http",
    url: "http://localhost:3002/mcp",
    headers: {},
    oauth: { clientId: "c" },
  });
});

test("protocolMcpServersToPi: malformed entries are skipped, not fatal", () => {
  const out = protocolMcpServersToPi([
    { name: "good", command: "npx", args: [], env: [] },
    { name: "" }, // empty name
    { command: "npx" }, // no name
    { name: "neither" }, // neither command nor url
    null,
    "junk",
  ]);
  assert.deepEqual(Object.keys(out), ["good"]);
});

test("piStatusToZCodeStatuses: connected/failed/disconnected mapping", () => {
  const statuses = piStatusToZCodeStatuses([
    { name: "a", transport: "stdio", status: "connected", toolCount: 3 },
    { name: "b", transport: "http", status: "failed", error: "boom" },
    { name: "c", transport: "sse", status: "disconnected" },
  ]);
  assert.equal(statuses.a.status, "connected");
  assert.equal(statuses.a.toolCount, 3);
  assert.equal(statuses.a.transport, "stdio");
  assert.ok(statuses.a.updatedAt);
  assert.equal(statuses.b.status, "failed");
  assert.equal(statuses.b.error, "boom");
  assert.equal(statuses.b.failureKind, "connection_failed");
  assert.equal(statuses.b.toolCount, 0);
  assert.equal(statuses.c.status, "disconnected");
  assert.equal(statuses.c.error, undefined);
});

test("piStatusToZCodeStatuses: failed without error gets a default message", () => {
  const statuses = piStatusToZCodeStatuses([{ name: "x", transport: "stdio", status: "failed" }]);
  assert.equal(statuses.x.error, "connection failed");
});

test("piStatusToZCodeStatuses: unknown transport/status degrade safely", () => {
  const statuses = piStatusToZCodeStatuses([
    { name: "x", transport: "carrier-pigeon", status: "weird" },
  ]);
  assert.equal(statuses.x.transport, "stdio");
  assert.equal(statuses.x.status, "disconnected");
});

test("SessionMcpStore: set → 重新加载 → get（bridge 重启重放路径）", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tack-mcp-store-"));
  const file = path.join(dir, "tack-mcp-servers.json");
  const servers = { "smoke-mcp": { command: "node", args: ["srv.mjs"], env: {} } };
  const store = new SessionMcpStore(file);
  store.set("session-1", servers);
  // 模拟 bridge 重启：新实例从同一文件恢复。
  const reloaded = new SessionMcpStore(file);
  assert.deepEqual(reloaded.get("session-1"), servers);
  fs.rmSync(dir, { recursive: true, force: true });
});

test("SessionMcpStore: delete 持久化 + rekey 换键", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tack-mcp-store-"));
  const file = path.join(dir, "tack-mcp-servers.json");
  const store = new SessionMcpStore(file);
  store.set("a", { s: { command: "x", args: [], env: {} } });
  store.rekey("a", "b"); // fork 换键：新 sessionId 继承配置
  assert.ok(store.get("b"));
  store.delete("a");
  const reloaded = new SessionMcpStore(file);
  assert.equal(reloaded.get("a"), undefined);
  assert.ok(reloaded.get("b"));
  fs.rmSync(dir, { recursive: true, force: true });
});

test("SessionMcpStore: 损坏文件按空档处理（不 throw）", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tack-mcp-store-"));
  const file = path.join(dir, "tack-mcp-servers.json");
  fs.writeFileSync(file, "{not json");
  const store = new SessionMcpStore(file);
  assert.equal(store.get("anything"), undefined);
  fs.rmSync(dir, { recursive: true, force: true });
});
