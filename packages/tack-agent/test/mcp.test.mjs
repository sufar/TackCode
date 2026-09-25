// Unit tests for the MCP translation layer (node --test test/mcp.test.mjs).
import test from "node:test";
import assert from "node:assert/strict";
import { piStatusToZCodeStatuses, protocolMcpServersToPi } from "../src/mcp.mjs";

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
