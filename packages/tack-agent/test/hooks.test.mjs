/**
 * hooks.mjs 单元测试（node --test test/hooks.test.mjs）：
 * schema 严格性 / matcher 归一化 / pi 翻译 / 信任存储读写 / provision 过滤 / grant 流程。
 */
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  parseWorkspaceHooksConfig,
  normalizeMatcher,
  entriesToPiHooks,
  provisionWorkspaceHooks,
  grantWorkspaceHookTrustFlow,
  revokeWorkspaceHookTrustFlow,
  grantTrustRecords,
  revokeTrustRecords,
  loadTrustStore,
  readProjectHookSources,
  buildBundleSnapshot,
} from "../src/hooks.mjs";

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "hook-unit-"));
  const ws = path.join(root, "ws");
  const home = path.join(root, "home");
  fs.mkdirSync(path.join(ws, ".git"), { recursive: true });
  fs.mkdirSync(path.join(ws, ".zcode"), { recursive: true });
  fs.mkdirSync(path.join(home, ".zcode", "cli"), { recursive: true });
  return { root, ws, home };
}

function writeHooks(ws, hooks) {
  fs.writeFileSync(path.join(ws, ".zcode", "config.json"), JSON.stringify({ hooks }));
}

/** 走 hooks.mjs 自身 API 重建 snapshot（digest 输入与 provision 完全一致）。 */
function snapshotOf(ws) {
  return buildBundleSnapshot({
    workspaceIdentity: ws,
    workspacePath: ws,
    sources: readProjectHookSources({ workingDirectory: ws }).sources,
    runtimeRoot: { enabled: true, timeoutMs: 60_000, maxOutputBytes: 32_768 },
  });
}

const SIMPLE_HOOKS = {
  enabled: true,
  events: {
    PreToolUse: [
      { matcher: "Bash", hooks: [{ type: "command", command: "guard.sh", timeout: 10 }] },
    ],
  },
};

test("parseWorkspaceHooksConfig rejects strict-key violations", () => {
  assert.equal(parseWorkspaceHooksConfig({ enabled: true, bogus: 1 }), null);
  assert.equal(parseWorkspaceHooksConfig({ events: { Unknown: [] } }), null);
  assert.equal(
    parseWorkspaceHooksConfig({ events: { Stop: [{ hooks: [], extra: 1 }] } }),
    null,
    "matcher group is strict",
  );
  assert.equal(parseWorkspaceHooksConfig({ events: { Stop: [{ hooks: [] }] } }), null);
  assert.equal(parseWorkspaceHooksConfig({ timeoutMs: -5 }), null);
  const ok = parseWorkspaceHooksConfig({
    enabled: true,
    events: {
      Stop: [
        {
          hooks: [{ type: "command", command: "x.sh", unknownPassthroughKey: 1, async: false }],
        },
      ],
    },
  });
  assert.ok(ok, "hook definitions are passthrough");
  assert.equal(ok.events.Stop[0].hooks[0].unknownPassthroughKey, undefined);
});

test("normalizeMatcher maps Claude names to pi-rs names", () => {
  assert.equal(normalizeMatcher("Bash"), "bash");
  assert.equal(normalizeMatcher("Bash|Edit"), "bash|edit");
  assert.equal(normalizeMatcher("WebFetch|TodoWrite"), "web_fetch|todo");
  assert.equal(normalizeMatcher("ApplyPatch"), "edit|write");
  assert.equal(normalizeMatcher("Task|Agent"), "subagent");
  assert.equal(normalizeMatcher("mcp__x__y"), "mcp__x__y");
  assert.equal(normalizeMatcher("^Bash$"), "^Bash$", "regex passes through");
  assert.equal(normalizeMatcher("*"), "*");
  assert.equal(normalizeMatcher("Write|write"), "write", "dedupes after mapping");
});

test("entriesToPiHooks translates and groups deterministically", () => {
  const piHooks = entriesToPiHooks([
    {
      event: "PreToolUse",
      matcher: "Bash|Edit",
      type: "command",
      command: "a.sh",
      async: true,
      resolvedTimeoutMs: 1500,
      resolvedMaxOutputBytes: 32768,
      statusMessage: "检查中",
    },
    {
      event: "PreToolUse",
      matcher: "Bash|Edit",
      type: "process",
      command: "/bin/echo",
      args: ["it's", "x y"],
      resolvedTimeoutMs: 60_000,
      resolvedMaxOutputBytes: 32768,
    },
    {
      event: "PermissionRequest",
      matcher: null,
      type: "command",
      command: "dropped.sh",
      resolvedTimeoutMs: 60_000,
      resolvedMaxOutputBytes: 32768,
    },
  ]);
  assert.deepEqual(Object.keys(piHooks), ["PreToolUse"], "PermissionRequest dropped");
  const group = piHooks.PreToolUse[0];
  assert.equal(group.matcher, "bash|edit");
  assert.equal(group.hooks.length, 2);
  assert.deepEqual(group.hooks[0], {
    type: "command",
    command: "a.sh",
    timeout: 2,
    async: true,
    statusMessage: "检查中",
  });
  assert.equal(group.hooks[1].command, `'/bin/echo' 'it'\\''s' 'x y'`);
  assert.equal(group.hooks[1].timeout, 60);
  assert.equal(group.hooks[1].async, undefined);
});

test("trust store grant/load/revoke roundtrip + corrupt quarantine", async () => {
  const { root, home } = fixture();
  try {
    const record = {
      workspaceIdentity: "/ws",
      hookDeclarationDigest: "a".repeat(64),
      digestAlgorithm: "sha256",
      decision: "trusted",
      grantedAt: new Date().toISOString(),
      bundleDigestAtGrant: "b".repeat(64),
      eventAtGrant: "Stop",
      displayCommandAtGrant: "notify.sh",
      sourcePathAtGrant: ".zcode/config.json",
      sourceDiscoveryOrderAtGrant: 0,
      matcherAtGrant: null,
      matcherIndexAtGrant: 0,
      hookIndexAtGrant: 0,
    };
    await grantTrustRecords({ homeDir: home }, [record]);
    let loaded = await loadTrustStore({ homeDir: home });
    assert.equal(loaded.status, "ok");
    assert.equal(loaded.records.length, 1);
    assert.equal(loaded.records[0].hookDeclarationDigest, record.hookDeclarationDigest);

    // upsert：同键替换不增行。
    await grantTrustRecords({ homeDir: home }, [
      { ...record, displayCommandAtGrant: "notify2.sh" },
    ]);
    loaded = await loadTrustStore({ homeDir: home });
    assert.equal(loaded.records.length, 1);
    assert.equal(loaded.records[0].displayCommandAtGrant, "notify2.sh");

    await revokeTrustRecords(
      { homeDir: home },
      { workspaceIdentity: "/ws", hookDeclarationDigests: [record.hookDeclarationDigest] },
    );
    loaded = await loadTrustStore({ homeDir: home });
    assert.equal(loaded.records.length, 0);

    // corrupt：坏文件 → 首次接触即 fail-closed（grant 内的 load 检出并隔离）。
    const storePath = loaded.filePath;
    fs.writeFileSync(storePath, "{not json");
    await assert.rejects(
      grantTrustRecords({ homeDir: home }, [record]),
      /workspace_hooks_trust_store_corrupt/,
    );
    assert.equal(fs.existsSync(storePath), false, "quarantined by the failing load");
    loaded = await loadTrustStore({ homeDir: home });
    assert.equal(loaded.status, "missing");
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("provision: 未信任 hook 不下发且计入 pending；信任后下发", async () => {
  const { root, ws, home } = fixture();
  try {
    writeHooks(ws, SIMPLE_HOOKS);
    let result = await provisionWorkspaceHooks({ workspacePath: ws, homeDir: home });
    assert.equal(result.discoveredCount, 1);
    assert.equal(result.pendingCount, 1);
    assert.equal(result.activeCount, 0);
    assert.deepEqual(result.piHooks, {});
    assert.ok(result.bundleDigest);

    // 真实 grant 流程（重新发现 + digest 校验 + 落盘）。
    const digest = snapshotOf(ws).hooks[0].hookDeclarationDigest;
    const granted = await grantWorkspaceHookTrustFlow({
      workspacePath: ws,
      bundleDigest: result.bundleDigest,
      hookDeclarationDigest: digest,
      homeDir: home,
    });
    assert.equal(granted.accepted, true, JSON.stringify(granted));

    result = await provisionWorkspaceHooks({ workspacePath: ws, homeDir: home });
    assert.equal(result.pendingCount, 0);
    assert.equal(result.activeCount, 1);
    assert.deepEqual(result.piHooks, {
      PreToolUse: [
        {
          matcher: "bash",
          hooks: [{ type: "command", command: "guard.sh", timeout: 10 }],
        },
      ],
    });
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("grant 流程错误路径：bundle_changed / snapshot_mismatch / 无配置", async () => {
  const { root, ws, home } = fixture();
  try {
    writeHooks(ws, SIMPLE_HOOKS);
    const good = await provisionWorkspaceHooks({ workspacePath: ws, homeDir: home });
    const digest = snapshotOf(ws).hooks[0].hookDeclarationDigest;

    let r = await grantWorkspaceHookTrustFlow({
      workspacePath: ws,
      bundleDigest: "0".repeat(64),
      hookDeclarationDigest: digest,
      homeDir: home,
    });
    assert.deepEqual(r, { accepted: false, reasonCode: "workspace_hooks_bundle_changed" });

    r = await grantWorkspaceHookTrustFlow({
      workspacePath: ws,
      bundleDigest: good.bundleDigest,
      hookDeclarationDigest: "f".repeat(64),
      homeDir: home,
    });
    assert.deepEqual(r, { accepted: false, reasonCode: "workspace_hooks_snapshot_mismatch" });

    const empty = fixture();
    try {
      r = await grantWorkspaceHookTrustFlow({
        workspacePath: empty.ws,
        bundleDigest: "0".repeat(64),
        hookDeclarationDigest: digest,
        homeDir: home,
      });
      assert.deepEqual(r, { accepted: false, reasonCode: "workspace_hooks_config_unreadable" });
    } finally {
      fs.rmSync(empty.root, { recursive: true, force: true });
    }

    // revoke：撤销已信任记录后 provision 回到 pending。
    const granted = await grantWorkspaceHookTrustFlow({
      workspacePath: ws,
      bundleDigest: good.bundleDigest,
      hookDeclarationDigest: digest,
      homeDir: home,
    });
    assert.equal(granted.accepted, true);
    let provisioned = await provisionWorkspaceHooks({ workspacePath: ws, homeDir: home });
    assert.equal(provisioned.activeCount, 1);
    const revoked = await revokeWorkspaceHookTrustFlow({
      workspacePath: ws,
      hookDeclarationDigests: [digest],
      homeDir: home,
    });
    assert.equal(revoked.accepted, true);
    provisioned = await provisionWorkspaceHooks({ workspacePath: ws, homeDir: home });
    assert.equal(provisioned.activeCount, 0);
    assert.equal(provisioned.pendingCount, 1);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("runtimeRoot.enabled=false 时全部 configuredEnabled=false（不下发不计 pending）", async () => {
  const { root, ws, home } = fixture();
  try {
    writeHooks(ws, { events: SIMPLE_HOOKS.events }); // 无 enabled:true
    const result = await provisionWorkspaceHooks({ workspacePath: ws, homeDir: home });
    assert.equal(result.discoveredCount, 1);
    assert.equal(result.pendingCount, 0);
    assert.deepEqual(result.piHooks, {});
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("storage.dir 覆盖：信任存储跟随用户配置的自定义目录", async () => {
  const { root, ws, home } = fixture();
  try {
    fs.writeFileSync(
      path.join(home, ".zcode", "cli", "config.json"),
      JSON.stringify({ storage: { dir: "~/custom-store" } }),
    );
    writeHooks(ws, SIMPLE_HOOKS);
    const digest = snapshotOf(ws).hooks[0].hookDeclarationDigest;
    const granted = await grantWorkspaceHookTrustFlow({
      workspacePath: ws,
      hookDeclarationDigest: digest,
      homeDir: home,
    });
    assert.equal(granted.accepted, true);
    assert.ok(
      fs.existsSync(path.join(home, "custom-store", "security", "workspace-hook-trust-v1.json")),
      "store written under storage.dir",
    );
    const result = await provisionWorkspaceHooks({ workspacePath: ws, homeDir: home });
    assert.equal(result.activeCount, 1);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
