/**
 * digest 奇偶校验：bridge hooks.mjs 的 发现/摘要 必须与上游
 * @zcode/shared workspace-hook-discovery 逐字节一致——任何分叉都会让
 * 既有信任记录静默失效（安全边界）。
 *
 * 运行：<repo-root>/node_modules/.bin/tsx packages/tack-agent/test/hooks-parity.mts
 */
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import assert from "node:assert/strict";

import {
  readWorkspaceHookProjectSources,
  resolveWorkspaceHookRuntimeRoot,
  buildWorkspaceHookBundleSnapshot,
  workspaceHooksConfigSchema,
} from "../../shared/src/workspace-hook-discovery.js";

import {
  readProjectHookSources,
  buildBundleSnapshot,
} from "../src/hooks.mjs";

const root = mkdtempSync(join(tmpdir(), "hook-parity-"));
const ws = join(root, "ws");
const nested = join(ws, "sub", "deeper");
const home = join(root, "home");
mkdirSync(join(ws, ".git"), { recursive: true });
mkdirSync(join(ws, ".zcode"), { recursive: true });
mkdirSync(nested, { recursive: true });
mkdirSync(join(home, ".zcode", "cli"), { recursive: true });

const zcodeJson = {
  hooks: {
    enabled: true,
    timeoutMs: 45_000,
    events: {
      SessionStart: [
        { hooks: [{ type: "command", command: "cat ctx.md", statusMessage: "载入上下文" }] },
      ],
      PreToolUse: [
        {
          matcher: "Bash|Edit",
          hooks: [
            { type: "command", command: "check.sh", timeout: 30, async: true, shell: "zsh" },
            { type: "command", command: "audit.sh --strict", enabled: false },
          ],
        },
        { matcher: "Write", hooks: [{ type: "command", command: "write-guard.sh" }] },
      ],
      PostToolUseFailure: [
        {
          hooks: [
            { type: "process", command: "/usr/bin/logger", args: ["-t", "hook", "--x y"] },
          ],
        },
      ],
      Stop: [{ hooks: [{ type: "command", command: "notify.sh", timeoutMs: 5_000 }] }],
    },
  },
  otherKey: { ignored: true },
};
const projectConfig = {
  hooks: {
    enabled: false,
    maxOutputBytes: 1024,
    events: {
      UserPromptSubmit: [
        {
          matcher: ".*",
          hooks: [{ type: "command", command: "censor.sh", enabled: true, timeout: 3 }],
        },
      ],
    },
  },
};
const userConfig = {
  storage: { dir: "~/custom-store" },
  hooks: { enabled: true, events: {} },
};
writeFileSync(join(ws, "zcode.json"), JSON.stringify(zcodeJson));
writeFileSync(join(ws, ".zcode", "config.json"), JSON.stringify(projectConfig));
writeFileSync(join(home, ".zcode", "cli", "config.json"), JSON.stringify(userConfig));

try {
  // ── 上游路径 ──
  const upstreamDiscovery = await readWorkspaceHookProjectSources({ workingDirectory: nested });
  const upstreamUserHooks = (() => {
    const parsed = workspaceHooksConfigSchema.safeParse(userConfig.hooks);
    return parsed.success ? parsed.data : undefined;
  })();
  const upstreamRuntimeRoot = resolveWorkspaceHookRuntimeRoot([
    upstreamUserHooks,
    ...upstreamDiscovery.sources.map((s) => s.hooks),
  ]);
  const upstreamSnapshot = buildWorkspaceHookBundleSnapshot({
    workspaceIdentity: ws,
    workspacePath: ws,
    sources: upstreamDiscovery.sources,
    runtimeRoot: upstreamRuntimeRoot,
  });

  // ── bridge 路径（从嵌套 cwd 发现）──
  const bridgeDiscovery = readProjectHookSources({ workingDirectory: nested });
  const bridgeSnapshot = buildBundleSnapshot({
    workspaceIdentity: ws,
    workspacePath: ws,
    sources: bridgeDiscovery.sources,
    runtimeRoot: upstreamRuntimeRoot,
  });

  assert.equal(bridgeDiscovery.errors.length, 0, `bridge errors: ${JSON.stringify(bridgeDiscovery.errors)}`);
  assert.equal(
    bridgeDiscovery.sources.length,
    upstreamDiscovery.sources.length,
    "source count mismatch",
  );
  for (const [i, source] of upstreamDiscovery.sources.entries()) {
    const mine = bridgeDiscovery.sources[i];
    assert.equal(mine.canonicalPath, source.canonicalPath, `sources[${i}].canonicalPath`);
    assert.equal(mine.baseDir, source.baseDir, `sources[${i}].baseDir`);
    assert.equal(mine.discoveryOrder, source.discoveryOrder, `sources[${i}].discoveryOrder`);
    assert.equal(mine.configFileKind, source.configFileKind, `sources[${i}].configFileKind`);
    assert.equal(mine.explicitProjectConfig, source.explicitProjectConfig);
    assert.equal(mine.editable, source.editable, `sources[${i}].editable`);
    assert.deepEqual(mine.hooks, source.hooks, `sources[${i}].hooks`);
  }

  assert.ok(upstreamSnapshot, "upstream snapshot missing");
  assert.ok(bridgeSnapshot, "bridge snapshot missing");
  assert.equal(
    bridgeSnapshot.bundleDigest,
    upstreamSnapshot.bundleDigest,
    "bundleDigest mismatch",
  );
  assert.equal(bridgeSnapshot.hooks.length, upstreamSnapshot.hooks.length, "entry count");
  for (const [i, entry] of upstreamSnapshot.hooks.entries()) {
    const mine = bridgeSnapshot.hooks[i];
    assert.equal(mine.reviewItemId, entry.reviewItemId, `hooks[${i}].reviewItemId`);
    assert.equal(
      mine.hookDeclarationDigest,
      entry.hookDeclarationDigest,
      `hooks[${i}].hookDeclarationDigest (${entry.event} ${entry.command})`,
    );
    assert.equal(mine.configuredEnabled, entry.configuredEnabled, `hooks[${i}].gates`);
    assert.equal(mine.resolvedTimeoutMs, entry.resolvedTimeoutMs, `hooks[${i}].timeout`);
    assert.equal(mine.matcher, entry.matcher, `hooks[${i}].matcher`);
  }
  console.log(
    `PARITY-OK: bundle=${upstreamSnapshot.bundleDigest.slice(0, 12)} entries=${upstreamSnapshot.hooks.length}`,
  );
} finally {
  rmSync(root, { recursive: true, force: true });
}
