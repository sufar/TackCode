/**
 * ZCode workspace hooks → pi-rs 桥接（零依赖移植，契约逐字对齐上游）。
 *
 * 上游单一权威（改动必须与之一致，否则 digest 分叉 = 既有信任记录全部失效）：
 *   packages/shared/src/workspace-hook-config.ts   发现/schema/闸门/常量
 *   packages/shared/src/workspace-hook-digest.ts   declaration/bundle digest
 *   packages/shared/src/workspace-hook-trust-store-file.ts  store 文件 schema
 *   apps/zcode-cli/packages/adapters/src/storage/workspace-hook-trust-store.ts  写路径
 *
 * 覆盖范围（MVP）：
 *   ① 项目 bundle（zcode.json/.zcode/config.json 沿目录树上溯，trust-gated）
 *   ② 用户 zcode hooks（~/.zcode/cli/config.json，免信任，同 enabled 闸门）
 *   legacy .agents/.claude settings.json 只做 UI 展示，不下发执行（后续跟进）。
 *
 * 信任模型：bridge 只把「已持久信任 + configuredEnabled」的项目 hook 翻译下发
 * pi-rs（pi-rs fail-open，无信任概念——信任裁决全部在 bridge 侧完成）。
 */
import { createHash } from "node:crypto";
import { existsSync, readFileSync, statSync } from "node:fs";
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";

// ── 常量（workspace-hook-config.ts）────────────────────────────────────────

const DIGEST_SCHEMA_VERSION = 1;
const TRUST_STORE_SCHEMA_VERSION = 1;
const DEFAULT_TIMEOUT_MS = 60_000;
const DEFAULT_MAX_OUTPUT_BYTES = 32_768;
const EVENT_NAMES = [
  "SessionStart",
  "UserPromptSubmit",
  "PreToolUse",
  "PermissionRequest",
  "PostToolUse",
  "PostToolUseFailure",
  "Stop",
];
const EVENT_NAME_SET = new Set(EVENT_NAMES);
/** pi-rs RPC 模式当前接线的事件；PermissionRequest 不下发（无对应执行点）。 */
const PI_FORWARDED_EVENTS = new Set([
  "SessionStart",
  "UserPromptSubmit",
  "PreToolUse",
  "PostToolUse",
  "PostToolUseFailure",
  "Stop",
]);

const TRUST_RECORD_KEY_PATTERN = /^[a-f0-9]{64}$/u;

// ── 小工具 ──────────────────────────────────────────────────────────────────

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNodeError(error, code) {
  return error instanceof Error && "code" in error && error.code === code;
}

function isPositiveNumber(value) {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

function sha256(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function resolveHome(homeDir) {
  const envHome = process.env.HOME?.trim() || process.env.USERPROFILE?.trim();
  return resolve(homeDir ?? envHome ?? homedir());
}

// ── 配置 schema（workspaceHooksConfigSchema 的手写镜像，fail-closed）────────
//
// 上游：root/events 严格键，hook 定义 passthrough（额外键保留但 digest 不覆盖——
// canonicalDeclarationPayload 只取具名字段，这里同样只读取具名字段）。

function parseHookDefinition(raw) {
  if (!isRecord(raw) || typeof raw.command !== "string" || raw.command.length < 1) {
    return null;
  }
  const type = raw.type ?? "command";
  const enabled = raw.enabled === undefined ? undefined : raw.enabled === true;
  if (raw.enabled !== undefined && typeof raw.enabled !== "boolean") return null;
  const timeoutMs = raw.timeoutMs === undefined ? undefined : raw.timeoutMs;
  if (timeoutMs !== undefined && !isPositiveNumber(timeoutMs)) return null;
  const statusMessage = raw.statusMessage === undefined ? undefined : raw.statusMessage;
  if (
    statusMessage !== undefined &&
    (typeof statusMessage !== "string" || statusMessage.length < 1)
  ) {
    return null;
  }
  if (type === "process") {
    if (raw.args !== undefined && !Array.isArray(raw.args)) return null;
    const args = raw.args?.map((item) => (typeof item === "string" ? item : null));
    if (args?.some((item) => item === null)) return null;
    return {
      type: "process",
      command: raw.command,
      ...(enabled !== undefined ? { enabled } : {}),
      ...(args !== undefined ? { args } : {}),
      ...(timeoutMs !== undefined ? { timeoutMs } : {}),
      ...(statusMessage !== undefined ? { statusMessage } : {}),
    };
  }
  if (type !== "command") return null;
  const timeout = raw.timeout === undefined ? undefined : raw.timeout;
  if (timeout !== undefined && !isPositiveNumber(timeout)) return null;
  const shell = raw.shell;
  if (shell !== undefined && shell !== true && (typeof shell !== "string" || shell.length < 1)) {
    return null;
  }
  return {
    type: "command",
    command: raw.command,
    ...(enabled !== undefined ? { enabled } : {}),
    ...(raw.async === true ? { async: true } : {}),
    ...(shell !== undefined ? { shell } : {}),
    ...(timeout !== undefined ? { timeout } : {}),
    ...(timeoutMs !== undefined ? { timeoutMs } : {}),
    ...(statusMessage !== undefined ? { statusMessage } : {}),
  };
}

function parseMatcherGroup(raw) {
  // 上游 matcher 组是 strict：只有 matcher/hooks 两个键。
  if (!isRecord(raw)) return null;
  for (const key of Object.keys(raw)) {
    if (key !== "matcher" && key !== "hooks") return null;
  }
  if (raw.matcher !== undefined && (typeof raw.matcher !== "string" || raw.matcher.length < 1)) {
    return null;
  }
  if (!Array.isArray(raw.hooks) || raw.hooks.length < 1) return null;
  const hooks = raw.hooks.map(parseHookDefinition);
  if (hooks.some((hook) => hook === null)) return null;
  return {
    ...(raw.matcher !== undefined ? { matcher: raw.matcher } : {}),
    hooks,
  };
}

/** @returns 解析后的 WorkspaceHooksConfig；结构非法时 null（zod safeParse 语义）。 */
export function parseWorkspaceHooksConfig(raw) {
  if (!isRecord(raw)) return null;
  const allowed = new Set(["enabled", "timeoutMs", "maxOutputBytes", "events"]);
  for (const key of Object.keys(raw)) {
    if (!allowed.has(key)) return null;
  }
  if (raw.enabled !== undefined && typeof raw.enabled !== "boolean") return null;
  if (raw.timeoutMs !== undefined && !isPositiveNumber(raw.timeoutMs)) return null;
  if (raw.maxOutputBytes !== undefined && !isPositiveNumber(raw.maxOutputBytes)) return null;
  const config = {
    ...(raw.enabled !== undefined ? { enabled: raw.enabled } : {}),
    ...(raw.timeoutMs !== undefined ? { timeoutMs: raw.timeoutMs } : {}),
    ...(raw.maxOutputBytes !== undefined ? { maxOutputBytes: raw.maxOutputBytes } : {}),
  };
  if (raw.events !== undefined) {
    if (!isRecord(raw.events)) return null;
    const events = {};
    for (const [name, groups] of Object.entries(raw.events)) {
      if (!EVENT_NAME_SET.has(name) || !Array.isArray(groups)) return null;
      const parsed = groups.map(parseMatcherGroup);
      if (parsed.some((group) => group === null)) return null;
      events[name] = parsed;
    }
    config.events = events;
  }
  return config;
}

// ── 发现（readWorkspaceHookProjectSources 移植，sync IO）────────────────────

function hasWorktreeMarker(directory) {
  const marker = join(directory, ".git");
  try {
    if (!existsSync(marker)) return false;
    const stats = statSync(marker);
    return stats.isDirectory() || stats.isFile();
  } catch {
    return false;
  }
}

function getProjectConfigDirectories(start) {
  const directories = [];
  let current = start;
  while (true) {
    directories.push(current);
    if (hasWorktreeMarker(current)) return directories.reverse();
    const parent = dirname(current);
    if (parent === current) break;
    current = parent;
  }
  return [start];
}

function createSourceInput({ path, workingDirectory, hooks, discoveryOrder }) {
  const canonicalPath = resolve(path);
  const configDirectory = dirname(canonicalPath);
  return {
    canonicalPath,
    baseDir: basename(configDirectory) === ".zcode" ? dirname(configDirectory) : configDirectory,
    discoveryOrder,
    configFileKind: basename(canonicalPath) === "zcode.json" ? "zcode.json" : ".zcode/config.json",
    explicitProjectConfig: false,
    editable: canonicalPath === resolve(workingDirectory, ".zcode", "config.json"),
    hooks,
  };
}

/** @returns {{sources: Array, errors: Array<{path: string, error: unknown}>}} */
export function readProjectHookSources({ workingDirectory }) {
  const start = resolve(workingDirectory);
  const directories = getProjectConfigDirectories(start);
  const candidates = directories.flatMap((directory) => [
    join(directory, "zcode.json"),
    join(directory, ".zcode", "config.json"),
  ]);
  const seen = new Set();
  const refs = [];
  for (const path of candidates) {
    if (!existsSync(path)) continue;
    const resolved = resolve(path);
    if (seen.has(resolved)) continue;
    seen.add(resolved);
    refs.push(resolved);
  }
  const sources = [];
  const errors = [];
  for (const [discoveryOrder, path] of refs.entries()) {
    let value;
    try {
      value = JSON.parse(readFileSync(path, "utf8"));
    } catch (error) {
      errors.push({ path, error });
      continue;
    }
    // 上游：无 hooks 键的文件直接跳过（不算错误）。
    if (!isRecord(value) || value.hooks === undefined) continue;
    const hooks = parseWorkspaceHooksConfig(value.hooks);
    if (hooks === null) {
      errors.push({ path, error: new Error("invalid workspace hooks config") });
      continue;
    }
    sources.push(createSourceInput({ path, workingDirectory: start, hooks, discoveryOrder }));
  }
  return { sources, errors };
}

/** 用户级 zcode hooks（~/.zcode/cli/config.json 的 hooks 键）；非法时按无配置处理。 */
export async function readUserZCodeHooks(homeDir) {
  const path = join(resolveHome(homeDir), ".zcode", "cli", "config.json");
  let value;
  try {
    value = JSON.parse(await readFile(path, "utf8"));
  } catch {
    return undefined;
  }
  if (!isRecord(value) || value.hooks === undefined) return undefined;
  return parseWorkspaceHooksConfig(value.hooks) ?? undefined;
}

// ── 闸门与 runtimeRoot（resolveWorkspaceHookRuntimeRoot / ConfiguredGates）──

function resolveRuntimeRoot(roots) {
  let enabled = false;
  let timeoutMs = DEFAULT_TIMEOUT_MS;
  let maxOutputBytes = DEFAULT_MAX_OUTPUT_BYTES;
  for (const root of roots) {
    if (!root) continue;
    if (root.enabled === true) enabled = true;
    if (root.timeoutMs !== undefined) timeoutMs = root.timeoutMs;
    if (root.maxOutputBytes !== undefined) maxOutputBytes = root.maxOutputBytes;
  }
  return {
    enabled,
    timeoutMs: Math.max(1, Math.round(timeoutMs)),
    maxOutputBytes: Math.max(1, Math.round(maxOutputBytes)),
  };
}

function resolveHookTimeoutMs(hook, defaultTimeoutMs) {
  const timeoutMs =
    hook.timeoutMs ??
    (hook.type === "command" && hook.timeout !== undefined
      ? hook.timeout * 1000
      : defaultTimeoutMs);
  return Math.max(1, Math.round(timeoutMs));
}

function resolveConfiguredGates({ sourceEnabled, declarationEnabled, runtimeHooksEnabled }) {
  const sourceRootEnabled = sourceEnabled !== false;
  const declarationOn = declarationEnabled !== false;
  return {
    sourceRootEnabled,
    declarationEnabled: declarationOn,
    runtimeHooksEnabled,
    configuredEnabled: sourceRootEnabled && declarationOn && runtimeHooksEnabled,
  };
}

// ── digest（workspace-hook-digest.ts 逐字移植；任何改动都会使既有信任失效）──

function normalizeRelativeSourcePath(workspacePath, sourcePath) {
  const value = relative(resolve(workspacePath), resolve(sourcePath)).replaceAll("\\", "/");
  return value || basename(sourcePath);
}

function canonicalOptional(value) {
  return value === undefined ? ["unset"] : ["set", value];
}

function canonicalDeclarationPayload(input) {
  const hook = input.hook;
  const execution =
    hook.type === "process"
      ? ["process", hook.command, [...(hook.args ?? [])]]
      : [
          "command",
          hook.command,
          hook.async === true,
          hook.shell === undefined
            ? ["unset"]
            : hook.shell === true
              ? ["true"]
              : ["string", hook.shell],
        ];
  return [
    "workspace-hook-declaration",
    DIGEST_SCHEMA_VERSION,
    input.sourceRelativePath,
    input.sourceDiscoveryOrder,
    input.event,
    input.matcher,
    input.matcherIndex,
    input.hookIndex,
    execution,
    input.resolvedTimeoutMs,
    input.resolvedMaxOutputBytes,
  ];
}

function createDeclarationDigest(input) {
  return sha256(
    canonicalDeclarationPayload({
      ...input,
      resolvedTimeoutMs: resolveHookTimeoutMs(input.hook, input.defaultTimeoutMs),
    }),
  );
}

function resolveEntries({ workspacePath, sources, runtimeRoot }) {
  const entries = [];
  for (const [sourceFileIndex, source] of sources.entries()) {
    const sourceRelativePath = normalizeRelativeSourcePath(workspacePath, source.canonicalPath);
    for (const event of EVENT_NAMES) {
      for (const [matcherIndex, matcher] of (source.hooks.events?.[event] ?? []).entries()) {
        for (const [hookIndex, hook] of matcher.hooks.entries()) {
          const gates = resolveConfiguredGates({
            sourceEnabled: source.hooks.enabled,
            declarationEnabled: hook.enabled,
            runtimeHooksEnabled: runtimeRoot.enabled,
          });
          const hookDeclarationDigest = createDeclarationDigest({
            sourceRelativePath,
            sourceDiscoveryOrder: source.discoveryOrder,
            event,
            matcher: matcher.matcher ?? null,
            matcherIndex,
            hookIndex,
            hook,
            defaultTimeoutMs: runtimeRoot.timeoutMs,
            resolvedMaxOutputBytes: runtimeRoot.maxOutputBytes,
          });
          entries.push({
            reviewItemId: `workspace-hook-${sourceFileIndex}-${event}-${matcherIndex}-${hookIndex}`,
            event,
            matcherIndex,
            hookIndex,
            sourceFileIndex,
            sourceRelativePath,
            matcher: matcher.matcher ?? null,
            type: hook.type,
            command: hook.command,
            ...(hook.type === "process" && hook.args?.length ? { args: [...hook.args] } : {}),
            ...(hook.type === "command" && hook.async === true ? { async: true } : {}),
            ...(hook.type === "command" && hook.shell !== undefined
              ? { shell: hook.shell }
              : {}),
            resolvedTimeoutMs: resolveHookTimeoutMs(hook, runtimeRoot.timeoutMs),
            resolvedMaxOutputBytes: runtimeRoot.maxOutputBytes,
            ...(hook.statusMessage ? { statusMessage: hook.statusMessage } : {}),
            ...gates,
            editable: source.editable,
            hookDeclarationDigest,
          });
        }
      }
    }
  }
  return entries;
}

export function buildBundleSnapshot({ workspaceIdentity, workspacePath, sources, runtimeRoot }) {
  const hooks = resolveEntries({ workspacePath, sources, runtimeRoot });
  if (hooks.length === 0) return undefined;
  const sourceFiles = sources.map((source) => ({
    canonicalPath: source.canonicalPath,
    baseDir: source.baseDir,
    discoveryOrder: source.discoveryOrder,
    configFileKind: source.configFileKind,
    explicitProjectConfig: source.explicitProjectConfig,
    editable: source.editable,
    hooksRoot: {
      ...(source.hooks.enabled !== undefined ? { enabled: source.hooks.enabled } : {}),
      ...(source.hooks.timeoutMs !== undefined ? { timeoutMs: source.hooks.timeoutMs } : {}),
      ...(source.hooks.maxOutputBytes !== undefined
        ? { maxOutputBytes: source.hooks.maxOutputBytes }
        : {}),
    },
  }));
  const bundlePayload = [
    "workspace-hook-bundle",
    DIGEST_SCHEMA_VERSION,
    sources.map((source) => [
      normalizeRelativeSourcePath(workspacePath, source.canonicalPath),
      source.discoveryOrder,
      source.configFileKind,
      source.explicitProjectConfig,
      canonicalOptional(source.hooks.enabled),
      canonicalOptional(source.hooks.timeoutMs),
      canonicalOptional(source.hooks.maxOutputBytes),
    ]),
    hooks.map((hook) => [
      hook.hookDeclarationDigest,
      hook.sourceRootEnabled,
      hook.declarationEnabled,
      hook.runtimeHooksEnabled,
      hook.configuredEnabled,
    ]),
  ];
  return {
    schemaVersion: DIGEST_SCHEMA_VERSION,
    workspaceIdentity,
    sourceFiles,
    hooks,
    digestAlgorithm: "sha256",
    bundleDigest: sha256(bundlePayload),
  };
}

// ── 信任存储（schema 与 adapters 写路径的最小兼容实现）───────────────────────
//
// 单写者假设：TackCode 下 zcode-cli 不在场，bridge 是 store 的唯一写者；
// 读者（host services loadHooks）由 tmp+rename 原子写保护。上游的跨进程
// 锁协议（.lock + stale 回收）不在此移植。

async function resolveTrustStorePath({ homeDir } = {}) {
  const home = resolveHome(homeDir);
  const userConfigPath = join(home, ".zcode", "cli", "config.json");
  let configured = "";
  try {
    const config = JSON.parse(await readFile(userConfigPath, "utf8"));
    const storage = isRecord(config?.storage) ? config.storage : {};
    configured = typeof storage.dir === "string" ? storage.dir.trim() : "";
  } catch (error) {
    if (!isNodeError(error, "ENOENT")) {
      // 与 adapters readUserConfig 一致：配置不可读是硬错误（调用方映射为
      // workspace_hooks_config_unreadable），不得静默退回默认路径写错文件。
      throw new Error("workspace_hooks_config_unreadable");
    }
  }
  const storageRoot = configured
    ? configured.startsWith("~/")
      ? join(home, configured.slice(2))
      : isAbsolute(configured)
        ? resolve(configured)
        : resolve(home, configured)
    : join(home, ".zcode");
  return join(storageRoot, "security", "workspace-hook-trust-v1.json");
}

function isValidTrustRecord(record) {
  if (!isRecord(record)) return false;
  const allowed = new Set([
    "workspaceIdentity",
    "hookDeclarationDigest",
    "digestAlgorithm",
    "decision",
    "grantedAt",
    "lastUsedAt",
    "bundleDigestAtGrant",
    "eventAtGrant",
    "displayCommandAtGrant",
    "sourcePathAtGrant",
    "sourceDiscoveryOrderAtGrant",
    "matcherAtGrant",
    "matcherIndexAtGrant",
    "hookIndexAtGrant",
    "appVersionAtGrant",
  ]);
  for (const key of Object.keys(record)) {
    if (!allowed.has(key)) return false;
  }
  const nonEmpty = (v) => typeof v === "string" && v.trim().length > 0;
  const nonnegInt = (v) => typeof v === "number" && Number.isInteger(v) && v >= 0;
  const isoDate = (v) => typeof v === "string" && !Number.isNaN(Date.parse(v));
  if (!nonEmpty(record.workspaceIdentity)) return false;
  if (!TRUST_RECORD_KEY_PATTERN.test(record.hookDeclarationDigest ?? "")) return false;
  if (record.digestAlgorithm !== "sha256" || record.decision !== "trusted") return false;
  if (!isoDate(record.grantedAt)) return false;
  if (record.lastUsedAt !== undefined && !isoDate(record.lastUsedAt)) return false;
  if (
    record.bundleDigestAtGrant !== undefined &&
    !TRUST_RECORD_KEY_PATTERN.test(record.bundleDigestAtGrant)
  ) {
    return false;
  }
  if (!EVENT_NAME_SET.has(record.eventAtGrant)) return false;
  if (!nonEmpty(record.displayCommandAtGrant) || !nonEmpty(record.sourcePathAtGrant)) return false;
  if (
    record.sourceDiscoveryOrderAtGrant !== undefined &&
    !nonnegInt(record.sourceDiscoveryOrderAtGrant)
  ) {
    return false;
  }
  if (
    record.matcherAtGrant !== undefined &&
    record.matcherAtGrant !== null &&
    typeof record.matcherAtGrant !== "string"
  ) {
    return false;
  }
  if (record.matcherIndexAtGrant !== undefined && !nonnegInt(record.matcherIndexAtGrant)) {
    return false;
  }
  if (record.hookIndexAtGrant !== undefined && !nonnegInt(record.hookIndexAtGrant)) return false;
  if (record.appVersionAtGrant !== undefined && !nonEmpty(record.appVersionAtGrant)) return false;
  return true;
}

function parseTrustStoreContent(content) {
  let parsed;
  try {
    parsed = JSON.parse(content);
  } catch {
    return { status: "invalid" };
  }
  if (
    !isRecord(parsed) ||
    parsed.schemaVersion !== TRUST_STORE_SCHEMA_VERSION ||
    !Array.isArray(parsed.records) ||
    Object.keys(parsed).some((key) => key !== "schemaVersion" && key !== "records")
  ) {
    return { status: "invalid" };
  }
  const seen = new Set();
  for (const record of parsed.records) {
    if (!isValidTrustRecord(record)) return { status: "invalid" };
    const key = `${record.workspaceIdentity} ${record.hookDeclarationDigest}`;
    if (seen.has(key)) return { status: "invalid" };
    seen.add(key);
  }
  return { status: "ok", records: parsed.records };
}

/**
 * 读取信任存储。损坏时与 adapters 同名语义：尝试隔离改名（best effort），
 * 返回 { status: "corrupt" }——调用方必须 fail-closed（全部按未信任处理，
 * 显式 grant 也必须拒绝）。
 */
export async function loadTrustStore(options = {}) {
  const filePath = await resolveTrustStorePath(options);
  let content;
  try {
    content = await readFile(filePath, "utf8");
  } catch (error) {
    if (isNodeError(error, "ENOENT")) return { status: "missing", records: [], filePath };
    return { status: "corrupt", records: [], filePath };
  }
  const parsed = parseTrustStoreContent(content);
  if (parsed.status === "ok") return { ...parsed, filePath };
  const quarantine = `${filePath}.corrupt-${Date.now()}`;
  await rename(filePath, quarantine).catch(() => undefined);
  return { status: "corrupt", records: [], filePath };
}

async function atomicWriteTrustStore(filePath, records) {
  const directory = dirname(filePath);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await chmod(directory, 0o700).catch(() => undefined);
  const tempPath = join(
    directory,
    `.${basename(filePath)}.${process.pid}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`,
  );
  const store = { schemaVersion: TRUST_STORE_SCHEMA_VERSION, records };
  try {
    await writeFile(tempPath, `${JSON.stringify(store, null, 2)}\n`, {
      encoding: "utf8",
      mode: 0o600,
    });
    await rename(tempPath, filePath);
    await chmod(filePath, 0o600).catch(() => undefined);
  } catch (error) {
    await rm(tempPath, { force: true }).catch(() => undefined);
    throw error;
  }
}

function trustKey(record) {
  return `${record.workspaceIdentity} ${record.hookDeclarationDigest}`;
}

/** upsert（保留既有顺序，重复键原位替换——与 adapters Map 语义一致）。 */
export async function grantTrustRecords(options, records) {
  const loaded = await loadTrustStore(options);
  if (loaded.status === "corrupt") {
    throw new Error("workspace_hooks_trust_store_corrupt");
  }
  const next = new Map(loaded.records.map((record) => [trustKey(record), record]));
  for (const record of records) next.set(trustKey(record), record);
  await atomicWriteTrustStore(loaded.filePath, [...next.values()]);
}

export async function revokeTrustRecords(options, { workspaceIdentity, hookDeclarationDigests }) {
  const loaded = await loadTrustStore(options);
  if (loaded.status === "corrupt") {
    throw new Error("workspace_hooks_trust_store_corrupt");
  }
  const selected = hookDeclarationDigests ? new Set(hookDeclarationDigests) : undefined;
  const records = loaded.records.filter(
    (record) =>
      record.workspaceIdentity !== workspaceIdentity ||
      (selected !== undefined && !selected.has(record.hookDeclarationDigest)),
  );
  await atomicWriteTrustStore(loaded.filePath, records);
}

// ── 翻译：ZCode entries → pi-rs Claude 平铺格式 ─────────────────────────────
//
// pi-rs 工具名为小写（bash/edit/write/...）；ZCode/Claude matcher 惯例是
// PascalCase（Bash|Edit）。简单 alternation（字母+竖线）整体映射小写并走
// 别名表；含正则元字符的 matcher 原样透传（高级用户直接写 pi-rs 名字）。

const MATCHER_NAME_MAP = new Map([
  ["bash", "bash"],
  ["edit", "edit"],
  ["write", "write"],
  ["read", "read"],
  ["glob", "glob"],
  ["grep", "grep"],
  ["webfetch", "web_fetch"],
  ["websearch", "web_search"],
  ["todowrite", "todo"],
  ["todo", "todo"],
  ["task", "subagent"],
  ["agent", "subagent"],
  ["subagent", "subagent"],
  ["applypatch", "edit|write"],
  ["multiedit", "edit"],
  ["notebookedit", "edit"],
]);

const SIMPLE_ALTERNATION = /^[A-Za-z][A-Za-z0-9]*(\|[A-Za-z][A-Za-z0-9]*)*$/;

export function normalizeMatcher(matcher) {
  if (!matcher || matcher === "*") return matcher ?? undefined;
  if (!SIMPLE_ALTERNATION.test(matcher)) return matcher;
  const mapped = matcher
    .split("|")
    .map((token) => MATCHER_NAME_MAP.get(token.toLowerCase()) ?? token.toLowerCase());
  // 展平别名展开（ApplyPatch → edit|write）并去重，保持顺序。
  const flat = [...new Set(mapped.flatMap((item) => item.split("|")))];
  return flat.join("|");
}

/** process 类型（无 shell 直 exec）→ pi 只有 shell command：POSIX 单引号转义拼接。 */
function shellQuote(argv) {
  return argv.map((part) => `'${String(part).replaceAll("'", `'\\''`)}'`).join(" ");
}

/**
 * @param entries 已过滤（trusted && configuredEnabled）的 bundle entries，
 *                以及/或用户 hooks 展开出的同形 entries。
 * @returns pi-rs `set_hooks` 的 Claude 平铺 hooks 对象。
 */
export function entriesToPiHooks(entries) {
  const hooks = {};
  const groupIndex = new Map(); // event\0matcher -> group
  for (const entry of entries) {
    if (!PI_FORWARDED_EVENTS.has(entry.event)) continue;
    const matcher = normalizeMatcher(entry.matcher);
    const key = `${entry.event} ${matcher ?? ""}`;
    let group = groupIndex.get(key);
    if (!group) {
      group = { ...(matcher ? { matcher } : {}), hooks: [] };
      groupIndex.set(key, group);
      (hooks[entry.event] ??= []).push(group);
    }
    const timeout = Math.max(1, Math.ceil(entry.resolvedTimeoutMs / 1000));
    if (entry.type === "process") {
      group.hooks.push({
        type: "command",
        command: shellQuote([entry.command, ...(entry.args ?? [])]),
        timeout,
        ...(entry.statusMessage ? { statusMessage: entry.statusMessage } : {}),
      });
    } else {
      group.hooks.push({
        type: "command",
        command: entry.command,
        timeout,
        ...(entry.async === true ? { async: true } : {}),
        ...(entry.statusMessage ? { statusMessage: entry.statusMessage } : {}),
      });
    }
  }
  return hooks;
}

/** 用户 zcode hooks 展开为 bundle 同形 entries（免信任，configFileKind 不影响 digest——不进 bundle）。 */
function userHooksToEntries({ userHooks, runtimeRoot }) {
  if (!userHooks?.events) return [];
  const pseudoSource = {
    canonicalPath: join(resolveHome(), ".zcode", "cli", "config.json"),
    baseDir: resolveHome(),
    discoveryOrder: 0,
    configFileKind: "explicit",
    explicitProjectConfig: true,
    editable: false,
    hooks: userHooks,
  };
  return resolveEntries({
    workspacePath: resolveHome(),
    sources: [pseudoSource],
    runtimeRoot,
  });
}

// ── 高层流程 ────────────────────────────────────────────────────────────────

/**
 * 发现 + 信任过滤 + 翻译。返回下发 pi-rs 的 hooks 与准入投影数据。
 * 任何单文件解析错误都不致命（记 errors），与上游 discovery 容错一致。
 */
export async function provisionWorkspaceHooks({ workspacePath, workspaceIdentity, homeDir, log }) {
  const resolvedPath = resolve(workspacePath);
  const identity = workspaceIdentity?.trim() || resolvedPath;
  const { sources, errors } = readProjectHookSources({ workingDirectory: resolvedPath });
  for (const error of errors) {
    log?.(`[tack-agent] workspace hook config unreadable: ${error.path}`);
  }
  const userHooks = await readUserZCodeHooks(homeDir);
  const runtimeRoot = resolveRuntimeRoot([userHooks, ...sources.map((source) => source.hooks)]);
  const snapshot = buildBundleSnapshot({
    workspaceIdentity: identity,
    workspacePath: resolvedPath,
    sources,
    runtimeRoot,
  });

  const trust = await loadTrustStore({ homeDir });
  const trustedDigests = new Set(
    trust.records
      .filter((record) => record.workspaceIdentity === identity)
      .map((record) => record.hookDeclarationDigest),
  );

  const bundleEntries = snapshot?.hooks ?? [];
  const corrupt = trust.status === "corrupt";
  const active = bundleEntries.filter(
    (entry) => entry.configuredEnabled && !corrupt && trustedDigests.has(entry.hookDeclarationDigest),
  );
  const pendingCount = bundleEntries.filter(
    (entry) => entry.configuredEnabled && !corrupt && !trustedDigests.has(entry.hookDeclarationDigest),
  ).length;

  const userEntries = userHooksToEntries({ userHooks, runtimeRoot }).filter(
    (entry) => entry.configuredEnabled,
  );
  const piHooks = entriesToPiHooks([...active, ...userEntries]);

  return {
    piHooks,
    pendingCount,
    bundleDigest: snapshot?.bundleDigest ?? null,
    workspaceIdentity: identity,
    discoveredCount: bundleEntries.length,
    activeCount: active.length + userEntries.length,
    trustStoreCorrupt: corrupt,
    errors,
  };
}

function formatCommand(entry) {
  return entry.type === "process" && entry.args?.length
    ? [entry.command, ...entry.args].join(" ")
    : entry.command;
}

/**
 * workspace/hooks/trustGrant：重新发现 canonical snapshot 后写信任记录
 * （UI 提交精确 bundle/declaration；bridge 不信任 UI 给的记录内容）。
 *
 * @returns {{accepted: true, workspaceIdentity: string}
 *          | {accepted: false, reasonCode: string}}
 */
export async function grantWorkspaceHookTrustFlow({
  workspacePath,
  workspaceIdentity,
  bundleDigest,
  hookDeclarationDigest,
  homeDir,
  appVersion,
  log,
}) {
  const fail = (reasonCode) => ({ accepted: false, reasonCode });
  const resolvedPath = resolve(workspacePath);
  const identity = workspaceIdentity?.trim() || resolvedPath;
  const { sources, errors } = readProjectHookSources({ workingDirectory: resolvedPath });
  if (errors.length > 0) {
    log?.(`[tack-agent] trustGrant: hook config unreadable: ${errors[0].path}`);
    return fail("workspace_hooks_config_unreadable");
  }
  const userHooks = await readUserZCodeHooks(homeDir);
  const runtimeRoot = resolveRuntimeRoot([userHooks, ...sources.map((source) => source.hooks)]);
  const snapshot = buildBundleSnapshot({
    workspaceIdentity: identity,
    workspacePath: resolvedPath,
    sources,
    runtimeRoot,
  });
  if (!snapshot) return fail("workspace_hooks_config_unreadable");
  if (bundleDigest && bundleDigest !== snapshot.bundleDigest) {
    return fail("workspace_hooks_bundle_changed");
  }
  const selected = snapshot.hooks.filter(
    (entry) => entry.hookDeclarationDigest === hookDeclarationDigest,
  );
  if (selected.length !== 1) return fail("workspace_hooks_snapshot_mismatch");

  const grantedAt = new Date().toISOString();
  const records = selected.map((entry) => ({
    workspaceIdentity: snapshot.workspaceIdentity,
    hookDeclarationDigest: entry.hookDeclarationDigest,
    digestAlgorithm: "sha256",
    decision: "trusted",
    grantedAt,
    bundleDigestAtGrant: snapshot.bundleDigest,
    eventAtGrant: entry.event,
    displayCommandAtGrant: formatCommand(entry),
    sourcePathAtGrant: entry.sourceRelativePath,
    sourceDiscoveryOrderAtGrant: snapshot.sourceFiles[entry.sourceFileIndex]?.discoveryOrder,
    matcherAtGrant: entry.matcher,
    matcherIndexAtGrant: entry.matcherIndex,
    hookIndexAtGrant: entry.hookIndex,
    ...(appVersion ? { appVersionAtGrant: appVersion } : {}),
  }));
  try {
    await grantTrustRecords({ homeDir }, records);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message === "workspace_hooks_trust_store_corrupt") {
      return fail("workspace_hooks_trust_store_corrupt");
    }
    if (message === "workspace_hooks_config_unreadable") {
      return fail("workspace_hooks_config_unreadable");
    }
    throw error;
  }
  return { accepted: true, workspaceIdentity: identity };
}

/** v4 revokeWorkspaceHookTrust：精确撤销 + 返回是否变化。 */
export async function revokeWorkspaceHookTrustFlow({
  workspacePath,
  workspaceIdentity,
  hookDeclarationDigests,
  homeDir,
}) {
  const resolvedPath = resolve(workspacePath);
  const identity = workspaceIdentity?.trim() || resolvedPath;
  try {
    await revokeTrustRecords(
      { homeDir },
      {
        workspaceIdentity: identity,
        ...(hookDeclarationDigests?.length ? { hookDeclarationDigests } : {}),
      },
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message === "workspace_hooks_trust_store_corrupt") {
      return { accepted: false, reasonCode: "workspace_hooks_trust_store_corrupt" };
    }
    throw error;
  }
  return { accepted: true, workspaceIdentity: identity };
}
