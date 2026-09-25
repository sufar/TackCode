// pi-rs local knowledge: agent dir layout, session file scanning, the `pi-rs
// models` catalog parse, and auth.json key injection. Everything here is
// read-mostly; the only writes are auth.json api_key entries (the same file
// `pi-rs login --provider X` maintains).
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

export function agentDir(env = process.env) {
  if (env.PI_RS_AGENT_DIR) return env.PI_RS_AGENT_DIR;
  return path.join(os.homedir(), ".pi-rs", "agent");
}

/** Mirror of pi-session's default_session_dir: --<cwd with /\: -> ->--. */
export function sessionDirForCwd(cwd, env = process.env) {
  // pi-rs resolves the process cwd through symlinks (e.g. /tmp -> /private/tmp);
  // scan the same canonical path or sessions vanish for symlinked workspaces.
  let resolved;
  try {
    resolved = fs.realpathSync(cwd).replace(/\\/g, "/");
  } catch {
    resolved = path.resolve(cwd).replace(/\\/g, "/");
  }
  let encoded = resolved.replace(/[/\\:]/g, "-");
  if (encoded.startsWith("-")) encoded = encoded.slice(1);
  return path.join(agentDir(env), "sessions", `--${encoded}--`);
}

const SESSION_FILE_RE = /^(.+)_([0-9a-f-]{36})\.jsonl$/;

/** List pi-rs session files for a cwd, newest activity first. */
export function scanSessionFiles(cwd, env = process.env) {
  const dir = sessionDirForCwd(cwd, env);
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return [];
  }
  const out = [];
  for (const entry of entries) {
    if (!entry.isFile()) continue;
    const match = SESSION_FILE_RE.exec(entry.name);
    if (!match) continue;
    const file = path.join(dir, entry.name);
    let stat;
    try {
      stat = fs.statSync(file);
    } catch {
      continue;
    }
    out.push({
      sessionId: match[2],
      file,
      createdAt: stat.birthtimeMs || stat.mtimeMs,
      lastActivityAt: stat.mtimeMs,
    });
  }
  out.sort((a, b) => b.lastActivityAt - a.lastActivityAt);
  return out;
}

function* iterateLines(file, { headBytes = 256 * 1024, tailBytes = 64 * 1024 } = {}) {
  // Sessions can be huge; read the head (header + first entries) and the tail
  // (latest session_info name) instead of the whole file.
  let fd;
  try {
    fd = fs.openSync(file, "r");
  } catch {
    return;
  }
  try {
    const { size } = fs.fstatSync(fd);
    const headLength = Math.min(size, headBytes);
    const head = Buffer.alloc(headLength);
    fs.readSync(fd, head, 0, headLength, 0);
    for (const line of head.toString("utf8").split("\n")) {
      if (line.trim().length > 0) yield { line, final: false };
    }
    if (size > headBytes) {
      const tailLength = Math.min(size, tailBytes);
      const tail = Buffer.alloc(tailLength);
      fs.readSync(fd, tail, 0, tailLength, size - tailLength);
      const lines = tail.toString("utf8").split("\n");
      for (const line of lines.slice(1)) {
        if (line.trim().length > 0) yield { line, final: true };
      }
    }
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Cheap metadata extraction from a session .jsonl: header (createdAt/cwd),
 * custom name (last session_info entry), first user text (title fallback),
 * last assistant text (preview), and rough activity time (last entry ts).
 */
export function readSessionFileMeta(file) {
  const meta = {
    sessionId: null,
    createdAt: null,
    cwd: null,
    name: null,
    firstUserText: null,
    lastAssistantText: null,
    lastTimestamp: null,
    messageCount: 0,
  };
  for (const { line, final } of iterateLines(file)) {
    let value;
    try {
      value = JSON.parse(line);
    } catch {
      continue;
    }
    if (value.kind === "header") {
      meta.sessionId = value.id ?? meta.sessionId;
      meta.createdAt = value.createdAt ?? meta.createdAt;
      meta.cwd = value.cwd ?? meta.cwd;
      continue;
    }
    if (value.kind !== "entry") continue;
    if (value.type === "session_info" && typeof value.name === "string" && value.name) {
      meta.name = value.name;
    }
    if (value.type !== "message") continue;
    const message = value.message;
    if (!message || typeof message !== "object") continue;
    const timestamp = typeof value.timestamp === "number" ? value.timestamp : message.timestamp;
    if (typeof timestamp === "number") meta.lastTimestamp = timestamp;
    if (message.role === "system") continue;
    meta.messageCount += 1;
    if (message.role === "user" && meta.firstUserText == null) {
      const text = userContentText(message.content);
      if (text) meta.firstUserText = text.slice(0, 200);
    }
    if (message.role === "assistant" && !final) {
      const text = assistantText(message);
      if (text) meta.lastAssistantText = text.slice(0, 120);
    }
  }
  return meta;
}

export function userContentText(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .filter((block) => block && block.type === "text" && typeof block.text === "string")
    .map((block) => block.text)
    .join("");
}

export function assistantText(message) {
  return userContentText(message?.content);
}

/** Parse `pi-rs models` text output into a provider→models catalog. */
export function listModels(piBinary, env = process.env) {
  const result = spawnSync(piBinary, ["models"], {
    env,
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
    timeout: 60_000,
  });
  if (result.status !== 0) {
    throw new Error(`pi-rs models failed: ${result.stderr || result.stdout || result.status}`);
  }
  const providers = [];
  let current = null;
  for (const line of result.stdout.split("\n")) {
    if (line.trim().length === 0) continue;
    const modelMatch = /^ {4}(\S+)\/(\S+)\t(.*)$/.exec(line);
    if (modelMatch && current) {
      current.models.push({
        id: modelMatch[2],
        name: modelMatch[3].trim() || modelMatch[2],
      });
      continue;
    }
    const providerMatch = /^(✓| ) (\S+) — (.+)$/.exec(line);
    if (providerMatch) {
      current = {
        id: providerMatch[2],
        name: providerMatch[3].trim(),
        hasAuth: providerMatch[1] === "✓",
        models: [],
      };
      providers.push(current);
    }
  }
  return providers;
}

export function readAuthJson(env = process.env) {
  try {
    const content = fs.readFileSync(path.join(agentDir(env), "auth.json"), "utf8");
    const value = JSON.parse(content);
    return value && typeof value === "object" ? value : {};
  } catch {
    return {};
  }
}

/**
 * Persist an API key the same way `pi-rs login --provider X` does. Never
 * clobber keyring placeholders or OAuth entries; only fill absent providers
 * or refresh plain api_key entries.
 */
export function writeAuthApiKey(providerId, apiKey, env = process.env) {
  const file = path.join(agentDir(env), "auth.json");
  const auth = readAuthJson(env);
  const existing = auth[providerId];
  if (existing && typeof existing === "object" && existing.type !== "api_key") {
    return false;
  }
  if (existing?.key === apiKey) return true;
  auth[providerId] = { type: "api_key", key: apiKey };
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const tmp = `${file}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, `${JSON.stringify(auth, null, 2)}\n`, { mode: 0o600 });
  fs.renameSync(tmp, file);
  return true;
}

export function findSessionFile(cwd, sessionId, env = process.env) {
  return scanSessionFiles(cwd, env).find((entry) => entry.sessionId === sessionId)?.file ?? null;
}

export function deleteSessionFile(cwd, sessionId, env = process.env) {
  const file = findSessionFile(cwd, sessionId, env);
  if (!file) return false;
  try {
    fs.unlinkSync(file);
    return true;
  } catch {
    return false;
  }
}

export function workspaceIdFromCwd(cwd) {
  return createHash("sha1").update(cwd).digest("hex").slice(0, 12);
}
