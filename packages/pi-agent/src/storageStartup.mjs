// Agent-owned session storage startup protocol. The desktop requires the
// agent to prepare its session storage before startup completes; pi-rs's
// session store is the JSONL sessions directory, so "preparation" here means
// ensuring the directory exists and reporting the phases truthfully.
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import { createInterface } from "node:readline";
import { sessionDirForCwd } from "./piHome.mjs";

export function sessionStorageDescriptor(cwd, env) {
  const path = sessionDirForCwd(cwd, env);
  return { path, databaseId: createHash("sha256").update(path).digest("hex") };
}

function writeFrame(output, frame) {
  return new Promise((resolve, reject) => {
    output.write(`${JSON.stringify(frame)}\n`, (error) => (error ? reject(error) : resolve()));
  });
}

function startupState({ attemptId, databaseId, sequence, phase, elapsedMs, errorCode }) {
  return {
    schemaVersion: 1,
    attemptId,
    databaseId,
    databaseKind: "session",
    sequence,
    phase,
    elapsedMs,
    ...(errorCode ? { errorCode } : {}),
  };
}

/** Boot-time report for the normal (long-running) agent process. */
export async function reportBootStorageReady({ cwd, env, output }) {
  const { path, databaseId } = sessionStorageDescriptor(cwd, env);
  const startedAt = Date.now();
  const attemptId = randomUUID();
  try {
    fs.mkdirSync(path, { recursive: true, mode: 0o700 });
  } catch (error) {
    await writeFrame(output, {
      method: "startup/storageState",
      params: startupState({
        attemptId,
        databaseId,
        sequence: 1,
        phase: "failed",
        elapsedMs: Date.now() - startedAt,
        errorCode: error?.code === "EACCES" || error?.code === "EPERM" ? "permission_denied" : "io_error",
      }),
    });
    return false;
  }
  await writeFrame(output, {
    method: "startup/storageState",
    params: startupState({ attemptId, databaseId, sequence: 1, phase: "checking", elapsedMs: 0 }),
  });
  await writeFrame(output, {
    method: "startup/storageState",
    params: startupState({
      attemptId,
      databaseId,
      sequence: 2,
      phase: "ready",
      elapsedMs: Date.now() - startedAt,
    }),
  });
  return true;
}

/** `--prepare-storage` one-shot mode run inside the host's worker. */
export async function runPrepareStorage({ cwd, env, input, output, log }) {
  const { path, databaseId } = sessionStorageDescriptor(cwd, env);
  const attemptId = randomUUID();
  log?.(`[pi-agent] prepare-storage begin (path=${path})`);
  const lines = createInterface({ input });
  const acknowledgement = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("storage acknowledgement timeout")), 30_000);
    lines.once("line", (line) => {
      clearTimeout(timer);
      try {
        const frame = JSON.parse(line);
        if (frame?.method !== "startup/storagePathReady") {
          reject(new Error(`unexpected frame: ${line.slice(0, 120)}`));
          return;
        }
        resolve(frame.reuse === true);
      } catch (error) {
        reject(error);
      }
    });
    lines.once("close", () => reject(new Error("storage preparation input closed")));
  });
  void acknowledgement.catch(() => {});
  try {
    await writeFrame(output, { method: "startup/storagePath", params: { path } });
    const reuse = await acknowledgement;
    lines.close();
    if (!reuse) {
      const startedAt = Date.now();
      await writeFrame(output, {
        method: "startup/storageState",
        params: startupState({
          attemptId,
          databaseId,
          sequence: 1,
          phase: "checking",
          elapsedMs: 0,
        }),
      });
      fs.mkdirSync(path, { recursive: true, mode: 0o700 });
      await writeFrame(output, {
        method: "startup/storageState",
        params: startupState({
          attemptId,
          databaseId,
          sequence: 2,
          phase: "ready",
          elapsedMs: Date.now() - startedAt,
        }),
      });
    }
    await writeFrame(output, { method: "startup/storagePrepared", params: {} });
    log?.(`[pi-agent] prepare-storage done`);
  } catch (error) {
    log?.(`[pi-agent] prepare-storage failed: ${error?.message ?? error}`);
    try {
      await writeFrame(output, {
        method: "startup/storageState",
        params: startupState({
          attemptId,
          databaseId,
          sequence: 1,
          phase: "failed",
          elapsedMs: 0,
          errorCode:
            error?.code === "EACCES" || error?.code === "EPERM"
              ? "permission_denied"
              : error?.code === "ENOSPC"
                ? "storage_full"
                : "io_error",
        }),
      });
    } catch {
      /* transport already broken */
    }
    process.exitCode = 1;
  }
}
