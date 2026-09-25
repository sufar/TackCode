// v4 attachment store: the begin/chunk/commit/abort upload transaction,
// on-disk persistence (survives bridge restarts for history previews), and
// chunked read-back for the UI's attachment previews.
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export const ATTACHMENT_CHUNK_MAX = 512 * 1024;
export const ATTACHMENT_TOTAL_MAX = 20 * 1024 * 1024;
export const ATTACHMENT_CHUNKS_MAX = 64;

const CHECKSUM_RE = /^sha256:[0-9a-f]{64}$/;

export class AttachmentError extends Error {
  constructor(message) {
    super(message);
    this.name = "AttachmentError";
  }
}

export class AttachmentStore {
  #dir;
  #indexFile;
  #byRef = new Map(); // ref -> {ref, sha256, fileName, mime, bytes}
  #bySha = new Map(); // sha256 -> ref
  #staging = new Map(); // uploadId -> {fileName, mime, totalBytes, totalChunks, checksum, chunks: Map<index, Buffer>, received}

  constructor(agentDir) {
    this.#dir = path.join(agentDir, "attachments");
    this.#indexFile = path.join(this.#dir, "index.json");
    this.#load();
  }

  #load() {
    try {
      const raw = JSON.parse(fs.readFileSync(this.#indexFile, "utf8"));
      for (const [sha256, entry] of Object.entries(raw)) {
        if (fs.existsSync(path.join(this.#dir, sha256))) {
          this.#byRef.set(entry.ref, { sha256, ...entry });
          this.#bySha.set(sha256, entry.ref);
        }
      }
    } catch {
      /* fresh store */
    }
  }

  #persist() {
    fs.mkdirSync(this.#dir, { recursive: true, mode: 0o700 });
    const out = {};
    for (const [sha256, ref] of this.#bySha) {
      const entry = this.#byRef.get(ref);
      out[sha256] = {
        ref: entry.ref,
        fileName: entry.fileName,
        mime: entry.mime,
        bytes: entry.bytes,
      };
    }
    const tmp = `${this.#indexFile}.tmp-${process.pid}`;
    fs.writeFileSync(tmp, `${JSON.stringify(out, null, 2)}\n`, { mode: 0o600 });
    fs.renameSync(tmp, this.#indexFile);
  }

  /** idempotent import of raw bytes (also used for history image blocks). */
  registerBytes(bytes, { fileName, mime }) {
    const sha256 = createHash("sha256").update(bytes).digest("hex");
    const existing = this.#bySha.get(sha256);
    if (existing) return this.#byRef.get(existing);
    const ref = `att-${sha256.slice(0, 24)}`;
    fs.mkdirSync(this.#dir, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(this.#dir, sha256), bytes, { mode: 0o600 });
    const entry = { ref, sha256, fileName, mime, bytes: bytes.length };
    this.#byRef.set(ref, entry);
    this.#bySha.set(sha256, ref);
    this.#persist();
    return entry;
  }

  get(ref) {
    return this.#byRef.get(ref) ?? null;
  }

  readBytes(ref) {
    const entry = this.#byRef.get(ref);
    if (!entry) return null;
    try {
      return { entry, bytes: fs.readFileSync(path.join(this.#dir, entry.sha256)) };
    } catch {
      return null;
    }
  }

  begin(params) {
    const { uploadId, fileName, mime, totalBytes, totalChunks, checksum } = params;
    if (!uploadId || !fileName || !mime || !CHECKSUM_RE.test(checksum ?? "")) {
      throw new AttachmentError("invalid begin params");
    }
    if (totalBytes > ATTACHMENT_TOTAL_MAX) {
      throw new AttachmentError(`attachment exceeds ${ATTACHMENT_TOTAL_MAX} bytes`);
    }
    if (totalChunks > ATTACHMENT_CHUNKS_MAX || totalChunks < 0) {
      throw new AttachmentError("invalid totalChunks");
    }
    if ((totalBytes === 0) !== (totalChunks === 0)) {
      throw new AttachmentError("zero-byte upload must declare zero chunks");
    }
    const sha256 = checksum.slice("sha256:".length);
    const existing = this.#bySha.get(sha256);
    if (existing) {
      return { uploadId, state: "committed", nextChunkIndex: totalChunks, ref: existing };
    }
    this.#staging.set(uploadId, {
      fileName,
      mime,
      totalBytes,
      totalChunks,
      checksum,
      chunks: new Map(),
      received: 0,
    });
    return { uploadId, state: "staging", nextChunkIndex: 0 };
  }

  chunk(params) {
    const { uploadId, chunkIndex, dataBase64 } = params;
    const staging = this.#staging.get(uploadId);
    if (!staging) throw new AttachmentError("unknown uploadId");
    const bytes = Buffer.from(dataBase64 ?? "", "base64");
    if (bytes.length > ATTACHMENT_CHUNK_MAX) {
      throw new AttachmentError(`chunk exceeds ${ATTACHMENT_CHUNK_MAX} decoded bytes`);
    }
    if (!Number.isInteger(chunkIndex) || chunkIndex < 0 || chunkIndex >= staging.totalChunks) {
      throw new AttachmentError("chunkIndex out of range");
    }
    if (!staging.chunks.has(chunkIndex)) {
      staging.chunks.set(chunkIndex, bytes);
      staging.received += bytes.length;
    }
    if (staging.received > staging.totalBytes) {
      throw new AttachmentError("received more bytes than declared");
    }
    let nextChunkIndex = 0;
    while (staging.chunks.has(nextChunkIndex)) nextChunkIndex += 1;
    return { uploadId, nextChunkIndex };
  }

  commit(params) {
    const { uploadId } = params;
    const staging = this.#staging.get(uploadId);
    if (!staging) throw new AttachmentError("unknown uploadId");
    if (staging.chunks.size !== staging.totalChunks || staging.received !== staging.totalBytes) {
      throw new AttachmentError(
        `incomplete upload: ${staging.chunks.size}/${staging.totalChunks} chunks, ${staging.received}/${staging.totalBytes} bytes`,
      );
    }
    const ordered = [];
    for (let index = 0; index < staging.totalChunks; index += 1) {
      ordered.push(staging.chunks.get(index));
    }
    const bytes = Buffer.concat(ordered);
    const sha256 = createHash("sha256").update(bytes).digest("hex");
    if (`sha256:${sha256}` !== staging.checksum) {
      this.#staging.delete(uploadId);
      throw new AttachmentError("checksum mismatch");
    }
    this.#staging.delete(uploadId);
    const entry = this.registerBytes(bytes, {
      fileName: staging.fileName,
      mime: staging.mime,
    });
    return { ref: entry.ref };
  }

  abort(params) {
    this.#staging.delete(params?.uploadId);
    return {};
  }

  read(params) {
    const { ref, offset = 0, limit = ATTACHMENT_CHUNK_MAX } = params;
    const found = this.readBytes(ref);
    if (!found) throw new AttachmentError("unknown attachment ref");
    const { entry, bytes } = found;
    const safeOffset = Math.min(Math.max(0, offset), bytes.length);
    const end = Math.min(bytes.length, safeOffset + limit);
    return {
      dataBase64: bytes.subarray(safeOffset, end).toString("base64"),
      mediaType: entry.mime,
      totalBytes: bytes.length,
      nextOffset: end < bytes.length ? end : null,
    };
  }

  stat(params) {
    const { ref } = params;
    const entry = this.get(ref);
    if (!entry) throw new AttachmentError("unknown attachment ref");
    const filePath = path.join(this.#dir, entry.sha256);
    let mtimeMs;
    try {
      mtimeMs = fs.statSync(filePath).mtimeMs;
    } catch {
      /* absent */
    }
    return {
      mediaType: entry.mime,
      totalBytes: entry.bytes,
      ...(mtimeMs !== undefined ? { mtimeMs } : {}),
    };
  }
}
