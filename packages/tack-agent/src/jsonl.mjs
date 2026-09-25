// JSONL framing shared by the host-facing stdio transport and the pi-rs rpc
// child transport. Frame boundary is LF only (U+2028/U+2029 must NOT split
// frames — model text can contain them inside legal JSON strings).
import { StringDecoder } from "node:string_decoder";

export class JsonlWriter {
  #stream;
  #disposed = false;

  constructor(stream) {
    this.#stream = stream;
  }

  get disposed() {
    return this.#disposed;
  }

  write(value) {
    if (this.#disposed) return false;
    try {
      this.#stream.write(`${JSON.stringify(value)}\n`);
      return true;
    } catch {
      this.#disposed = true;
      return false;
    }
  }

  dispose() {
    this.#disposed = true;
  }
}

export class JsonlReader {
  #decoder = new StringDecoder("utf8");
  #buffer = "";
  #onLine;
  #onClose;

  constructor(stream, { onLine, onClose }) {
    this.#onLine = onLine;
    this.#onClose = onClose;
    stream.on("data", (chunk) => this.#push(chunk));
    const end = () => {
      this.#flush();
      this.#onClose?.();
    };
    stream.once("end", end);
    stream.once("close", end);
    stream.on("error", end);
  }

  #push(chunk) {
    this.#buffer += typeof chunk === "string" ? chunk : this.#decoder.write(chunk);
    let index;
    while ((index = this.#buffer.indexOf("\n")) !== -1) {
      const line = this.#buffer.slice(0, index);
      this.#buffer = this.#buffer.slice(index + 1);
      if (line.length > 0) this.#onLine(line);
    }
  }

  #flush() {
    const tail = this.#buffer + this.#decoder.end();
    this.#buffer = "";
    if (tail.trim().length > 0) this.#onLine(tail);
  }
}

export function safeParse(line) {
  try {
    return { ok: true, value: JSON.parse(line) };
  } catch (error) {
    return { ok: false, error };
  }
}
