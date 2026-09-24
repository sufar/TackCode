// V4 physical wire encoding (mirror of packages/shared zcode-protocol-v4
// wire-codec.ts): logical topic frames travel as `complete` envelopes, or as
// crc32-checked UTF-8 fragments when they exceed the physical frame budget.
import { randomUUID } from "node:crypto";

export const V4_WIRE_PROTOCOL_VERSION = 3;
const MAX_FRAME_BYTES = 1024 * 1024;
const MAX_ASSEMBLY_BYTES = 16 * 1024 * 1024;
const MAX_FRAGMENTS = 1024;

const encoder = new TextEncoder();

export function crc32Hex(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) {
      crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
    }
  }
  return ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0");
}

function notificationBytes(wire) {
  return encoder.encode(JSON.stringify({ method: "v4/conversation/frame", params: wire }))
    .byteLength + 1;
}

function makeFragment(options, fragmentIndex, fragmentCount, logicalBytes, checksum, dataBase64) {
  return {
    wireVersion: V4_WIRE_PROTOCOL_VERSION,
    kind: "fragment",
    deliveryKind: options.deliveryKind,
    logicalFrameId: options.logicalFrameId,
    logicalFrameOrdinal: options.logicalFrameOrdinal,
    topic: options.topic,
    subscriptionId: options.subscriptionId,
    fragmentIndex,
    fragmentCount,
    logicalBytes,
    checksum,
    dataBase64,
  };
}

function findFragmentBudget(options, logicalBytes, checksum, maxFrameBytes) {
  let low = 1;
  let high = Math.min(logicalBytes, maxFrameBytes);
  let best = 0;
  const worstCount = logicalBytes;
  while (low <= high) {
    const candidate = Math.floor((low + high) / 2);
    const wire = makeFragment(
      options,
      Math.max(0, worstCount - 1),
      worstCount,
      logicalBytes,
      checksum,
      "A".repeat(4 * Math.ceil(candidate / 3)),
    );
    if (notificationBytes(wire) <= maxFrameBytes) {
      best = candidate;
      low = candidate + 1;
    } else {
      high = candidate - 1;
    }
  }
  return best;
}

/**
 * Encode one logical topic frame into wire frames.
 * options: {deliveryKind, topic, subscriptionId, logicalFrameOrdinal, maxFrameBytes?}
 */
export function encodeTopicWireFrames(frame, options) {
  const maxFrameBytes = options.maxFrameBytes ?? MAX_FRAME_BYTES;
  const logical = encoder.encode(JSON.stringify(frame));
  if (logical.byteLength > MAX_ASSEMBLY_BYTES) {
    throw new Error("proto.frameAssemblyTooLarge");
  }
  const complete = {
    wireVersion: V4_WIRE_PROTOCOL_VERSION,
    kind: "complete",
    deliveryKind: options.deliveryKind,
    logicalFrameId: options.logicalFrameId ?? randomUUID(),
    logicalFrameOrdinal: options.logicalFrameOrdinal,
    topic: options.topic,
    subscriptionId: options.subscriptionId,
    frame,
  };
  if (notificationBytes(complete) <= maxFrameBytes) {
    return [complete];
  }
  const checksum = { algorithm: "crc32", value: crc32Hex(logical) };
  const chunkBytes = findFragmentBudget(options, logical.byteLength, checksum, maxFrameBytes);
  if (chunkBytes < 1) throw new Error("proto.frameEnvelopeTooLarge");
  const fragmentCount = Math.ceil(logical.byteLength / chunkBytes);
  if (fragmentCount > MAX_FRAGMENTS) throw new Error("proto.frameFragmentCountExceeded");
  const frames = [];
  for (let index = 0; index < fragmentCount; index += 1) {
    const slice = logical.subarray(index * chunkBytes, (index + 1) * chunkBytes);
    frames.push(
      makeFragment(
        options,
        index,
        fragmentCount,
        logical.byteLength,
        checksum,
        Buffer.from(slice).toString("base64"),
      ),
    );
  }
  return frames;
}
