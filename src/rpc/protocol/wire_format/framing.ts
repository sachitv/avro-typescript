import {
  concatUint8Arrays,
  toUint8Array,
} from "../../../internal/collections/array_utils.ts";

const DEFAULT_FRAME_SIZE = 8 * 1024;
const FRAME_HEADER_SIZE = 4;

export interface FrameMessageOptions {
  frameSize?: number;
}

export interface DecodeFramedMessageOptions {
  offset?: number;
}

export interface DecodeFramedMessageResult {
  payload: Uint8Array;
  nextOffset: number;
}

function normalizeFrameSize(frameSize: number | undefined): number {
  if (frameSize === undefined) {
    return DEFAULT_FRAME_SIZE;
  }
  if (!Number.isInteger(frameSize) || frameSize <= 0) {
    throw new RangeError("frameSize must be a positive integer.");
  }
  return frameSize;
}

export function frameMessage(
  payload: ArrayBuffer | Uint8Array,
  options?: FrameMessageOptions,
): Uint8Array {
  const bytes = toUint8Array(payload);
  const frameSize = normalizeFrameSize(options?.frameSize);

  const frames: Uint8Array[] = [];
  let offset = 0;
  while (offset < bytes.length) {
    const chunkSize = Math.min(frameSize, bytes.length - offset);
    const header = new Uint8Array(FRAME_HEADER_SIZE);
    // Write big-endian uint32 frame length
    new DataView(header.buffer).setUint32(0, chunkSize, false);
    frames.push(header);
    frames.push(bytes.subarray(offset, offset + chunkSize));
    offset += chunkSize;
  }

  frames.push(new Uint8Array(FRAME_HEADER_SIZE));

  return frames.length === 1 ? frames[0] : concatUint8Arrays(frames);
}

export function decodeFramedMessage(
  buffer: ArrayBuffer,
  options: DecodeFramedMessageOptions = {},
): DecodeFramedMessageResult {
  const { offset = 0 } = options;
  if (!Number.isInteger(offset) || offset < 0) {
    throw new RangeError("offset must be a non-negative integer.");
  }

  const bytes = new Uint8Array(buffer);
  if (offset > bytes.length) {
    throw new RangeError("offset exceeds buffer length.");
  }

  const frames: Uint8Array[] = [];
  const view = new DataView(buffer);
  let cursor = offset;
  let foundTerminator = false;

  while (cursor + FRAME_HEADER_SIZE <= bytes.length) {
    const frameLength = view.getUint32(cursor, false);
    cursor += FRAME_HEADER_SIZE;
    if (frameLength === 0) {
      foundTerminator = true;
      break;
    }
    if (cursor + frameLength > bytes.length) {
      throw new Error("Incomplete frame data.");
    }
    frames.push(bytes.subarray(cursor, cursor + frameLength));
    cursor += frameLength;
  }

  if (!foundTerminator) {
    throw new Error("Message terminator not found.");
  }

  return {
    payload: concatUint8Arrays(frames),
    nextOffset: cursor,
  };
}
