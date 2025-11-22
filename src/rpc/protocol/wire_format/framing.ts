import {
  concatUint8Arrays,
  toUint8Array,
} from "../../../internal/collections/array_utils.ts";

const DEFAULT_FRAME_SIZE = 8 * 1024;
const FRAME_HEADER_SIZE = 4;

/**
 * Options for framing a message, specifying the frame size.
 */
export interface FrameMessageOptions {
  /**
   * The maximum size in bytes for each frame. Defaults to 8192.
   */
  frameSize?: number;
}

/**
 * Options for decoding a framed message, specifying the starting offset.
 */
export interface DecodeFramedMessageOptions {
  /**
   * The byte offset in the buffer to start decoding from. Defaults to 0.
   */
  offset?: number;
}

/**
 * Result of decoding a framed message, containing the payload and next offset.
 */
export interface DecodeFramedMessageResult {
  /**
   * The reassembled message payload as a Uint8Array.
   */
  payload: Uint8Array;
  /**
   * The byte offset in the buffer after the decoded message.
   */
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

/**
 * Frames a message payload into a sequence of frames with size headers and a terminator frame.
 * Splits the payload into frames of up to the specified frameSize bytes, each prefixed with a 4-byte
 * big-endian length header, and appends a zero-length terminator frame.
 */
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

/**
 * Decodes a framed message from a buffer, reassembling the payload from frames.
 * Reads frames starting from the specified offset, each consisting of a 4-byte big-endian length
 * header followed by the frame data, until a zero-length frame (terminator) is encountered.
 * Concatenates the frame data into the payload.
 */
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
