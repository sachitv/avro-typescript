import { FrameAssembler } from "../protocol/frame_assembler.ts";
import type { BinaryReadable } from "../protocol/transports/transport_helpers.ts";
import type {
  CallRequestEnvelope,
  CallResponseEnvelope,
} from "../protocol/wire_format/messages.ts";
import type { Message } from "../definitions/message_definition.ts";
import type { ResolverEntry } from "../definitions/protocol_definitions.ts";

/**
 * Reads a single framed message from a binary readable stream in the context of message endpoints.
 * Uses a FrameAssembler to accumulate chunks until a complete message is formed.
 * @param readable The binary readable stream to read from.
 * @returns A promise that resolves to the Uint8Array of the framed message.
 * @throws Error if no framed message is received.
 */
export async function readSingleMessage(
  readable: BinaryReadable,
): Promise<Uint8Array> {
  const assembler = new FrameAssembler();
  while (true) {
    const chunk = await readable.read();
    if (chunk === null) {
      break;
    }
    const messages = assembler.push(chunk);
    if (messages.length) {
      return messages[0];
    }
  }
  throw new Error("no framed message received");
}

/**
 * Drains a binary readable stream by consuming all remaining bytes in the context of message endpoints.
 * Useful for clearing the stream after processing.
 * @param readable The binary readable stream to drain.
 * @returns A promise that resolves when the stream is fully drained.
 */
export async function drainReadable(readable: BinaryReadable): Promise<void> {
  while ((await readable.read()) !== null) {
    // consume remaining bytes
  }
}

/**
 * Reads the payload from a call response envelope in the context of message endpoints.
 * Handles error responses and uses a resolver for schema resolution if provided.
 * @param envelope The call response envelope containing the body tap.
 * @param message The message definition for type information.
 * @param resolver Optional resolver entry for custom schema resolution.
 * @returns A promise that resolves to the deserialized response payload.
 */
export async function readResponsePayload(
  envelope: CallResponseEnvelope,
  message: Message,
  resolver?: ResolverEntry,
): Promise<unknown> {
  const tap = envelope.bodyTap;
  if (envelope.isError) {
    if (resolver?.error) {
      return await resolver.error.read(tap);
    }
    return await message.errorType.read(tap);
  }
  if (resolver?.response) {
    return await resolver.response.read(tap);
  }
  return await message.responseType.read(tap);
}

/**
 * Reads the payload from a call request envelope in the context of message endpoints.
 * Uses a resolver for schema resolution if provided, otherwise falls back to message types.
 * @param envelope The call request envelope containing the body tap.
 * @param message The message definition for type information.
 * @param resolver Optional resolver entry for custom schema resolution.
 * @returns A promise that resolves to the deserialized request payload.
 */
export async function readRequestPayload(
  envelope: CallRequestEnvelope,
  message: Message,
  resolver?: ResolverEntry,
): Promise<unknown> {
  if (resolver?.request) {
    return await resolver.request.read(envelope.bodyTap);
  }
  return await message.requestType.read(envelope.bodyTap);
}
