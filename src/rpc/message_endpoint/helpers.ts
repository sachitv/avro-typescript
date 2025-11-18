import { FrameAssembler } from "../protocol/frame_assembler.ts";
import type { BinaryReadable } from "../protocol/transports/transport_helpers.ts";
import type {
  CallRequestEnvelope,
  CallResponseEnvelope,
} from "../protocol/wire_format/messages.ts";
import type { Message } from "../definitions/message_definition.ts";
import type { ResolverEntry } from "../definitions/protocol_definitions.ts";

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

export async function drainReadable(readable: BinaryReadable): Promise<void> {
  while ((await readable.read()) !== null) {
    // consume remaining bytes
  }
}

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
