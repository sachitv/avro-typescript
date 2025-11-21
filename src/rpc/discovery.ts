import {
  decodeCallResponseEnvelope,
  encodeCallRequest,
} from "./protocol/wire_format/messages.ts";
import { frameMessage } from "./protocol/wire_format/framing.ts";
import type { HandshakeRequestInit } from "./protocol/wire_format/handshake.ts";
import { metadataWithId, toArrayBuffer } from "./protocol/protocol_helpers.ts";
import { readSingleMessage } from "./message_endpoint/helpers.ts";
import { toBinaryDuplex } from "./protocol/transports/transport_helpers.ts";
import type { BinaryDuplexLike } from "./protocol/transports/transport_helpers.ts";
import type { MessageTransportOptions } from "./definitions/protocol_definitions.ts";
import { Protocol } from "./protocol_core.ts";
import { createType } from "../type/create_type.ts";

/**
 * Discovers the protocol from a remote server by sending a handshake request.
 *
 * @param transportFactory Factory function that returns a Promise resolving to a BinaryDuplexLike transport.
 * @param opts Optional message transport options.
 * @returns A Promise that resolves to the discovered Protocol.
 * @throws Error if the server does not provide a protocol in the handshake response.
 */
export async function discoverProtocol(
  transportFactory: () => Promise<BinaryDuplexLike>,
  opts: MessageTransportOptions = {},
): Promise<Protocol> {
  const transport = toBinaryDuplex(await transportFactory());

  // Use a dummy hash to force a mismatch
  const dummyHash = new Uint8Array(16);
  const handshake: HandshakeRequestInit = {
    clientHash: dummyHash,
    clientProtocol: null,
    serverHash: dummyHash,
  };

  // Send a dummy request
  const metadata = metadataWithId(0);
  const encoded = await encodeCallRequest({
    handshake,
    metadata,
    messageName: "",
    request: null,
    requestType: createType("null"),
  });
  const framed = frameMessage(encoded, { frameSize: opts.frameSize });
  await transport.writable.write(framed);
  await transport.writable.close();

  const payload = await readSingleMessage(transport.readable);
  const envelope = await decodeCallResponseEnvelope(
    toArrayBuffer(payload),
    { expectHandshake: true },
  );

  if (envelope.handshake?.serverProtocol) {
    // Security: validate JSON before parsing
    if (
      typeof envelope.handshake.serverProtocol !== "string" ||
      envelope.handshake.serverProtocol.length > 1024 * 1024
    ) { // 1MB limit
      throw new Error("invalid server protocol: too large or not a string");
    }
    try {
      const protocolData = JSON.parse(envelope.handshake.serverProtocol);
      return Protocol.create(protocolData);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "unknown error";
      throw new Error(`invalid server protocol JSON: ${errorMessage}`);
    }
  }

  throw new Error("Server did not provide protocol in handshake response");
}
