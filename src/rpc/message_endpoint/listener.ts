import { encodeCallResponse } from "../protocol/wire_format/messages.ts";
import { frameMessage } from "../protocol/wire_format/framing.ts";
import type {
  HandshakeRequestMessage,
  HandshakeResponseInit,
} from "../protocol/wire_format/handshake.ts";
import { concatUint8Arrays } from "../../internal/collections/array_utils.ts";
import {
  bytesEqual,
  bytesToHex,
  errorToPayload,
  textEncoder,
  toArrayBuffer,
} from "../protocol/protocol_helpers.ts";
import { createType } from "../../type/create_type.ts";
import type {
  BinaryDuplexLike,
  BinaryWritable,
} from "../protocol/transports/transport_helpers.ts";
import { toBinaryDuplex } from "../protocol/transports/transport_helpers.ts";
import { MessageEndpoint } from "./base.ts";
import { decodeCallRequestEnvelope } from "../protocol/wire_format/messages.ts";
import { readRequestPayload } from "./helpers.ts";
import type {
  CallRequestEnvelopeDecoder,
  MessageTransportOptions,
  ProtocolFactory,
  ProtocolLike,
} from "../definitions/protocol_definitions.ts";
import { FrameAssembler } from "../protocol/frame_assembler.ts";
import type { Message } from "../definitions/message_definition.ts";

export abstract class MessageListener extends MessageEndpoint {
  abstract destroy(noWait?: boolean): void;
}

export class StatelessListener extends MessageListener {
  #transport: BinaryDuplexLike;
  #destroyed = false;
  #protocolFactory: ProtocolFactory;
  #requestDecoder: CallRequestEnvelopeDecoder;
  #runPromise: Promise<void>;

  constructor(
    protocol: ProtocolLike,
    transport: BinaryDuplexLike,
    opts: MessageTransportOptions,
    protocolFactory: ProtocolFactory,
  ) {
    super(protocol, opts);
    this.#transport = transport;
    this.#protocolFactory = protocolFactory;
    this.#requestDecoder = opts.requestDecoder ?? decodeCallRequestEnvelope;
    this.#runPromise = this.#run();
  }

  destroy(): void {
    if (!this.#destroyed) {
      this.#destroyed = true;
      this.dispatchEot(0);
    }
  }

  waitForClose(): Promise<void> {
    return this.#runPromise;
  }

  async #run(): Promise<void> {
    const duplex = toBinaryDuplex(this.#transport);
    const assembler = new FrameAssembler();
    try {
      const chunks: Uint8Array[] = [];
      while (true) {
        const chunk = await duplex.readable.read();
        if (chunk === null) {
          break;
        }
        chunks.push(chunk);
      }
      const payload = concatUint8Arrays(chunks);
      const messages = assembler.push(payload);
      if (!messages.length) {
        throw new Error("no request payload");
      }
      await this.#handleRequest(messages[0], duplex);
    } catch (err) {
      this.dispatchError(err);
    } finally {
      await duplex.writable.close();
      this.destroy();
    }
  }

  async #handleRequest(
    payload: Uint8Array,
    duplex: { writable: BinaryWritable },
  ): Promise<void> {
    const envelope = await this.#requestDecoder(
      toArrayBuffer(payload),
      { expectHandshake: true },
    );
    if (!envelope.handshake) {
      throw new Error("missing handshake request");
    }
    const {
      response: handshakeResponse,
      requestResolverKey,
    } = await this.#validateHandshake(envelope.handshake);

    let message: Message | undefined;
    let result: unknown;
    let isError = false;
    let errorPayload: unknown;

    try {
      message = this.protocol.getMessages().get(envelope.messageName);
      if (!message) {
        throw new Error(`unsupported message: ${envelope.messageName}`);
      }
      const resolver = this.protocol.getListenerResolvers(
        requestResolverKey,
        message.name,
      );
      const requestValue = await readRequestPayload(
        envelope,
        message,
        resolver,
      );
      const handler = this.protocol.getHandler(message.name);
      if (!handler) {
        throw new Error(`unsupported message: ${message.name}`);
      }
      result = await handler(requestValue, this, {
        metadata: envelope.metadata,
      });
    } catch (err) {
      isError = true;
      errorPayload = errorToPayload(err);
    }

    const responsePayload = await encodeCallResponse({
      handshake: handshakeResponse,
      metadata: envelope.metadata,
      isError,
      payload: isError ? errorPayload : result,
      responseType: message ? message.responseType : createType("null"),
      errorType: message ? message.errorType : createType(["string"]),
    });
    const framed = frameMessage(responsePayload, { frameSize: this.frameSize });
    await duplex.writable.write(framed);
  }

  // deno-lint-ignore require-await
  async #validateHandshake(
    handshake: HandshakeRequestMessage,
  ): Promise<{
    response: HandshakeResponseInit;
    requestResolverKey: string;
  }> {
    let validationError: unknown = null;
    const clientHashKey = bytesToHex(handshake.clientHash);
    const needsResolver = clientHashKey !== this.protocol.hashKey;
    try {
      if (needsResolver && !this.protocol.hasListenerResolvers(clientHashKey)) {
        if (handshake.clientProtocol) {
          const emitterProtocol = this.#protocolFactory(
            JSON.parse(handshake.clientProtocol),
          );
          this.protocol.ensureListenerResolvers(clientHashKey, emitterProtocol);
        } else {
          validationError = new Error("unknown client protocol hash");
        }
      }
    } catch (err) {
      validationError = err;
    }

    const serverMatch = bytesEqual(
      handshake.serverHash,
      this.protocol.getHashBytes(),
    );
    const response: HandshakeResponseInit = {
      match: validationError ? "NONE" : serverMatch ? "BOTH" : "CLIENT",
      serverProtocol: validationError || serverMatch
        ? null
        : this.protocol.toString(),
      serverHash: serverMatch ? null : this.protocol.getHashBytes(),
      meta: validationError
        ? new Map([["error", textEncoder.encode(String(validationError))]])
        : null,
    };
    return { response, requestResolverKey: clientHashKey };
  }
}
