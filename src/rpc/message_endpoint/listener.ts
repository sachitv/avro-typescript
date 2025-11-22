import { encodeCallResponse } from "../protocol/wire_format/messages.ts";
import { frameMessage } from "../protocol/wire_format/framing.ts";
import type {
  HandshakeRequestMessage,
  HandshakeResponseInit,
} from "../protocol/wire_format/handshake.ts";
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

/**
 * Abstract base class for message listeners.
 * Listens for incoming RPC requests and dispatches them to handlers.
 */
export abstract class MessageListener extends MessageEndpoint {
  /**
   * Destroys the listener, stopping it from accepting new requests.
   * @param noWait If true, does not wait for pending requests to complete (if applicable).
   */
  abstract destroy(noWait?: boolean): void;
}

/**
 * Stateless implementation of MessageListener.
 * Handles one request per connection, common in HTTP-based transports.
 */
export class StatelessListener extends MessageListener {
  #transport: BinaryDuplexLike;
  #destroyed = false;
  #protocolFactory: ProtocolFactory;
  #requestDecoder: CallRequestEnvelopeDecoder;
  #runPromise: Promise<void>;
  #exposeProtocol: boolean;
  #maxRequestSize: number;
  #requestTimeout: number;

  /**
   * Creates a new StatelessListener.
   * @param protocol The protocol definition.
   * @param transport The binary transport to read from/write to.
   * @param opts Configuration options.
   * @param protocolFactory Factory for creating protocol instances from schema.
   */
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
    this.#exposeProtocol = opts.exposeProtocol ?? false;
    // Security: limit request size to prevent DoS (default 10MB)
    this.#maxRequestSize = opts.maxRequestSize ?? 10 * 1024 * 1024;
    // Security: timeout to prevent hanging requests (default 30 seconds)
    this.#requestTimeout = opts.requestTimeout ?? 30_000;
    this.#runPromise = this.#run();
  }

  /**
   * Destroys the listener.
   */
  destroy(): void {
    if (!this.#destroyed) {
      this.#destroyed = true;
      this.dispatchEot(0);
    }
  }

  /**
   * Waits for the listener's main loop to exit.
   */
  waitForClose(): Promise<void> {
    return this.#runPromise;
  }

  async #run(): Promise<void> {
    const duplex = toBinaryDuplex(this.#transport);
    const assembler = new FrameAssembler();
    let totalSize = 0;

    try {
      // Set up timeout for the entire request processing
      const timeoutController = new AbortController();
      const timeoutId = setTimeout(() => {
        timeoutController.abort();
      }, this.#requestTimeout);

      try {
        while (!timeoutController.signal.aborted) {
          const chunk = await duplex.readable.read();
          if (chunk === null) {
            break;
          }

          // Security: check size limit to prevent DoS
          totalSize += chunk.length;
          if (totalSize > this.#maxRequestSize) {
            throw new Error(
              `request too large: ${totalSize} bytes (max: ${this.#maxRequestSize})`,
            );
          }

          const messages = assembler.push(chunk);
          if (messages.length > 0) {
            // Process the first complete message
            await this.#handleRequest(messages[0], duplex);
            return; // Only process one request per connection in stateless mode
          }
        }

        if (timeoutController.signal.aborted) {
          throw new Error("request timeout");
        }

        throw new Error("no request payload");
      } finally {
        clearTimeout(timeoutId);
      }
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
    let resolverError: unknown = null;
    const clientHashKey = bytesToHex(handshake.clientHash);
    const needsResolver = clientHashKey !== this.protocol.hashKey;

    if (needsResolver && !this.protocol.hasListenerResolvers(clientHashKey)) {
      if (handshake.clientProtocol) {
        // Security: validate JSON before parsing
        if (
          typeof handshake.clientProtocol !== "string" ||
          handshake.clientProtocol.length > 1024 * 1024
        ) { // 1MB limit
          validationError = new Error(
            "invalid client protocol: too large or not a string",
          );
        } else {
          try {
            // Basic JSON validation
            const parsedProtocol = JSON.parse(handshake.clientProtocol);
            const emitterProtocol = this.#protocolFactory(parsedProtocol);
            this.protocol.ensureListenerResolvers(
              clientHashKey,
              emitterProtocol,
            );
          } catch (err) {
            validationError = err instanceof Error
              ? err
              : new Error("invalid client protocol JSON");
          }
        }
      } else {
        resolverError = new Error("unknown client protocol hash");
      }
    }

    const handshakeIssue = validationError ?? resolverError;
    const serverMatch = bytesEqual(
      handshake.serverHash,
      this.protocol.getHashBytes(),
    );

    // Protocol exposure logic:
    // - Expose when server hash doesn't match AND client hash is known (normal schema evolution)
    // - Expose when exposeProtocol is true (explicit discovery mode)
    // - Never expose when there are validation errors (malformed input)
    const clientHashKnown = !needsResolver;
    const shouldExposeProtocol = ((!serverMatch && clientHashKnown) ||
      this.#exposeProtocol) && !validationError;

    const response: HandshakeResponseInit = {
      match: handshakeIssue ? "NONE" : serverMatch ? "BOTH" : "CLIENT",
      serverProtocol: shouldExposeProtocol ? this.protocol.toString() : null,
      serverHash: serverMatch ? null : this.protocol.getHashBytes(),
      meta: validationError
        ? new Map([["error", textEncoder.encode(String(validationError))]])
        : resolverError
        ? new Map([["error", textEncoder.encode(String(resolverError))]])
        : null,
    };
    return { response, requestResolverKey: clientHashKey };
  }
}
