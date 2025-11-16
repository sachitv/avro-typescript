import {
  decodeCallResponseEnvelope,
  encodeCallRequest,
} from "../protocol/wire_format/messages.ts";
import { frameMessage } from "../protocol/wire_format/framing.ts";
import type {
  HandshakeRequestInit,
  HandshakeResponseMessage,
} from "../protocol/wire_format/handshake.ts";
import {
  bytesToHex,
  metadataWithId,
  toArrayBuffer,
} from "../helpers/protocol_helpers.ts";
import type {
  MessageTransportOptions,
  ProtocolFactory,
  ProtocolLike,
} from "../definitions/protocol_definitions.ts";
import { MessageEndpoint } from "./base.ts";
import type { Message } from "../definitions/message_definition.ts";
import type { BinaryDuplexLike } from "../protocol/transports.ts";
import { toBinaryDuplex } from "../protocol/transports.ts";
import { readResponsePayload } from "./helpers.ts";
import { drainReadable, readSingleMessage } from "./helpers.ts";

interface PendingCall {
  resolve(value: unknown): void;
  reject(reason: unknown): void;
}

/**
 * Abstract base class for message emitters in Avro RPC.
 * Manages server hash, destruction state, and handshake protocol for RPC communication.
 */
export abstract class MessageEmitter extends MessageEndpoint {
  #serverHash: Uint8Array;
  #serverHashKey: string;
  #destroyed = false;

  /**
   * Initializes the emitter with protocol and options, setting initial server hash.
   * @param protocol The Avro protocol.
   * @param opts Transport options.
   */
  protected constructor(protocol: ProtocolLike, opts: MessageTransportOptions) {
    super(protocol, opts);
    this.#serverHash = protocol.getHashBytes();
    this.#serverHashKey = protocol.hashKey;
  }

  /**
   * Gets whether the emitter has been destroyed.
   */
  get destroyed(): boolean {
    return this.#destroyed;
  }

  /**
   * Destroys the emitter, marking it as destroyed and dispatching end-of-transmission.
   */
  destroy(): void {
    if (!this.#destroyed) {
      this.#destroyed = true;
      this.dispatchEot(0);
    }
  }

  /**
   * Gets the current server hash bytes.
   */
  protected get serverHash(): Uint8Array {
    return this.#serverHash;
  }

  /**
   * Updates the server hash with new bytes and recomputes the hex key.
   * @param bytes The new server hash bytes.
   */
  protected updateServerHash(bytes: Uint8Array): void {
    this.#serverHash = bytes.slice();
    this.#serverHashKey = bytesToHex(bytes);
  }

  /**
   * Gets the current server hash as a hex string key.
   */
  protected get serverHashKey(): string {
    return this.#serverHashKey;
  }

  /**
   * Builds a handshake request object with client/server hashes and optional protocol.
   * @param omitProtocol Whether to omit the client protocol string.
   * @returns The handshake request data.
   */
  protected buildHandshakeRequest(omitProtocol: boolean): {
    clientHash: Uint8Array;
    clientProtocol: string | null;
    serverHash: Uint8Array;
  } {
    return {
      clientHash: this.protocol.getHashBytes(),
      clientProtocol: omitProtocol ? null : this.protocol.toString(),
      serverHash: this.#serverHash,
    };
  }

  /**
   * Finalizes the handshake by dispatching events, handling errors, and determining retry.
   * Updates server hash if provided and checks for protocol mismatches.
   * @param request The handshake request.
   * @param response The handshake response.
   * @returns Object indicating if retry is needed and the server hash.
   */
  protected finalizeHandshake(
    request: HandshakeRequestInit,
    response: HandshakeResponseMessage,
  ): { retry: boolean; serverHash: Uint8Array } {
    this.dispatchHandshake(request, response);
    if (response.match === "NONE") {
      const reason = response.meta?.get("error");
      if (reason) {
        throw new Error(new TextDecoder().decode(reason));
      }
    }
    if (!response.serverHash) {
      return { retry: false, serverHash: this.#serverHash };
    }
    this.updateServerHash(response.serverHash);
    let retry = false;
    if (response.match === "NONE" && request.clientProtocol === null) {
      retry = true;
    }
    return {
      retry,
      serverHash: response.serverHash,
    };
  }

  /**
   * Sends an RPC message with the given request data.
   * @param message The message definition.
   * @param request The request payload.
   * @returns Promise resolving to the response or undefined for one-way calls.
   */
  abstract send(message: Message, request: unknown): Promise<unknown>;
}

/**
 * Stateless implementation of MessageEmitter for Avro RPC.
 * Creates a new transport connection for each RPC call, enabling stateless communication.
 * Handles encoding requests, performing handshakes, decoding responses, and tracking pending calls.
 */
export class StatelessEmitter extends MessageEmitter {
  #transportFactory: () => Promise<BinaryDuplexLike>;
  #pending = new Set<PendingCall>();
  #nextId = 1;
  #protocolFactory: ProtocolFactory;

  /**
   * Initializes the stateless emitter with protocol, transport factory, options, and protocol factory.
   * @param protocol The Avro protocol.
   * @param transportFactory Factory function to create binary duplex transports.
   * @param opts Transport options.
   * @param protocolFactory Factory to create protocols from JSON.
   */
  constructor(
    protocol: ProtocolLike,
    transportFactory: () => Promise<BinaryDuplexLike>,
    opts: MessageTransportOptions,
    protocolFactory: ProtocolFactory,
  ) {
    super(protocol, opts);
    this.#transportFactory = transportFactory;
    this.#protocolFactory = protocolFactory;
  }

  /**
   * Destroys the emitter, rejecting all pending calls and clearing the set.
   */
  override destroy(): void {
    if (this.destroyed) {
      return;
    }
    for (const pending of this.#pending) {
      pending.reject(new Error("emitter destroyed"));
    }
    this.#pending.clear();
    super.destroy();
  }

  /**
   * Sends an RPC message, handling one-way and request-response calls.
   * For one-way calls, performs the call and returns undefined.
   * For request-response, tracks the pending call and returns the result.
   * @param message The message to send.
   * @param request The request data.
   * @returns Promise resolving to response or undefined.
   */
  async send(message: Message, request: unknown): Promise<unknown> {
    if (this.destroyed) {
      throw new Error("emitter destroyed");
    }
    if (message.oneWay) {
      await this.#performCall(message, request, true);
      return undefined;
    }
    const promise = this.#performCall(message, request, false);
    return await this.#trackPending(promise);
  }

  /**
   * Tracks a pending call by wrapping it in a new promise and managing resolve/reject.
   * Adds to pending set and removes on completion.
   * @param promise The call promise to track.
   * @returns Wrapped promise that resolves/rejects based on the original.
   */
  async #trackPending(promise: Promise<unknown>): Promise<unknown> {
    let resolve!: (value: unknown) => void;
    let reject!: (error: unknown) => void;
    const wrapped = new Promise<unknown>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    const pending: PendingCall = { resolve, reject };
    this.#pending.add(pending);
    promise
      .then((value) => {
        if (this.#pending.delete(pending)) {
          pending.resolve(value);
        }
      })
      .catch((err) => {
        if (this.#pending.delete(pending)) {
          pending.reject(err);
        }
      });
    return await wrapped;
  }

  /**
   * Performs the actual RPC call by creating transport, encoding request, handshaking, and decoding response.
   * Retries on handshake failure if needed.
   * @param message The RPC message.
   * @param request The request payload.
   * @param oneWay Whether this is a one-way call.
   * @returns Promise resolving to response payload or undefined for one-way.
   */
  async #performCall(
    message: Message,
    request: unknown,
    oneWay: boolean,
  ): Promise<unknown> {
    let omitProtocol = true;
    while (true) {
      const transport = toBinaryDuplex(await this.#transportFactory());
      const metadata = metadataWithId(this.#nextId++);
      const handshake = this.buildHandshakeRequest(omitProtocol);
      const encoded = await encodeCallRequest({
        handshake,
        metadata,
        messageName: message.name,
        request,
        requestType: message.requestType,
      });
      const framed = frameMessage(encoded, { frameSize: this.frameSize });
      await transport.writable.write(framed);
      await transport.writable.close();

      if (oneWay) {
        await drainReadable(transport.readable);
        return undefined;
      }

      const payload = await readSingleMessage(transport.readable);
      const envelope = await decodeCallResponseEnvelope(
        toArrayBuffer(payload),
        { expectHandshake: true },
      );
      const handshakeResponse = envelope.handshake!;
      const outcome = this.finalizeHandshake(handshake, handshakeResponse);

      if (handshakeResponse.serverProtocol) {
        const remote = this.#protocolFactory(
          JSON.parse(handshakeResponse.serverProtocol),
        );
        this.protocol.ensureEmitterResolvers(
          bytesToHex(outcome.serverHash),
          remote,
        );
      }

      if (outcome.retry) {
        omitProtocol = false;
        continue;
      }

      const resolvers = this.protocol.getEmitterResolvers(
        bytesToHex(outcome.serverHash),
        message.name,
      );
      const payloadValue = await readResponsePayload(
        envelope,
        message,
        resolvers,
      );
      return payloadValue;
    }
  }
}
