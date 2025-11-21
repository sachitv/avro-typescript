import type { CreateTypeOptions, SchemaLike } from "../../type/create_type.ts";
import type { MetadataMap } from "../protocol/wire_format/metadata.ts";
import type { CallRequestEnvelope } from "../protocol/wire_format/messages.ts";
import type { Resolver } from "../../schemas/resolver.ts";
import type { Type } from "../../schemas/type.ts";
import type { Message } from "./message_definition.ts";

export interface ProtocolDefinition {
  protocol: string;
  namespace?: string;
  types?: SchemaLike[];
  messages?: Record<string, MessageDefinition>;
}

export interface MessageDefinition {
  doc?: string;
  request: SchemaLike[];
  response: SchemaLike;
  errors?: SchemaLike[];
  "one-way"?: boolean;
}

export interface ProtocolOptions extends CreateTypeOptions {}

/**
 * Options for configuring message transport behavior in RPC communications.
 * These options control aspects such as buffering, framing, transport mode,
 * and protocol exposure for schema discovery.
 */
export interface MessageTransportOptions {
  /**
   * The size of the buffer used for reading/writing data. When undefined,
   * the transport implementation determines the buffer size, often defaulting
   * to a reasonable value or unlimited capacity.
   */
  bufferSize?: number;
  /**
   * The maximum size of each frame in the message stream. Defaults to 8192.
   */
  frameSize?: number;
  /**
   * The transport mode: "stateless" for request-response (e.g., HTTP),
   * or "stateful" for persistent connections (e.g., WebSockets). Defaults to "stateless".
   */
  mode?: "stateless" | "stateful";
  /**
   * Custom decoder for call request envelopes. Defaults to decodeCallRequestEnvelope.
   */
  requestDecoder?: CallRequestEnvelopeDecoder;
  /**
   * When enabled, allows clients to discover the server's protocol schema
   * even when their protocol hash is unknown. This facilitates schema evolution
   * and interoperability by providing the server protocol in handshake responses.
   * Enable this option to support server schema discovery in development or
   * trusted environments. Defaults to false.
   */
  exposeProtocol?: boolean;
  /**
   * Maximum size in bytes for incoming request payloads. This prevents DoS attacks
   * by limiting memory usage. Defaults to 10MB (10 * 1024 * 1024).
   */
  maxRequestSize?: number;
  /**
   * Timeout in milliseconds for processing a single request. This prevents
   * resource exhaustion from hanging connections. Defaults to 30000 (30 seconds).
   */
  requestTimeout?: number;
}

export interface ProtocolHandlerContext {
  metadata: MetadataMap;
}

export interface CallRequestEnvelopeOptions {
  expectHandshake?: boolean;
}

export type CallRequestEnvelopeDecoder = (
  buffer: ArrayBuffer,
  options?: CallRequestEnvelopeOptions,
) => Promise<CallRequestEnvelope>;

export interface MessageListener {
  /**
   * Protocol that produced this listener so handlers can inspect it.
   */
  readonly protocol: ProtocolLike;
}

export type MessageHandler = (
  request: unknown,
  listener: MessageListener,
  context: ProtocolHandlerContext,
) => Promise<unknown> | unknown;

export interface ResolverEntry {
  response?: Resolver;
  error?: Resolver;
  request?: Resolver;
}

export interface ProtocolLike {
  hashKey: string;
  getHashBytes(): Uint8Array;
  /**
   * Return a shallow copy of the protocol messages map. Mutating the returned
   * map has no effect on the owning protocol.
   */
  getMessages(): ReadonlyMap<string, Message>;
  /**
   * Returns the message definition for `name`. The exposed message remains
   * immutable from the protocol's perspective.
   */
  getMessage(name: string): Message | undefined;
  /**
   * Returns a fresh array of message names to prevent external consumers from
   * mutating the internal tracking array.
   */
  getMessageNames(): readonly string[];
  getMessageDefinition(name: string): MessageDefinition | undefined;
  getHandler(name: string): MessageHandler | undefined;
  hasListenerResolvers(hashKey: string): boolean;
  hasEmitterResolvers(hashKey: string): boolean;
  ensureEmitterResolvers(hashKey: string, remote: ProtocolLike): void;
  ensureListenerResolvers(hashKey: string, emitterProtocol: ProtocolLike): void;
  getEmitterResolvers(
    hashKey: string,
    messageName: string,
  ): ResolverEntry | undefined;
  getListenerResolvers(
    hashKey: string,
    messageName: string,
  ): ResolverEntry | undefined;
  toString(): string;
  getName(): string;
  getNamedTypes(): Type[];
  getProtocolInfo(): ProtocolInfo;
}

export interface ProtocolInfo {
  name: string;
  namespace?: string;
  hashKey: string;
  hashBytes: Uint8Array;
  /**
   * Fresh array to guarantee the caller can't mutate the recorded list.
   */
  messageNames: readonly string[];
}

export type ProtocolFactory<T extends ProtocolLike = ProtocolLike> = (
  definition: ProtocolDefinition,
) => T;
