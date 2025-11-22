import type { CreateTypeOptions, SchemaLike } from "../../type/create_type.ts";
import type { MetadataMap } from "../protocol/wire_format/metadata.ts";
import type { CallRequestEnvelope } from "../protocol/wire_format/messages.ts";
import type { Resolver } from "../../schemas/resolver.ts";
import type { Type } from "../../schemas/type.ts";
import type { Message } from "./message_definition.ts";

/**
 * Represents the definition of an Avro protocol, which includes the protocol name, optional namespace,
 * optional custom types, and a map of message definitions.
 */
export interface ProtocolDefinition {
  /** The name of the protocol. */
  protocol: string;
  /** Optional namespace for the protocol. */
  namespace?: string;
  /** Optional array of custom schema types defined in the protocol. */
  types?: SchemaLike[];
  /** Optional record of message names to their definitions. */
  messages?: Record<string, MessageDefinition>;
}

/**
 * Defines a message within an Avro protocol, specifying the request parameters, response type,
 * possible errors, and whether it's one-way.
 */
export interface MessageDefinition {
  /** Optional documentation for the message. */
  doc?: string;
  /** Array of schemas for the request parameters. */
  request: SchemaLike[];
  /** Schema for the response. */
  response: SchemaLike;
  /** Optional array of schemas for possible error types. */
  errors?: SchemaLike[];
  /** Optional flag indicating if the message is one-way (no response expected). */
  "one-way"?: boolean;
}

/**
 * Options for creating protocol types, extending CreateTypeOptions.
 */
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

/**
 * Context provided to message handlers during RPC processing.
 */
export interface ProtocolHandlerContext {
  /** Metadata map associated with the message. */
  metadata: MetadataMap;
}

/**
 * Options for decoding call request envelopes.
 */
export interface CallRequestEnvelopeOptions {
  /** Whether to expect a handshake in the envelope. */
  expectHandshake?: boolean;
}

/**
 * Function type for decoding call request envelopes from a buffer.
 */
export type CallRequestEnvelopeDecoder = (
  buffer: ArrayBuffer,
  options?: CallRequestEnvelopeOptions,
) => Promise<CallRequestEnvelope>;

/**
 * Interface for a message listener in the RPC system.
 */
export interface MessageListener {
  /**
   * Protocol that produced this listener so handlers can inspect it.
   */
  readonly protocol: ProtocolLike;
}

/**
 * Function type for handling incoming messages.
 */
export type MessageHandler = (
  request: unknown,
  listener: MessageListener,
  context: ProtocolHandlerContext,
) => Promise<unknown> | unknown;

/**
 * Entry containing resolvers for request, response, and error schemas.
 */
export interface ResolverEntry {
  /** Optional resolver for the response schema. */
  response?: Resolver;
  /** Optional resolver for error schemas. */
  error?: Resolver;
  /** Optional resolver for the request schema. */
  request?: Resolver;
}

/**
 * Represents an Avro RPC protocol instance, providing access to message definitions,
 * handlers, resolvers for schema compatibility, and protocol metadata.
 */
export interface ProtocolLike {
  /** The unique hash key identifying this protocol for compatibility checks. */
  hashKey: string;
  /** Returns the hash bytes used for protocol identification. */
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
  /** Retrieves the message definition for the given message name. */
  getMessageDefinition(name: string): MessageDefinition | undefined;
  /** Gets the handler function for the specified message name. */
  getHandler(name: string): MessageHandler | undefined;
  /** Checks if resolvers are available for listening to messages from a protocol with the given hash key. */
  hasListenerResolvers(hashKey: string): boolean;
  /** Checks if resolvers are available for emitting messages to a protocol with the given hash key. */
  hasEmitterResolvers(hashKey: string): boolean;
  /** Ensures that resolvers for emitting messages to the remote protocol are set up. */
  ensureEmitterResolvers(hashKey: string, remote: ProtocolLike): void;
  /** Ensures that resolvers for listening to messages from the emitter protocol are set up. */
  ensureListenerResolvers(hashKey: string, emitterProtocol: ProtocolLike): void;
  /** Retrieves resolvers for emitting the specified message to a protocol with the given hash key. */
  getEmitterResolvers(
    hashKey: string,
    messageName: string,
  ): ResolverEntry | undefined;
  /** Retrieves resolvers for listening to the specified message from a protocol with the given hash key. */
  getListenerResolvers(
    hashKey: string,
    messageName: string,
  ): ResolverEntry | undefined;
  /** Returns a string representation of the protocol. */
  toString(): string;
  /** Gets the name of the protocol. */
  getName(): string;
  /** Returns an array of named types defined in the protocol. */
  getNamedTypes(): Type[];
  /** Retrieves metadata information about the protocol. */
  getProtocolInfo(): ProtocolInfo;
}

/**
 * Metadata information about an Avro RPC protocol.
 */
export interface ProtocolInfo {
  /** The name of the protocol. */
  name: string;
  /** Optional namespace for the protocol. */
  namespace?: string;
  /** The unique hash key for the protocol. */
  hashKey: string;
  /** The hash bytes for protocol identification. */
  hashBytes: Uint8Array;
  /**
   * Fresh array to guarantee the caller can't mutate the recorded list.
   */
  messageNames: readonly string[];
}

/**
 * A factory function type for creating a ProtocolLike instance from a protocol definition.
 */
export type ProtocolFactory<T extends ProtocolLike = ProtocolLike> = (
  definition: ProtocolDefinition,
) => T;
