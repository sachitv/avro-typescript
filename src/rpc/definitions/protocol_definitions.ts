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

export interface MessageTransportOptions {
  bufferSize?: number;
  frameSize?: number;
  mode?: "stateless" | "stateful";
  requestDecoder?: CallRequestEnvelopeDecoder;
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

interface MessageListener {
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
  getMessages(): Map<string, Message>;
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
}

export type ProtocolFactory = (definition: ProtocolDefinition) => ProtocolLike;
