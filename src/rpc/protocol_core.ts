import { createType } from "../type/create_type.ts";
import type { CreateTypeOptions } from "../type/create_type.ts";
import { NamedType } from "../schemas/complex/named_type.ts";
import type { Type } from "../schemas/type.ts";
import { md5FromString } from "../internal/crypto/md5.ts";
import { Message } from "./definitions/message_definition.ts";
import type {
  MessageDefinition,
  MessageHandler,
  MessageTransportOptions,
  ProtocolDefinition,
  ProtocolLike,
  ProtocolOptions,
  ResolverEntry,
} from "./definitions/protocol_definitions.ts";
import {
  bytesToHex,
  normalizeProtocolName,
  stringifyProtocol,
} from "./protocol/protocol_helpers.ts";
import { StatelessListener } from "./message_endpoint/listener.ts";
import type { MessageListener } from "./message_endpoint/listener.ts";
import { StatelessEmitter } from "./message_endpoint/emitter.ts";
import type { MessageEmitter } from "./message_endpoint/emitter.ts";
import type {
  BinaryDuplexLike,
  StatelessTransportFactory,
} from "./protocol/transports/transport_helpers.ts";

/**
 * Core Avro RPC protocol representation that validates schemas, hashes the
 * protocol definition, and coordinates message handlers and transports.
 */
export class Protocol implements ProtocolLike {
  #name: string;
  #namespace?: string;
  #messages: Map<string, Message>;
  #types: Map<string, Type>;
  #parent?: Protocol;
  #handlers = new Map<string, MessageHandler>();

  #hashBytes: Uint8Array;
  #hashKey: string;

  #emitterResolvers: Map<string, Map<string, ResolverEntry>> = new Map();
  #listenerResolvers: Map<string, Map<string, ResolverEntry>> = new Map();

  private constructor(
    name: string,
    namespace: string | undefined,
    messages: Map<string, Message>,
    types: Map<string, Type>,
    parent?: Protocol,
  ) {
    this.#name = name;
    this.#namespace = namespace;
    this.#messages = messages;
    this.#types = types;
    this.#parent = parent;

    const canonical = stringifyProtocol(this);
    this.#hashBytes = md5FromString(canonical);
    this.#hashKey = bytesToHex(this.#hashBytes);
  }

  /**
   * Creates a new Protocol instance from a definition.
   * @param attrs The protocol definition object.
   * @param opts Protocol creation options.
   * @returns A new Protocol instance.
   */
  static create(
    attrs: ProtocolDefinition,
    opts: ProtocolOptions = {},
  ): Protocol {
    const name = normalizeProtocolName(
      attrs.protocol,
      attrs.namespace ?? opts.namespace,
    );
    const registry = opts.registry ?? new Map<string, Type>();
    const createOpts: CreateTypeOptions = {
      namespace: attrs.namespace ?? opts.namespace,
      registry,
    };

    if (attrs.types) {
      for (const schema of attrs.types) {
        createType(schema, createOpts);
      }
    }

    const messages = new Map<string, Message>();
    if (attrs.messages) {
      for (const [messageName, definition] of Object.entries(attrs.messages)) {
        messages.set(
          messageName,
          new Message(messageName, definition, createOpts),
        );
      }
    }

    return new Protocol(
      name,
      attrs.namespace ?? opts.namespace,
      messages,
      registry,
    );
  }

  /**
   * Creates a subprotocol sharing the same context but without a parent reference.
   * @returns A new Protocol instance.
   */
  subprotocol(): Protocol {
    return new Protocol(
      this.#name,
      this.#namespace,
      this.#messages,
      this.#types,
      this,
    );
  }

  /**
   * Gets the protocol name.
   */
  getName(): string {
    return this.#name;
  }

  /**
   * Returns the string representation of the protocol (JSON).
   */
  toString(): string {
    return stringifyProtocol(this);
  }

  /**
   * Gets the MD5 hash of the protocol as a hex string.
   */
  get hashKey(): string {
    return this.#hashKey;
  }

  /**
   * Gets the MD5 hash of the protocol as bytes.
   */
  getHashBytes(): Uint8Array {
    return this.#hashBytes.slice();
  }

  /**
   * Gets all messages defined in the protocol.
   */
  getMessages(): Map<string, Message> {
    return this.#messages;
  }

  /**
   * Gets all named types defined in the protocol.
   */
  getNamedTypes(): Type[] {
    const named: Type[] = [];
    for (const type of this.#types.values()) {
      if (type instanceof NamedType) {
        named.push(type);
      }
    }
    return named;
  }

  /**
   * Gets a specific message by name.
   */
  getMessage(name: string): Message | undefined {
    return this.#messages.get(name);
  }

  /**
   * Gets the names of all messages in the protocol.
   */
  getMessageNames(): readonly string[] {
    return Array.from(this.#messages.keys());
  }

  /**
   * Gets the definition of a specific message.
   */
  getMessageDefinition(name: string): MessageDefinition | undefined {
    const message = this.#messages.get(name);
    if (!message) {
      return undefined;
    }
    const json = message.toJSON();
    // Type assertion is safe here since Message.toJSON() returns a structure
    // compatible with MessageDefinition
    return json as unknown as MessageDefinition;
  }

  /**
   * Gets summary information about the protocol.
   */
  getProtocolInfo(): import("./definitions/protocol_definitions.ts").ProtocolInfo {
    return {
      name: this.#name,
      namespace: this.#namespace,
      hashKey: this.#hashKey,
      hashBytes: this.#hashBytes,
      messageNames: this.getMessageNames(),
    };
  }

  /**
   * Gets a type definition by name.
   */
  getType(name: string): Type | undefined {
    return this.#types.get(name);
  }

  /**
   * Registers a message handler for a specific message name.
   */
  on(name: string, handler: MessageHandler): this {
    if (!this.#messages.has(name)) {
      throw new Error(`unknown message: ${name}`);
    }
    this.#handlers.set(name, handler);
    return this;
  }

  /**
   * Emits a message using the provided emitter.
   * @param name The name of the message to emit.
   * @param request The request payload.
   * @param emitter The emitter to use.
   */
  async emit(
    name: string,
    request: unknown,
    emitter: MessageEmitter,
  ): Promise<unknown> {
    const message = this.#messages.get(name);
    if (!message) {
      throw new Error(`unknown message: ${name}`);
    }
    if (emitter.protocol.hashKey !== this.hashKey) {
      throw new Error("invalid emitter");
    }
    return await emitter.send(message, request);
  }

  /**
   * Creates a new message emitter for this protocol.
   * @param transport The transport factory or instance.
   * @param opts Transport options.
   */
  createEmitter(
    transport: BinaryDuplexLike | StatelessTransportFactory,
    opts: MessageTransportOptions = {},
  ): MessageEmitter {
    if (typeof transport === "function") {
      return new StatelessEmitter(this, transport, opts, Protocol.create);
    }
    throw new Error("Stateful transports are not supported yet.");
  }

  /**
   * Creates a new message listener for this protocol.
   * @param transport The transport instance.
   * @param opts Transport options.
   */
  createListener(
    transport: BinaryDuplexLike,
    opts: MessageTransportOptions = {},
  ): MessageListener {
    if (opts.mode && opts.mode !== "stateless") {
      throw new Error("Stateful listeners are not supported yet.");
    }
    return new StatelessListener(this, transport, opts, Protocol.create);
  }

  /**
   * Gets the handler registered for a specific message.
   */
  getHandler(name: string): MessageHandler | undefined {
    const handler = this.#handlers.get(name);
    if (handler) {
      return handler;
    }
    return this.#parent?.getHandler(name);
  }

  /**
   * Gets schema resolvers for an emitter (client) given a server hash.
   */
  getEmitterResolvers(
    hashKey: string,
    messageName: string,
  ): ResolverEntry | undefined {
    return this.#emitterResolvers.get(hashKey)?.get(messageName);
  }

  /**
   * Gets schema resolvers for a listener (server) given a client hash.
   */
  getListenerResolvers(
    hashKey: string,
    messageName: string,
  ): ResolverEntry | undefined {
    return this.#listenerResolvers.get(hashKey)?.get(messageName);
  }

  /**
   * Checks if resolvers exist for a given client hash.
   */
  hasListenerResolvers(hashKey: string): boolean {
    return this.#listenerResolvers.has(hashKey);
  }

  /**
   * Checks if resolvers exist for a given server hash.
   */
  hasEmitterResolvers(hashKey: string): boolean {
    return this.#emitterResolvers.has(hashKey);
  }

  /**
   * Ensures resolvers exist for communicating with a remote protocol (client-side).
   */
  ensureEmitterResolvers(hashKey: string, remote: ProtocolLike): void {
    if (this.#emitterResolvers.has(hashKey)) {
      return;
    }
    const resolvers = new Map<string, ResolverEntry>();
    for (const [name, localMessage] of this.#messages.entries()) {
      const remoteMessage = remote.getMessages().get(name);
      if (!remoteMessage) {
        throw new Error(`missing server message: ${name}`);
      }
      resolvers.set(name, {
        response: localMessage.responseType.createResolver(
          remoteMessage.responseType,
        ),
        error: localMessage.errorType.createResolver(remoteMessage.errorType),
      });
    }
    this.#emitterResolvers.set(hashKey, resolvers);
  }

  /**
   * Ensures resolvers exist for communicating with a remote protocol (server-side).
   */
  ensureListenerResolvers(
    hashKey: string,
    emitterProtocol: ProtocolLike,
  ): void {
    if (this.#listenerResolvers.has(hashKey)) {
      return;
    }
    const resolvers = new Map<string, ResolverEntry>();
    for (const [name, serverMessage] of this.#messages.entries()) {
      const clientMessage = emitterProtocol.getMessages().get(name);
      if (!clientMessage) {
        throw new Error(`missing client message: ${name}`);
      }
      resolvers.set(name, {
        request: serverMessage.requestType.createResolver(
          clientMessage.requestType,
        ),
      });
    }
    this.#listenerResolvers.set(hashKey, resolvers);
  }
}
