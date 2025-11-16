import { createType } from "../createType/mod.ts";
import type { CreateTypeOptions } from "../createType/mod.ts";
import { NamedType } from "../schemas/named_type.ts";
import type { Type } from "../schemas/type.ts";
import { md5FromString } from "../crypto/md5.ts";
import { Message } from "./definitions/message_definition.ts";
import type {
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
} from "./helpers/protocol_helpers.ts";
import { StatelessEmitter, StatelessListener } from "./mod.ts";
import type { MessageEmitter, MessageListener } from "./mod.ts";
import type {
  BinaryDuplexLike,
  StatelessTransportFactory,
} from "./protocol/transports.ts";

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

  subprotocol(): Protocol {
    return new Protocol(
      this.#name,
      this.#namespace,
      this.#messages,
      this.#types,
      this,
    );
  }

  getName(): string {
    return this.#name;
  }

  toString(): string {
    return stringifyProtocol(this);
  }

  get hashKey(): string {
    return this.#hashKey;
  }

  getHashBytes(): Uint8Array {
    return this.#hashBytes.slice();
  }

  getMessages(): Map<string, Message> {
    return this.#messages;
  }

  getNamedTypes(): Type[] {
    const named: Type[] = [];
    for (const type of this.#types.values()) {
      if (type instanceof NamedType) {
        named.push(type);
      }
    }
    return named;
  }

  getType(name: string): Type | undefined {
    return this.#types.get(name);
  }

  on(name: string, handler: MessageHandler): this {
    if (!this.#messages.has(name)) {
      throw new Error(`unknown message: ${name}`);
    }
    this.#handlers.set(name, handler);
    return this;
  }

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

  createEmitter(
    transport: BinaryDuplexLike | StatelessTransportFactory,
    opts: MessageTransportOptions = {},
  ): MessageEmitter {
    if (typeof transport === "function") {
      return new StatelessEmitter(this, transport, opts, Protocol.create);
    }
    throw new Error("Stateful transports are not supported yet.");
  }

  createListener(
    transport: BinaryDuplexLike,
    opts: MessageTransportOptions = {},
  ): MessageListener {
    if (opts.mode && opts.mode !== "stateless") {
      throw new Error("Stateful listeners are not supported yet.");
    }
    return new StatelessListener(this, transport, opts, Protocol.create);
  }

  getHandler(name: string): MessageHandler | undefined {
    const handler = this.#handlers.get(name);
    if (handler) {
      return handler;
    }
    return this.#parent?.getHandler(name);
  }

  getEmitterResolvers(
    hashKey: string,
    messageName: string,
  ): ResolverEntry | undefined {
    return this.#emitterResolvers.get(hashKey)?.get(messageName);
  }

  getListenerResolvers(
    hashKey: string,
    messageName: string,
  ): ResolverEntry | undefined {
    return this.#listenerResolvers.get(hashKey)?.get(messageName);
  }

  hasListenerResolvers(hashKey: string): boolean {
    return this.#listenerResolvers.has(hashKey);
  }

  hasEmitterResolvers(hashKey: string): boolean {
    return this.#emitterResolvers.has(hashKey);
  }

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
