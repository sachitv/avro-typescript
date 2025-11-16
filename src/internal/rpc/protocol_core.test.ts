import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { Protocol, type ProtocolDefinition, StatelessListener } from "./mod.ts";
import type { MessageEmitter } from "./mod.ts";
import type { Message } from "./definitions/message_definition.ts";
import { NamedType } from "../schemas/named_type.ts";
import type { BinaryDuplexLike } from "./protocol/transports.ts";

const definition: ProtocolDefinition = {
  protocol: "Math",
  namespace: "org.example",
  types: [
    {
      name: "Point",
      type: "record",
      fields: [
        { name: "x", type: "int" },
        { name: "y", type: "int" },
      ],
    },
  ],
  messages: {
    add: {
      request: [
        { name: "values", type: { type: "array", items: "int" } },
      ],
      response: "int",
    },
  },
};

const emptyDefinition: ProtocolDefinition = {
  protocol: "Empty",
};

describe("Protocol core", () => {
  it("returns named types", () => {
    const protocol = Protocol.create(definition);
    assertEquals(protocol.getNamedTypes().length, 1);
    assert(protocol.getNamedTypes()[0] instanceof NamedType);
  });

  it("returns protocol name", () => {
    const protocol = Protocol.create(definition);
    assertEquals(protocol.getName(), "org.example.Math");
  });

  it("returns string representation", () => {
    const protocol = Protocol.create(definition);
    assertEquals(typeof protocol.toString(), "string");
  });

  it("returns hash key", () => {
    const protocol = Protocol.create(definition);
    assertEquals(typeof protocol.hashKey, "string");
    assertEquals(protocol.hashKey.length, 32);
  });

  it("returns hash bytes", () => {
    const protocol = Protocol.create(definition);
    const bytes = protocol.getHashBytes();
    assert(bytes instanceof Uint8Array);
    assertEquals(bytes.length, 16);
  });

  it("returns messages map", () => {
    const protocol = Protocol.create(definition);
    const messages = protocol.getMessages();
    assert(messages instanceof Map);
    assertEquals(messages.has("add"), true);
  });

  it("returns type by name", () => {
    const protocol = Protocol.create(definition);
    const type = protocol.getType("org.example.Point");
    assert(type !== undefined);
    assert(type instanceof NamedType);
    assertEquals(protocol.getType("nonexistent"), undefined);
  });

  it("allows handler lookup on parent protocol", () => {
    const parent = Protocol.create(definition);
    const child = parent.subprotocol();
    const handler = () => "ok";
    parent.on("add", handler);
    assertEquals(child.getHandler("add"), handler);
  });

  it("creates subprotocol", () => {
    const parent = Protocol.create(definition);
    const child = parent.subprotocol();
    assert(child instanceof Protocol);
    assertEquals(child.getName(), parent.getName());
    assertEquals(child.hashKey, parent.hashKey);
  });

  it("returns local handler", () => {
    const protocol = Protocol.create(definition);
    const handler = () => "local";
    protocol.on("add", handler);
    assertEquals(protocol.getHandler("add"), handler);
  });

  it("creates protocol with no types or messages", () => {
    const protocol = Protocol.create(emptyDefinition);
    assertEquals(protocol.getNamedTypes().length, 0);
    assertEquals(protocol.getMessages().size, 0);
    assertEquals(protocol.getName(), "Empty");
  });

  it("throws on unknown message in on", () => {
    const protocol = Protocol.create(definition);
    assertThrows(() => {
      protocol.on("unknown", () => {});
    });
  });

  it("throws on unknown message in emit", async () => {
    const protocol = Protocol.create(definition);
    const otherProtocol = Protocol.create({ protocol: "Other" });
    const emitter = otherProtocol.createEmitter(() =>
      Promise.resolve({} as BinaryDuplexLike)
    );
    try {
      await protocol.emit("unknown", {}, emitter);
      assert(false, "should have thrown");
    } catch (e) {
      assertEquals((e as Error).message, "unknown message: unknown");
    }
  });

  it("throws on invalid emitter in emit", async () => {
    const protocol = Protocol.create(definition);
    const otherProtocol = Protocol.create({ protocol: "Other" });
    const emitter = otherProtocol.createEmitter(() =>
      Promise.resolve({} as BinaryDuplexLike)
    );
    try {
      await protocol.emit("add", {}, emitter);
      assert(false, "should have thrown");
    } catch (e) {
      assertEquals((e as Error).message, "invalid emitter");
    }
  });

  it("throws on stateful transport in createEmitter", () => {
    const protocol = Protocol.create(definition);
    assertThrows(() => {
      protocol.createEmitter({} as BinaryDuplexLike);
    });
  });

  it("throws on stateful mode in createListener", () => {
    const protocol = Protocol.create(definition);
    assertThrows(() => {
      protocol.createListener({} as BinaryDuplexLike, { mode: "stateful" });
    });
  });

  it("emits through provided emitter", async () => {
    const protocol = Protocol.create(definition);
    let calledRequest: unknown = null;
    const emitter = {
      protocol,
      send: (message: Message, request: unknown) => {
        assertEquals(message.name, "add");
        calledRequest = request;
        return "emitted";
      },
    } as unknown as MessageEmitter;
    const response = await protocol.emit("add", { values: [1, 2, 3] }, emitter);
    assertEquals(response, "emitted");
    assertEquals(calledRequest, { values: [1, 2, 3] });
  });

  it("creates stateless listener", () => {
    const protocol = Protocol.create(definition);
    const transport: BinaryDuplexLike = {
      readable: {
        // deno-lint-ignore require-await
        read: async () => null,
      },
      writable: {
        write: async () => {},
        close: async () => {},
      },
    };
    const listener = protocol.createListener(transport);
    assert(listener instanceof StatelessListener);
  });

  it("builds emitter resolvers and rejects missing messages", () => {
    const local = Protocol.create(definition);
    const remote = Protocol.create(definition);
    local.ensureEmitterResolvers(remote.hashKey, remote);
    // Call again to cover early return
    local.ensureEmitterResolvers(remote.hashKey, remote);
    const resolver = local.getEmitterResolvers(remote.hashKey, "add");
    assertEquals(resolver !== undefined, true);
    assertEquals(local.hasEmitterResolvers(remote.hashKey), true);
    const other = Protocol.create({ protocol: "Other" });
    assertThrows(() => {
      local.ensureEmitterResolvers(other.hashKey, other);
    });
  });

  it("builds listener resolvers", () => {
    const server = Protocol.create(definition);
    const client = Protocol.create(definition);
    server.ensureListenerResolvers(client.hashKey, client);
    // Call again to cover early return
    server.ensureListenerResolvers(client.hashKey, client);
    const resolver = server.getListenerResolvers(client.hashKey, "add");
    assertEquals(resolver !== undefined, true);
    assertEquals(server.hasListenerResolvers(client.hashKey), true);
  });

  it("throws when client missing messages during listener resolver setup", () => {
    const server = Protocol.create(definition);
    const client = Protocol.create(emptyDefinition);
    assertThrows(
      () => {
        server.ensureListenerResolvers(client.hashKey, client);
      },
      Error,
      "missing client message: add",
    );
  });
});
