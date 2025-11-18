import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type {
  HandshakeRequestInit,
  HandshakeResponseInit,
  HandshakeResponseMessage,
} from "../../protocol/wire_format/handshake.ts";
import type { ProtocolLike } from "../../definitions/protocol_definitions.ts";
import { Protocol } from "../../protocol_core.ts";
import { StatelessEmitter } from "../../message_endpoint/emitter.ts";
import { MockDuplex, TestEmitter } from "./test_utils.ts";
import type { BinaryDuplexLike } from "../../protocol/transports/transport_helpers.ts";
import { encodeCallResponse } from "../../protocol/wire_format/messages.ts";
import { frameMessage } from "../../protocol/wire_format/framing.ts";
import { bytesToHex } from "../../protocol/protocol_helpers.ts";

describe("StatelessEmitter", () => {
  const protocol = Protocol.create({
    protocol: "TestProtocol",
    messages: {
      testMessage: {
        request: [{ name: "input", type: "string" }],
        response: "string",
      },
    },
  });

  it("should reject pending calls when destroyed", async () => {
    const mockDuplex = new MockDuplex();
    let resolveTransport: (value: MockDuplex) => void;
    const transportPromise = new Promise<MockDuplex>((resolve) => {
      resolveTransport = resolve;
    });
    const factory = () => transportPromise as Promise<BinaryDuplexLike>;
    const emitter = new StatelessEmitter(
      protocol,
      factory,
      {},
      Protocol.create,
    );
    const message = protocol.getMessages().get("testMessage")!;

    const sendPromise = emitter.send(message, { input: "test" });
    emitter.destroy();

    resolveTransport!(mockDuplex);
    mockDuplex.closeReadable();

    await assertRejects(
      () => sendPromise,
      Error,
      "emitter destroyed",
    );
  });

  it("should send a message and receive a response", async () => {
    const mockDuplex = new MockDuplex();
    const factory = () => Promise.resolve(mockDuplex);
    const emitter = new StatelessEmitter(
      protocol,
      factory,
      {},
      Protocol.create,
    );
    const message = protocol.getMessages().get("testMessage")!;

    const handshakeResponse: HandshakeResponseInit = {
      match: "BOTH",
      serverProtocol: null,
      serverHash: null,
      meta: null,
    };
    const responsePayload = await encodeCallResponse({
      handshake: handshakeResponse,
      metadata: new Map([["id", new Uint8Array([1, 0, 0, 0])]]),
      isError: false,
      payload: "response",
      responseType: message.responseType,
      errorType: message.errorType,
    });
    const framedResponse = frameMessage(responsePayload, { frameSize: 2048 });

    mockDuplex.setResponse(framedResponse);

    const result = await emitter.send(message, { input: "test" });
    assertEquals(result, "response");
  });

  it("should send a one-way message", async () => {
    const oneWayProtocol = Protocol.create({
      protocol: "OneWayProtocol",
      messages: {
        ping: {
          request: [{ name: "data", type: "string" }],
          response: "null",
          "one-way": true,
        },
      },
    });
    const mockDuplex = new MockDuplex();
    const factory = () => Promise.resolve(mockDuplex);
    const emitter = new StatelessEmitter(
      oneWayProtocol,
      factory,
      {},
      Protocol.create,
    );
    const message = oneWayProtocol.getMessages().get("ping")!;

    const handshakeResponse: HandshakeResponseInit = {
      match: "BOTH",
      serverProtocol: null,
      serverHash: null,
      meta: null,
    };
    const responsePayload = await encodeCallResponse({
      handshake: handshakeResponse,
      metadata: new Map([["id", new Uint8Array([1, 0, 0, 0])]]),
      isError: false,
      payload: null,
      responseType: message.responseType,
      errorType: message.errorType,
    });
    const framedResponse = frameMessage(responsePayload, { frameSize: 2048 });

    mockDuplex.setResponse(framedResponse);

    const result = await emitter.send(message, { data: "hello" });
    assertEquals(result, undefined);
  });

  it("should retry on handshake mismatch", async () => {
    const message = protocol.getMessages().get("testMessage")!;
    const handshakeResponse1: HandshakeResponseInit = {
      match: "NONE",
      serverProtocol: protocol.toString(),
      serverHash: protocol.getHashBytes(),
      meta: null,
    };
    const responsePayload1 = await encodeCallResponse({
      handshake: handshakeResponse1,
      metadata: new Map([["id", new Uint8Array([1, 0, 0, 0])]]),
      isError: false,
      payload: "response",
      responseType: message.responseType,
      errorType: message.errorType,
    });
    const framedResponse1 = frameMessage(responsePayload1, { frameSize: 2048 });

    const handshakeResponse2: HandshakeResponseInit = {
      match: "BOTH",
      serverProtocol: null,
      serverHash: null,
      meta: null,
    };
    const responsePayload2 = await encodeCallResponse({
      handshake: handshakeResponse2,
      metadata: new Map([["id", new Uint8Array([2, 0, 0, 0])]]),
      isError: false,
      payload: "response",
      responseType: message.responseType,
      errorType: message.errorType,
    });
    const framedResponse2 = frameMessage(responsePayload2, { frameSize: 2048 });

    let callCount = 0;
    const factory = () => {
      const duplex = new MockDuplex();
      if (callCount === 0) {
        duplex.setResponse(framedResponse1);
      } else {
        duplex.setResponse(framedResponse2);
      }
      callCount++;
      return Promise.resolve(duplex);
    };
    const emitter = new StatelessEmitter(
      protocol,
      factory,
      {},
      Protocol.create,
    );

    const result = await emitter.send(message, { input: "test" });
    assertEquals(result, "response");
    assertEquals(callCount, 2);
  });

  it("should throw when sending after emitter is destroyed", async () => {
    const mockDuplex = new MockDuplex();
    const factory = () => Promise.resolve(mockDuplex);
    const emitter = new StatelessEmitter(
      protocol,
      factory,
      {},
      Protocol.create,
    );
    const message = protocol.getMessages().get("testMessage")!;

    emitter.destroy();

    await assertRejects(
      () => emitter.send(message, { input: "test" }),
      Error,
      "emitter destroyed",
    );
  });

  it("should do nothing when destroying an already destroyed emitter", () => {
    const mockDuplex = new MockDuplex();
    const factory = () => Promise.resolve(mockDuplex);
    const emitter = new StatelessEmitter(
      protocol,
      factory,
      {},
      Protocol.create,
    );

    emitter.destroy();
    // Should not throw or do anything
    emitter.destroy();
    assertEquals(emitter.destroyed, true);
  });

  it("should emit an eot event on destroy", () => {
    const mockDuplex = new MockDuplex();
    const factory = () => Promise.resolve(mockDuplex);
    const emitter = new StatelessEmitter(
      protocol,
      factory,
      {},
      Protocol.create,
    );

    const eotEvents: number[] = [];
    emitter.addEventListener("eot", (event: Event) => {
      const eotEvent = event as CustomEvent<{ pending: number }>;
      eotEvents.push(eotEvent.detail.pending);
    });

    emitter.destroy();
    assertEquals(eotEvents, [0]);
    assertEquals(emitter.destroyed, true);
  });

  it("should ensure resolvers when serverProtocol is provided", async () => {
    let resolversEnsured = false;
    const baseProtocol = Protocol.create({
      protocol: "TestProtocol",
      messages: {
        testMessage: {
          request: [{ name: "input", type: "string" }],
          response: "string",
        },
      },
    });
    const testProtocol = Object.assign(baseProtocol, {
      ensureEmitterResolvers(_hashKey: string, _remote: ProtocolLike): void {
        resolversEnsured = true;
        // Do not call base to avoid potential recursion
      },
    }) as ProtocolLike;
    const mockDuplex = new MockDuplex();
    const factory = () => Promise.resolve(mockDuplex);
    const emitter = new StatelessEmitter(
      testProtocol,
      factory,
      {},
      Protocol.create,
    );
    const message = testProtocol.getMessages().get("testMessage")!;

    const handshakeResponse: HandshakeResponseInit = {
      match: "BOTH",
      serverProtocol: testProtocol.toString(), // Provide serverProtocol
      serverHash: null,
      meta: null,
    };
    const responsePayload = await encodeCallResponse({
      handshake: handshakeResponse,
      metadata: new Map([["id", new Uint8Array([1, 0, 0, 0])]]),
      isError: false,
      payload: "response",
      responseType: message.responseType,
      errorType: message.errorType,
    });
    const framedResponse = frameMessage(responsePayload, { frameSize: 2048 });

    mockDuplex.setResponse(framedResponse);

    const result = await emitter.send(message, { input: "test" });
    assertEquals(result, "response");
    assertEquals(resolversEnsured, true);
  });

  it("should reject on transport read error", async () => {
    const mockDuplex = new MockDuplex();
    mockDuplex.throwOnRead = true;
    const factory = () => Promise.resolve(mockDuplex);
    const emitter = new StatelessEmitter(
      protocol,
      factory,
      {},
      Protocol.create,
    );
    const message = protocol.getMessages().get("testMessage")!;

    await assertRejects(
      () => emitter.send(message, { input: "test" }),
      Error,
      "transport read failed",
    );
  });

  describe("finalizeHandshake", () => {
    it("should initialize serverHash from protocol", () => {
      const emitter = new TestEmitter(protocol);
      assertEquals(emitter.getServerHash(), protocol.getHashBytes());
    });

    it("should return retry false for match BOTH", () => {
      const emitter = new TestEmitter(protocol);
      const request: HandshakeRequestInit = {
        clientHash: protocol.getHashBytes(),
        clientProtocol: null,
        serverHash: protocol.getHashBytes(),
      };
      const response: HandshakeResponseMessage = {
        match: "BOTH",
        serverHash: protocol.getHashBytes(),
        serverProtocol: null,
        meta: null,
      };
      let eventFired = false;
      emitter.addEventListener("handshake", () => {
        eventFired = true;
      });
      const outcome = emitter.triggerFinalizeHandshake(request, response);
      assertEquals(eventFired, true);
      assertEquals(outcome.retry, false);
      assertEquals(outcome.serverHash, protocol.getHashBytes());
      assertEquals(
        emitter.getServerHashKey(),
        bytesToHex(protocol.getHashBytes()),
      );
    });

    it("should return retry true for match NONE and clientProtocol null", () => {
      const emitter = new TestEmitter(protocol);
      const request: HandshakeRequestInit = {
        clientHash: protocol.getHashBytes(),
        clientProtocol: null,
        serverHash: protocol.getHashBytes(),
      };
      const response: HandshakeResponseMessage = {
        match: "NONE",
        serverHash: protocol.getHashBytes(),
        serverProtocol: null,
        meta: null,
      };
      let eventFired = false;
      emitter.addEventListener("handshake", () => {
        eventFired = true;
      });
      const outcome = emitter.triggerFinalizeHandshake(request, response);
      assertEquals(eventFired, true);
      assertEquals(outcome.retry, true);
      assertEquals(outcome.serverHash, protocol.getHashBytes());
      assertEquals(
        emitter.getServerHashKey(),
        bytesToHex(protocol.getHashBytes()),
      );
    });

    it("should return retry false for match NONE and clientProtocol not null", () => {
      const emitter = new TestEmitter(protocol);
      const request: HandshakeRequestInit = {
        clientHash: protocol.getHashBytes(),
        clientProtocol: protocol.toString(),
        serverHash: protocol.getHashBytes(),
      };
      const response: HandshakeResponseMessage = {
        match: "NONE",
        serverHash: protocol.getHashBytes(),
        serverProtocol: null,
        meta: null,
      };
      let eventFired = false;
      emitter.addEventListener("handshake", () => {
        eventFired = true;
      });
      const outcome = emitter.triggerFinalizeHandshake(request, response);
      assertEquals(eventFired, true);
      assertEquals(outcome.retry, false);
      assertEquals(outcome.serverHash, protocol.getHashBytes());
      assertEquals(
        emitter.getServerHashKey(),
        bytesToHex(protocol.getHashBytes()),
      );
    });

    it("should throw an error for match NONE with error meta", () => {
      const emitter = new TestEmitter(protocol);
      const request: HandshakeRequestInit = {
        clientHash: protocol.getHashBytes(),
        clientProtocol: null,
        serverHash: protocol.getHashBytes(),
      };
      const response: HandshakeResponseMessage = {
        match: "NONE",
        serverHash: null,
        serverProtocol: null,
        meta: new Map([["error", new TextEncoder().encode("test error")]]),
      };
      let eventFired = false;
      emitter.addEventListener("handshake", () => {
        eventFired = true;
      });
      assertThrows(
        () => {
          emitter.triggerFinalizeHandshake(request, response);
        },
        Error,
        "test error",
      );
      assertEquals(eventFired, true);
    });

    it("should update serverHash when provided", () => {
      const emitter = new TestEmitter(protocol);
      const newHash = new Uint8Array(16);
      newHash[0] = 1;
      const request: HandshakeRequestInit = {
        clientHash: protocol.getHashBytes(),
        clientProtocol: null,
        serverHash: protocol.getHashBytes(),
      };
      const response: HandshakeResponseMessage = {
        match: "CLIENT",
        serverHash: newHash,
        serverProtocol: null,
        meta: null,
      };
      const outcome = emitter.triggerFinalizeHandshake(request, response);
      assertEquals(outcome.serverHash, newHash);
      assertEquals(emitter.getServerHashKey(), bytesToHex(newHash));
      assertEquals(emitter.getServerHash(), newHash);
    });
  });
});
