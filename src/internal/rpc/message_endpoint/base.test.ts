import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type {
  HandshakeRequestInit,
  HandshakeResponseInit,
} from "../protocol/wire_format/handshake.ts";
import type { ProtocolLike } from "../definitions/protocol_definitions.ts";
import { TestEmitter } from "./test_utils.ts";

describe("Message endpoint events", () => {
  const mockProtocol = {
    getHashBytes: () => new Uint8Array([0x12, 0x34, 0x56, 0x78]),
    hashKey: "12345678",
    toString: () => "MockProtocol",
  } as ProtocolLike;

  it("emits error events", () => {
    const emitter = new TestEmitter(mockProtocol);
    let handled = false;
    emitter.addEventListener("error", (event: Event) => {
      const errorEvent = event as ErrorEvent;
      assertEquals(errorEvent.error.message, "boom");
      handled = true;
    });
    emitter.triggerError(new Error("boom"));
    assertEquals(handled, true);
  });

  it("emits error events for non-Error input", () => {
    const emitter = new TestEmitter(mockProtocol);
    let handled = false;
    emitter.addEventListener("error", (event: Event) => {
      const errorEvent = event as ErrorEvent;
      assertEquals(errorEvent.error.message, "boom");
      handled = true;
    });
    emitter.triggerError("boom");
    assertEquals(handled, true);
  });

  it("emits error events using CustomEvent when ErrorEvent unavailable", () => {
    const originalErrorEvent = globalThis.ErrorEvent;
    (globalThis as { ErrorEvent?: typeof ErrorEvent }).ErrorEvent = undefined;
    try {
      const emitter = new TestEmitter(mockProtocol);
      let handled = false;
      emitter.addEventListener("error", (event: Event) => {
        const customEvent = event as CustomEvent;
        assertEquals((customEvent.detail as Error).message, "boom");
        handled = true;
      });
      emitter.triggerError(new Error("boom"));
      assertEquals(handled, true);
    } finally {
      globalThis.ErrorEvent = originalErrorEvent;
    }
  });

  it("emits handshake events", () => {
    const emitter = new TestEmitter(mockProtocol);
    const request: HandshakeRequestInit = {
      clientHash: mockProtocol.getHashBytes(),
      clientProtocol: null,
      serverHash: mockProtocol.getHashBytes(),
    };
    const response: HandshakeResponseInit = {
      match: "BOTH",
      serverHash: mockProtocol.getHashBytes(),
      serverProtocol: null,
      meta: null,
    };
    let handled = false;
    emitter.addEventListener("handshake", (event: Event) => {
      const handshakeEvent = event as CustomEvent<{
        request: HandshakeRequestInit;
        response: HandshakeResponseInit;
      }>;
      assertEquals(handshakeEvent.detail.request, request);
      assertEquals(handshakeEvent.detail.response, response);
      handled = true;
    });
    emitter.triggerHandshake(request, response);
    assertEquals(handled, true);
  });

  it("emits handshake events with clientProtocol", () => {
    const emitter = new TestEmitter(mockProtocol);
    const request: HandshakeRequestInit = {
      clientHash: mockProtocol.getHashBytes(),
      clientProtocol: mockProtocol.toString(),
      serverHash: mockProtocol.getHashBytes(),
    };
    const response: HandshakeResponseInit = {
      match: "CLIENT",
      serverHash: mockProtocol.getHashBytes(),
      serverProtocol: null,
      meta: null,
    };
    let handled = false;
    emitter.addEventListener("handshake", (event: Event) => {
      const handshakeEvent = event as CustomEvent<{
        request: HandshakeRequestInit;
        response: HandshakeResponseInit;
      }>;
      assertEquals(handshakeEvent.detail.request, request);
      assertEquals(handshakeEvent.detail.response, response);
      handled = true;
    });
    emitter.triggerHandshake(request, response);
    assertEquals(handled, true);
  });

  it("emits eot on destroy and updates destroyed state", () => {
    const emitter = new TestEmitter(mockProtocol);
    const pendings: number[] = [];
    emitter.addEventListener("eot", (event: Event) => {
      const eotEvent = event as CustomEvent<{ pending: number }>;
      pendings.push(eotEvent.detail.pending);
    });
    emitter.destroy();
    assertEquals(pendings, [0]);
    assertEquals(emitter.destroyed, true);
  });

  it("returns value from inherited send", async () => {
    const emitter = new TestEmitter(mockProtocol);
    const result = await emitter.send();
    assertEquals(result, "ok");
  });
});
