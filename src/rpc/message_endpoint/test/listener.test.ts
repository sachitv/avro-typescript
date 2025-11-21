import { assertEquals, assertRejects } from "@std/assert";
import { beforeEach, describe, it } from "@std/testing/bdd";
import type { HandshakeRequestInit } from "../../protocol/wire_format/handshake.ts";
import { Protocol } from "../../protocol_core.ts";
import { StatelessListener } from "../listener.ts";
import {
  type CallRequestEnvelope,
  decodeCallResponse,
  encodeCallRequest,
} from "../../protocol/wire_format/messages.ts";
import { frameMessage } from "../../protocol/wire_format/framing.ts";
import { bytesToHex } from "../../protocol/protocol_helpers.ts";
import { FrameAssembler } from "../../protocol/frame_assembler.ts";
import { createType } from "../../../type/create_type.ts";
import type { Message } from "../../definitions/message_definition.ts";
import { MockDuplex } from "./test_utils.ts";
import { discoverProtocol } from "../../discovery.ts";
import type { BinaryDuplexLike } from "../../protocol/transports/transport_helpers.ts";

describe("StatelessListener", () => {
  const protocol = Protocol.create({
    protocol: "TestProtocol",
    messages: {
      testMessage: {
        request: [{ name: "input", type: "string" }],
        response: "string",
      },
    },
  });
  const message = protocol.getMessages().get("testMessage")!;
  const textDecoder = new TextDecoder();

  beforeEach(() => {
    protocol.on("testMessage", () => "response");
  });

  const createMetadata = () => new Map([["id", new Uint8Array([1, 0, 0, 0])]]);

  const frameTestRequest = async (handshake: HandshakeRequestInit) => {
    const payload = await encodeCallRequest({
      handshake,
      metadata: createMetadata(),
      messageName: message.name,
      request: { input: "test" },
      requestType: message.requestType,
    });
    return frameMessage(payload, { frameSize: 2048 });
  };

  const frameUnknownRequest = async (
    handshake: HandshakeRequestInit,
    messageName: string,
  ) => {
    const payload = await encodeCallRequest({
      handshake,
      metadata: createMetadata(),
      messageName,
      request: null,
      requestType: createType("null"),
    });
    return frameMessage(payload, { frameSize: 2048 });
  };

  const frameRequestWithMessage = async (
    handshake: HandshakeRequestInit,
    msg: Message,
  ) => {
    const payload = await encodeCallRequest({
      handshake,
      metadata: createMetadata(),
      messageName: msg.name,
      request: { input: "test" },
      requestType: msg.requestType,
    });
    return frameMessage(payload, { frameSize: 2048 });
  };

  const waitForListenerClose = async (listener: StatelessListener) => {
    await new Promise<void>((resolve) => {
      listener.addEventListener("eot", () => resolve());
    });
    await listener.waitForClose();
  };

  const chunkToBuffer = (chunk: Uint8Array): ArrayBuffer => {
    const assembler = new FrameAssembler();
    const frames = assembler.push(chunk);
    if (!frames.length) {
      throw new Error("missing response payload");
    }
    return frames[0].slice().buffer;
  };

  const createHash = (modifier: number): Uint8Array => {
    const hash = new Uint8Array(protocol.getHashBytes());
    hash[0] ^= modifier;
    return hash;
  };

  const createDuplexPair = (): {
    clientSide: BinaryDuplexLike;
    serverSide: BinaryDuplexLike;
  } => {
    const clientToServer = new TransformStream<Uint8Array, Uint8Array>();
    const serverToClient = new TransformStream<Uint8Array, Uint8Array>();

    const clientSide: BinaryDuplexLike = {
      readable: serverToClient.readable,
      writable: clientToServer.writable,
    };

    const serverSide: BinaryDuplexLike = {
      readable: clientToServer.readable,
      writable: serverToClient.writable,
    };

    return { clientSide, serverSide };
  };

  it("handles request and sends response", async () => {
    let handled = false;
    protocol.on("testMessage", (request) => {
      handled = true;
      assertEquals(request, { input: "test" });
      return "response";
    });

    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: protocol.getHashBytes(),
      clientProtocol: null,
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    assertEquals(mockDuplex.sent.length, 1);
    assertEquals(handled, true);
    listener.destroy();
  });

  it("does not emit an error after processing a request", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: protocol.getHashBytes(),
      clientProtocol: null,
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    const errors: Error[] = [];
    listener.addEventListener("error", (event) => {
      const errorEvent = event as ErrorEvent;
      if (errorEvent.error instanceof Error) {
        errors.push(errorEvent.error);
      }
    });

    await waitForListenerClose(listener);

    assertEquals(errors.length, 0);
    listener.destroy();
  });

  it("handles error in request", async () => {
    protocol.on("testMessage", () => {
      throw new Error("handler error");
    });

    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: protocol.getHashBytes(),
      clientProtocol: null,
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    assertEquals(mockDuplex.sent.length, 1);
    listener.destroy();
  });

  it("emits error when stream closes without a payload", async () => {
    const mockDuplex = new MockDuplex();
    mockDuplex.setResponse(new Uint8Array());

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );
    const errors: Error[] = [];
    listener.addEventListener("error", (event) => {
      const errorEvent = event as ErrorEvent;
      if (errorEvent.error instanceof Error) {
        errors.push(errorEvent.error);
      }
    });

    await waitForListenerClose(listener);

    assertEquals(errors.map((err) => err.message), ["no request payload"]);
    listener.destroy();
  });

  it("dispatches error when the handshake is missing", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: protocol.getHashBytes(),
      clientProtocol: null,
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {
        requestDecoder: () =>
          Promise.resolve(
            {
              handshake: undefined,
              metadata: new Map(),
              messageName: message.name,
              bodyTap: new Uint8Array(0) as never,
            } satisfies CallRequestEnvelope,
          ),
      },
      Protocol.create,
    );
    const errors: Error[] = [];
    listener.addEventListener("error", (event) => {
      const errorEvent = event as ErrorEvent;
      if (errorEvent.error instanceof Error) {
        errors.push(errorEvent.error);
      }
    });

    await waitForListenerClose(listener);

    assertEquals(errors.map((err) => err.message), [
      "missing handshake request",
    ]);
    listener.destroy();
  });

  it("returns NONE when client hash is unknown and no protocol is supplied", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: createHash(1),
      clientProtocol: null,
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "NONE");
    assertEquals(response.handshake?.serverProtocol, null);
    const errorMeta = response.handshake?.meta?.get("error");
    assertEquals(
      textDecoder.decode(errorMeta ?? new Uint8Array()),
      "Error: unknown client protocol hash",
    );
    listener.destroy();
  });

  it("bubbles validation errors when the client protocol is missing messages", async () => {
    const mockDuplex = new MockDuplex();
    const incompleteProtocol = Protocol.create({
      protocol: "TestProtocol",
      messages: {},
    });
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: createHash(2),
      clientProtocol: incompleteProtocol.toString(),
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "NONE");
    const errorMeta = response.handshake?.meta?.get("error");
    assertEquals(
      textDecoder.decode(errorMeta ?? new Uint8Array()),
      "Error: missing client message: testMessage",
    );
    listener.destroy();
  });

  it("ensures resolvers when the client provides its protocol", async () => {
    const mockDuplex = new MockDuplex();
    const clientHash = createHash(3);
    const handshakeRequest: HandshakeRequestInit = {
      clientHash,
      clientProtocol: protocol.toString(),
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    const clientHashKey = bytesToHex(clientHash);
    assertEquals(protocol.hasListenerResolvers(clientHashKey), true);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "BOTH");
    assertEquals(response.handshake?.serverProtocol, null);
    assertEquals(response.handshake?.meta, null);
    listener.destroy();
  });

  it("advertises the server protocol when the server hash mismatches", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: protocol.getHashBytes(),
      clientProtocol: null,
      serverHash: createHash(4),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "CLIENT");
    assertEquals(response.handshake?.serverProtocol, protocol.toString());
    assertEquals(
      response.handshake?.serverHash,
      protocol.getHashBytes(),
    );
    listener.destroy();
  });

  it("handles unsupported message", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: protocol.getHashBytes(),
      clientProtocol: null,
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameUnknownRequest(
      handshakeRequest,
      "unknownMessage",
    );
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    assertEquals(mockDuplex.sent.length, 1);
    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: createType("null"),
        errorType: createType(["string"]),
        expectHandshake: true,
      },
    );
    assertEquals(response.isError, true);
    assertEquals(response.payload, {
      string: "unsupported message: unknownMessage",
    });
    listener.destroy();
  });

  it("handles message without handler", async () => {
    const protocolWithoutHandler = Protocol.create({
      protocol: "TestProtocol2",
      messages: {
        noHandlerMessage: {
          request: [{ name: "input", type: "string" }],
          response: "string",
        },
      },
    });
    const message = protocolWithoutHandler.getMessages().get(
      "noHandlerMessage",
    )!;

    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: protocolWithoutHandler.getHashBytes(),
      clientProtocol: null,
      serverHash: protocolWithoutHandler.getHashBytes(),
    };
    const framedRequest = await frameRequestWithMessage(
      handshakeRequest,
      message,
    );
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocolWithoutHandler,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    assertEquals(mockDuplex.sent.length, 1);
    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.isError, true);
    assertEquals(response.payload, {
      string: "unsupported message: noHandlerMessage",
    });
    listener.destroy();
  });

  it("handles invalid client protocol JSON", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: createHash(5),
      clientProtocol: "{invalid json",
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "NONE");
    const errorMeta = response.handshake?.meta?.get("error");
    assertEquals(
      textDecoder.decode(errorMeta ?? new Uint8Array()),
      "SyntaxError: Expected property name or '}' in JSON at position 1 (line 1 column 2)",
    );
    listener.destroy();
  });

  it("does not expose protocol on validation failure even when discovery is enabled", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: createHash(14),
      clientProtocol: "{broken json",
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      { exposeProtocol: true },
      Protocol.create,
    );

    await waitForListenerClose(listener);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "NONE");
    assertEquals(response.handshake?.serverProtocol, null);
    listener.destroy();
  });

  it("does not expose protocol by default on unknown hash", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: createHash(6),
      clientProtocol: null,
      serverHash: createHash(7),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "NONE");
    assertEquals(response.handshake?.serverProtocol, null);
    listener.destroy();
  });

  it("exposes protocol when exposeProtocol is true on unknown hash", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: createHash(8),
      clientProtocol: null,
      serverHash: createHash(9),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      { exposeProtocol: true },
      Protocol.create,
    );

    await waitForListenerClose(listener);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "NONE");
    assertEquals(response.handshake?.serverProtocol, protocol.toString());
    listener.destroy();
  });

  it("Protocol.discover retrieves protocol from server", async () => {
    const { clientSide, serverSide } = createDuplexPair();

    const listener = new StatelessListener(
      protocol,
      serverSide,
      { exposeProtocol: true },
      Protocol.create,
    );

    const discoveryPromise = discoverProtocol(() =>
      Promise.resolve(clientSide)
    );

    await waitForListenerClose(listener);

    const discoveredProtocol = await discoveryPromise;

    assertEquals(discoveredProtocol.getName(), protocol.getName());
    assertEquals(discoveredProtocol.getHashBytes(), protocol.getHashBytes());
    assertEquals(discoveredProtocol.toString(), protocol.toString());

    listener.destroy();
  });

  it("Protocol.discover fails when server does not expose protocol", async () => {
    const { clientSide, serverSide } = createDuplexPair();

    // Create listener that doesn't expose protocol
    const listener = new StatelessListener(
      protocol,
      serverSide,
      { exposeProtocol: false }, // Don't expose protocol
      Protocol.create,
    );

    const discoveryPromise = discoverProtocol(() =>
      Promise.resolve(clientSide)
    );

    await waitForListenerClose(listener);

    await assertRejects(
      () => discoveryPromise,
      Error,
      "Server did not provide protocol in handshake response",
    );

    listener.destroy();
  });

  it("respects configurable maxRequestSize and requestTimeout", async () => {
    const mockDuplex = new MockDuplex();
    // Create a large payload that exceeds the custom limit
    const largePayload = new Uint8Array(1024); // 1KB, but we'll set limit to 512B
    mockDuplex.setResponse(largePayload);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {
        maxRequestSize: 512, // Small limit for testing
        requestTimeout: 100, // Short timeout for testing
      },
      Protocol.create,
    );

    const errors: Error[] = [];
    listener.addEventListener("error", (event) => {
      const errorEvent = event as ErrorEvent;
      if (errorEvent.error instanceof Error) {
        errors.push(errorEvent.error);
      }
    });

    await waitForListenerClose(listener);

    // Should get size limit error
    assertEquals(errors.length, 1);
    assertEquals(errors[0].message.includes("too large"), true);

    listener.destroy();
  });

  it("handles destroy called multiple times", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: protocol.getHashBytes(),
      clientProtocol: null,
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    // Call destroy multiple times - should not throw
    listener.destroy();
    listener.destroy();
  });

  it("handles transport read error", async () => {
    const mockDuplex = new MockDuplex();
    mockDuplex.throwOnRead = true;

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    const errors: Error[] = [];
    listener.addEventListener("error", (event) => {
      let error: Error | undefined;
      if (event instanceof ErrorEvent) {
        error = event.error;
      } else if (event instanceof CustomEvent) {
        error = event.detail;
      }
      if (error instanceof Error) {
        errors.push(error);
      }
    });

    await waitForListenerClose(listener);

    assertEquals(errors.length, 1);
    assertEquals(errors[0].message, "transport read failed");

    listener.destroy();
  });

  it("handles invalid client protocol type", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: createHash(10),
      clientProtocol: null,
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {
        requestDecoder: () =>
          Promise.resolve(
            {
              handshake: {
                clientHash: createHash(10),
                clientProtocol: 123 as unknown as string, // Invalid type
                serverHash: protocol.getHashBytes(),
                meta: null,
              },
              metadata: new Map(),
              messageName: message.name,
              bodyTap: new Uint8Array(0) as never,
            } satisfies CallRequestEnvelope,
          ),
      },
      Protocol.create,
    );

    await waitForListenerClose(listener);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "NONE");
    const errorMeta = response.handshake?.meta?.get("error");
    assertEquals(
      textDecoder.decode(errorMeta ?? new Uint8Array()),
      "Error: invalid client protocol: too large or not a string",
    );

    listener.destroy();
  });

  it("handles client protocol too large", async () => {
    const mockDuplex = new MockDuplex();
    const largeProtocol = "x".repeat(1024 * 1024 + 1); // >1MB
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: createHash(11),
      clientProtocol: largeProtocol,
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      Protocol.create,
    );

    await waitForListenerClose(listener);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "NONE");
    const errorMeta = response.handshake?.meta?.get("error");
    assertEquals(
      textDecoder.decode(errorMeta ?? new Uint8Array()),
      "Error: invalid client protocol: too large or not a string",
    );

    listener.destroy();
  });

  it("handles request timeout", async () => {
    const sent: Uint8Array[] = [];
    const duplex: BinaryDuplexLike = {
      readable: {
        read: () =>
          new Promise<Uint8Array | null>((resolve) =>
            setTimeout(() => resolve(null), 200) // Delay longer than timeout
          ),
      },
      writable: new WritableStream<Uint8Array>({
        write: (chunk) => {
          sent.push(chunk);
        },
      }),
    };

    const listener = new StatelessListener(
      protocol,
      duplex,
      { requestTimeout: 100 }, // Short timeout
      Protocol.create,
    );

    const errors: Error[] = [];
    listener.addEventListener("error", (event) => {
      const errorEvent = event as ErrorEvent;
      if (errorEvent.error instanceof Error) {
        errors.push(errorEvent.error);
      }
    });

    await waitForListenerClose(listener);

    assertEquals(errors.length, 1);
    assertEquals(errors[0].message, "request timeout");

    listener.destroy();
  });

  it("handles protocol factory non-Error throw", async () => {
    const mockDuplex = new MockDuplex();
    const handshakeRequest: HandshakeRequestInit = {
      clientHash: createHash(13),
      clientProtocol: '{"protocol":"Test","messages":{}}', // Valid JSON
      serverHash: protocol.getHashBytes(),
    };
    const framedRequest = await frameTestRequest(handshakeRequest);
    mockDuplex.setResponse(framedRequest);

    const failingProtocolFactory = () => {
      throw "protocol factory non-error failure"; // Throw a string, not an Error
    };

    const listener = new StatelessListener(
      protocol,
      mockDuplex,
      {},
      failingProtocolFactory,
    );

    await waitForListenerClose(listener);

    const response = await decodeCallResponse(
      chunkToBuffer(mockDuplex.sent[0]),
      {
        responseType: message.responseType,
        errorType: message.errorType,
        expectHandshake: true,
      },
    );
    assertEquals(response.handshake?.match, "NONE");
    const errorMeta = response.handshake?.meta?.get("error");
    assertEquals(
      textDecoder.decode(errorMeta ?? new Uint8Array()),
      "Error: invalid client protocol JSON",
    );

    listener.destroy();
  });
});
