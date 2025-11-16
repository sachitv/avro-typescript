import { assert, assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type { WebSocketTransportOptions } from "./transport_helpers.ts";
import { createWebSocketTransport } from "./websocket.ts";

class MockWebSocket extends EventTarget implements WebSocket {
  binaryType: BinaryType = "arraybuffer";
  bufferedAmount = 0;
  extensions = "";
  protocol = "";
  readyState = WebSocket.CONNECTING;
  url: string;
  onclose: ((this: WebSocket, ev: CloseEvent) => unknown) | null = null;
  onerror: ((this: WebSocket, ev: Event) => unknown) | null = null;
  onmessage: ((this: WebSocket, ev: MessageEvent) => unknown) | null = null;
  onopen: ((this: WebSocket, ev: Event) => unknown) | null = null;
  readonly CONNECTING = WebSocket.CONNECTING;
  readonly OPEN = WebSocket.OPEN;
  readonly CLOSING = WebSocket.CLOSING;
  readonly CLOSED = WebSocket.CLOSED;

  sent: Uint8Array[] = [];

  constructor(url = "wss://example.test/ws") {
    super();
    this.url = url;
    queueMicrotask(() => {
      this.readyState = WebSocket.OPEN;
      const event = new Event("open");
      this.dispatchEvent(event);
      this.onopen?.call(this as unknown as WebSocket, event);
    });
  }

  close(): void {
    if (this.readyState === WebSocket.CLOSED) {
      return;
    }
    this.readyState = WebSocket.CLOSED;
    const event = new CloseEvent("close");
    this.dispatchEvent(event);
    this.onclose?.call(this as unknown as WebSocket, event);
  }

  send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    if (typeof data === "string") {
      throw new Error("MockWebSocket expects binary data.");
    }
    if (data instanceof ArrayBuffer) {
      this.sent.push(new Uint8Array(data));
      return;
    }
    if (ArrayBuffer.isView(data)) {
      const view = data as ArrayBufferView;
      this.sent.push(
        new Uint8Array(
          view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength),
        ),
      );
      return;
    }
    throw new Error("Unsupported send payload.");
  }

  emit(chunk: Uint8Array): void {
    const event = new MessageEvent("message", { data: chunk });
    this.dispatchEvent(event);
    this.onmessage?.call(this as unknown as WebSocket, event);
  }

  accept(): void {}
  ping(): void {}
  pong(): void {}
}

describe("createWebSocketTransport", () => {
  it("builds a stateless factory", async () => {
    const mock = new MockWebSocket();
    const factory = createWebSocketTransport("wss://example.test/ws", {
      socketFactory: () => mock,
    });
    const transport = await factory();

    const writable = transport.writable as WritableStream<Uint8Array>;
    const writer = writable.getWriter();
    await writer.write(new Uint8Array([1, 2, 3]));
    assertEquals(mock.sent.length, 1);

    const readable = transport.readable as ReadableStream<Uint8Array>;
    const reader = readable.getReader();
    mock.emit(new Uint8Array([7, 8]));
    const { value } = await reader.read();
    assertEquals(value, new Uint8Array([7, 8]));
    await reader.cancel();
    reader.releaseLock();
    await writer.close();
    writer.releaseLock();
    mock.close();
  });

  it("rejects writes when socket is not open", async () => {
    const mock = new MockWebSocket();
    const factory = createWebSocketTransport("wss://example.test/ws", {
      socketFactory: () => mock,
    });
    const transport = await factory();
    const writer = (transport.writable as WritableStream<Uint8Array>)
      .getWriter();
    mock.close();
    await assertRejects(
      () => writer.write(new Uint8Array([1, 2, 3])),
      Error,
      "WebSocket is not open.",
    );
    writer.releaseLock();
  });

  it("closes the socket when writer aborts", async () => {
    const mock = new MockWebSocket();
    const factory = createWebSocketTransport("wss://example.test/ws", {
      socketFactory: () => mock,
    });
    const transport = await factory();
    const writer = (transport.writable as WritableStream<Uint8Array>)
      .getWriter();
    await writer.abort(new Error("fail"));
    writer.releaseLock();
    assertEquals(mock.readyState, WebSocket.CLOSED);
  });

  it("creates transport with default options", async () => {
    const originalWebSocket = globalThis.WebSocket;

    class DefaultMock {
      readyState = WebSocket.OPEN;
      binaryType = "arraybuffer";
      addEventListener = () => {};
      removeEventListener = () => {};
      close = () => {};
      send = () => {};
    }

    (globalThis as unknown as { WebSocket: typeof DefaultMock }).WebSocket =
      DefaultMock;
    const transportFactory = createWebSocketTransport("ws://example.com");
    const transport = await transportFactory();
    assert(transport.readable instanceof ReadableStream);
    assert(transport.writable instanceof WritableStream);

    globalThis.WebSocket = originalWebSocket;
  });

  it("creates transport with custom options", async () => {
    const originalWebSocket = globalThis.WebSocket;

    let capturedProtocols: string | string[] | undefined;
    class CustomMock {
      readyState = WebSocket.OPEN;
      binaryType = "blob";
      constructor(_url: string, protocols?: string | string[]) {
        capturedProtocols = protocols;
      }
      addEventListener = () => {};
      removeEventListener = () => {};
      close = () => {};
      send = () => {};
    }

    (globalThis as unknown as { WebSocket: typeof CustomMock }).WebSocket =
      CustomMock;
    const options: WebSocketTransportOptions = {
      protocols: ["proto1", "proto2"],
      binaryType: "blob",
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    await transportFactory();
    assertEquals(capturedProtocols, ["proto1", "proto2"]);

    globalThis.WebSocket = originalWebSocket;
  });

  it("creates transport using socketFactory", async () => {
    const mockSocket = {
      readyState: WebSocket.OPEN,
      binaryType: "arraybuffer",
      addEventListener: () => {},
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    const transport = await transportFactory();
    assert(transport.readable instanceof ReadableStream);
    assert(transport.writable instanceof WritableStream);
  });

  it("waits for WebSocket to open", async () => {
    let openHandler: (() => void) | undefined;
    const mockSocket = {
      readyState: WebSocket.CONNECTING,
      binaryType: "arraybuffer",
      addEventListener: (event: string, handler: () => void) => {
        if (event === "open") openHandler = handler;
      },
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    const promise = transportFactory();
    const timer = setTimeout(() => openHandler?.(), 10);
    await promise;
    await Promise.resolve();
    clearTimeout(timer);
  });

  it("handles WebSocket connection error", async () => {
    let errorHandler: (() => void) | undefined;
    const mockSocket = {
      readyState: WebSocket.CONNECTING,
      binaryType: "arraybuffer",
      addEventListener: (event: string, handler: () => void) => {
        if (event === "error") errorHandler = handler;
      },
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    const promise = transportFactory();
    queueMicrotask(() => errorHandler?.());
    await assertRejects(
      () => promise,
      Error,
      "WebSocket connection failed.",
    );
  });

  it("handles connection timeout", async () => {
    const mockSocket = {
      readyState: WebSocket.CONNECTING,
      binaryType: "arraybuffer",
      addEventListener: () => {},
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
      connectTimeoutMs: 10,
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    await assertRejects(
      () => transportFactory(),
      Error,
      "WebSocket connection timed out.",
    );
  });

  it("handles WebSocket message in readable stream", async () => {
    let messageTimer: number | undefined;
    const mockSocket = {
      readyState: WebSocket.OPEN,
      binaryType: "arraybuffer",
      addEventListener: (event: string, handler: (e: MessageEvent) => void) => {
        if (event === "message") {
          messageTimer = setTimeout(
            () => handler({ data: new Uint8Array([1, 2, 3]) } as MessageEvent),
            0,
          );
        }
      },
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    const transport = await transportFactory();
    const reader = (transport.readable as ReadableStream<Uint8Array>)
      .getReader();
    const { value } = await reader.read();
    if (messageTimer !== undefined) {
      clearTimeout(messageTimer);
    }
    assertEquals(value, new Uint8Array([1, 2, 3]));
    reader.releaseLock();
  });

  it("handles WebSocket close in readable stream", async () => {
    let closeHandler: (() => void) | undefined;
    const mockSocket = {
      readyState: 1,
      binaryType: "arraybuffer",
      addEventListener: (event: string, handler: () => void) => {
        if (event === "close") closeHandler = handler;
      },
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    const transport = await transportFactory();
    const reader = (transport.readable as ReadableStream<Uint8Array>)
      .getReader();
    const readPromise = reader.read();
    const timer = setTimeout(() => closeHandler?.(), 10);
    const { done } = await readPromise;
    clearTimeout(timer);
    assert(done);
    reader.releaseLock();
  });

  it("errors when WebSocket emits error in readable stream", async () => {
    let errorHandler: (() => void) | undefined;
    const mockSocket = {
      readyState: WebSocket.OPEN,
      binaryType: "arraybuffer",
      addEventListener: (event: string, handler: () => void) => {
        if (event === "error") errorHandler = handler;
      },
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    const transport = await transportFactory();
    const reader = (transport.readable as ReadableStream<Uint8Array>)
      .getReader();
    queueMicrotask(() => errorHandler?.());
    await assertRejects(
      () => reader.read(),
      Error,
      "WebSocket error.",
    );
    reader.releaseLock();
  });

  it("extracts binary from ArrayBufferView message", async () => {
    let messageTimer: number | undefined;
    const mockSocket = {
      readyState: 1,
      binaryType: "arraybuffer",
      addEventListener: (event: string, handler: (e: MessageEvent) => void) => {
        if (event === "message") {
          const buffer = new ArrayBuffer(6);
          const view = new Int8Array(buffer, 1, 3);
          view.set([1, 2, 3]);
          messageTimer = setTimeout(
            () => handler({ data: view } as MessageEvent),
            10,
          );
        }
      },
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    const transport = await transportFactory();
    const reader = (transport.readable as ReadableStream<Uint8Array>)
      .getReader();
    const { value } = await reader.read();
    if (messageTimer !== undefined) {
      clearTimeout(messageTimer);
    }
    assertEquals(value, new Uint8Array([1, 2, 3]));
    reader.releaseLock();
  });

  it("extracts binary from string message", async () => {
    let messageTimer: number | undefined;
    const mockSocket = {
      readyState: 1,
      binaryType: "arraybuffer",
      addEventListener: (event: string, handler: (e: MessageEvent) => void) => {
        if (event === "message") {
          messageTimer = setTimeout(
            () => handler({ data: "hello" } as MessageEvent),
            10,
          );
        }
      },
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    const transport = await transportFactory();
    const reader = (transport.readable as ReadableStream<Uint8Array>)
      .getReader();
    const { value } = await reader.read();
    if (messageTimer !== undefined) {
      clearTimeout(messageTimer);
    }
    assertEquals(value, new TextEncoder().encode("hello"));
    reader.releaseLock();
  });

  it("throws on Blob message", async () => {
    let messageTimer: number | undefined;
    const mockSocket = {
      readyState: 1,
      binaryType: "blob",
      addEventListener: (event: string, handler: (e: MessageEvent) => void) => {
        if (event === "message") {
          const blob = new Blob([new Uint8Array([1, 2, 3])]);
          messageTimer = setTimeout(
            () => handler({ data: blob } as MessageEvent),
            10,
          );
        }
      },
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
      binaryType: "blob",
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    const transport = await transportFactory();
    const reader = (transport.readable as ReadableStream<Uint8Array>)
      .getReader();
    await assertRejects(
      () => reader.read(),
      Error,
      "Blob WebSocket messages are not supported.",
    );
    if (messageTimer !== undefined) {
      clearTimeout(messageTimer);
    }
    reader.releaseLock();
  });

  it("throws on unsupported message type", async () => {
    let messageTimer: number | undefined;
    const mockSocket = {
      readyState: 1,
      binaryType: "arraybuffer",
      addEventListener: (event: string, handler: (e: MessageEvent) => void) => {
        if (event === "message") {
          messageTimer = setTimeout(
            () => handler({ data: 123 } as unknown as MessageEvent),
            10,
          );
        }
      },
      removeEventListener: () => {},
      close: () => {},
      send: () => {},
    };
    const options: WebSocketTransportOptions = {
      socketFactory: () => mockSocket as unknown as WebSocket,
    };
    const transportFactory = createWebSocketTransport(
      "ws://example.com",
      options,
    );
    const transport = await transportFactory();
    const reader = (transport.readable as ReadableStream<Uint8Array>)
      .getReader();
    await assertRejects(
      () => reader.read(),
      Error,
      "Unsupported WebSocket message data.",
    );
    if (messageTimer !== undefined) {
      clearTimeout(messageTimer);
    }
    reader.releaseLock();
  });

  it("supports URL endpoints", async () => {
    const originalWebSocket = globalThis.WebSocket;

    class UrlMock {
      readyState = WebSocket.OPEN;
      binaryType = "arraybuffer";
      addEventListener = () => {};
      removeEventListener = () => {};
      close = () => {};
      send = () => {};
    }

    (globalThis as unknown as { WebSocket: typeof UrlMock }).WebSocket =
      UrlMock;
    const transportFactory = createWebSocketTransport(
      new URL("ws://example.com"),
    );
    const transport = await transportFactory();
    assert(transport.readable instanceof ReadableStream);
    assert(transport.writable instanceof WritableStream);

    globalThis.WebSocket = originalWebSocket;
  });
});
