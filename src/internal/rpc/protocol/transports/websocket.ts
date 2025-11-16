import type {
  StatelessTransportFactory,
  WebSocketTransportOptions,
} from "./transport_helpers.ts";
import { extractBinary } from "./transport_helpers.ts";

export function createWebSocketTransport(
  endpoint: string | URL,
  options: WebSocketTransportOptions = {},
): StatelessTransportFactory {
  return async () => {
    const socket = options.socketFactory
      ? options.socketFactory()
      : new WebSocket(
        typeof endpoint === "string" ? endpoint : endpoint.toString(),
        options.protocols,
      );
    socket.binaryType = options.binaryType ?? "arraybuffer";
    await waitForOpen(socket, options.connectTimeoutMs);
    return {
      readable: websocketReadable(socket),
      writable: websocketWritable(socket),
    };
  };
}

function waitForOpen(socket: WebSocket, timeout?: number): Promise<void> {
  if (socket.readyState === WebSocket.OPEN) {
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    let timer: number | undefined;
    const cleanup = () => {
      socket.removeEventListener("open", handleOpen);
      socket.removeEventListener("error", handleError);
      if (timer !== undefined) {
        clearTimeout(timer);
      }
    };
    const handleOpen = () => {
      cleanup();
      resolve();
    };
    const handleError = () => {
      cleanup();
      reject(new Error("WebSocket connection failed."));
    };
    socket.addEventListener("open", handleOpen, { once: true });
    socket.addEventListener("error", handleError, { once: true });
    if (timeout !== undefined) {
      timer = setTimeout(() => {
        cleanup();
        reject(new Error("WebSocket connection timed out."));
      }, timeout);
    }
  });
}

class WebSocketReadableStream extends ReadableStream<Uint8Array> {
  constructor(socket: WebSocket) {
    let cleanup: (() => void) | undefined;

    const start = (controller: ReadableStreamDefaultController<Uint8Array>) => {
      const handleMessage = (event: MessageEvent) => {
        try {
          controller.enqueue(extractBinary(event.data));
        } catch (err) {
          controller.error(err);
        }
      };
      const handleClose = () => controller.close();
      const handleError = () => controller.error(new Error("WebSocket error."));

      socket.addEventListener("message", handleMessage);
      socket.addEventListener("close", handleClose, { once: true });
      socket.addEventListener("error", handleError, { once: true });
      cleanup = () => {
        socket.removeEventListener("message", handleMessage);
        socket.removeEventListener("close", handleClose);
        socket.removeEventListener("error", handleError);
      };
    };

    const cancel = (): Promise<void> => {
      cleanup?.();
      socket.close();
      return Promise.resolve();
    };

    super({ start, cancel });
  }
}

function websocketReadable(socket: WebSocket): ReadableStream<Uint8Array> {
  return new WebSocketReadableStream(socket);
}

function websocketWritable(socket: WebSocket): WritableStream<Uint8Array> {
  return new WritableStream<Uint8Array>({
    write(chunk) {
      if (socket.readyState !== WebSocket.OPEN) {
        throw new Error("WebSocket is not open.");
      }
      socket.send(chunk);
    },
    close() {
      socket.close();
    },
    abort() {
      socket.close();
    },
  });
}
