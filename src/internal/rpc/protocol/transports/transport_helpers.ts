export interface BinaryReadable {
  read(): Promise<Uint8Array | null>;
}

export interface BinaryWritable {
  write(chunk: Uint8Array): Promise<void>;
  close(): Promise<void>;
}

export interface BinaryDuplex {
  readable: BinaryReadable;
  writable: BinaryWritable;
}

export type BinaryReadableLike = BinaryReadable | ReadableStream<Uint8Array>;
export type BinaryWritableLike = BinaryWritable | WritableStream<Uint8Array>;

export interface BinaryDuplexLike {
  readable: BinaryReadableLike;
  writable: BinaryWritableLike;
}

export type StatelessTransportFactory = () => Promise<BinaryDuplexLike>;

export interface FetchTransportOptions {
  method?: string;
  headers?: HeadersInit;
  init?: RequestInit;
  fetch?: typeof fetch;
}

export interface WebSocketTransportOptions {
  protocols?: string | string[];
  binaryType?: BinaryType;
  connectTimeoutMs?: number;
  socketFactory?: () => WebSocket;
}

const textEncoder = new TextEncoder();

export function toBinaryDuplex(input: BinaryDuplexLike): BinaryDuplex {
  return {
    readable: toBinaryReadable(input.readable),
    writable: toBinaryWritable(input.writable),
  };
}

export function toBinaryReadable(source: BinaryReadableLike): BinaryReadable {
  if (isBinaryReadable(source)) {
    return source;
  }
  if (!isReadableStream(source)) {
    throw new TypeError("Unsupported readable transport.");
  }
  const reader = source.getReader();
  return {
    async read() {
      const { value, done } = await reader.read();
      if (done) {
        return null;
      } else {
        if (value === undefined) {
          return new Uint8Array(0);
        } else {
          return value;
        }
      }
    },
  };
}

export function toBinaryWritable(target: BinaryWritableLike): BinaryWritable {
  if (isBinaryWritable(target)) {
    return target;
  }
  if (!isWritableStream(target)) {
    throw new TypeError("Unsupported writable transport.");
  }
  const writer = target.getWriter();
  return {
    async write(chunk: Uint8Array) {
      await writer.write(chunk);
    },
    async close() {
      await writer.close();
    },
  };
}

export function extractBinary(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) {
    return data;
  }
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }
  if (ArrayBuffer.isView(data)) {
    const view = data as ArrayBufferView;
    return new Uint8Array(
      view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength),
    );
  }
  if (typeof Blob !== "undefined" && data instanceof Blob) {
    throw new Error("Blob WebSocket messages are not supported.");
  }
  if (typeof data === "string") {
    return textEncoder.encode(data);
  }
  throw new Error("Unsupported WebSocket message data.");
}

function isBinaryReadable(value: unknown): value is BinaryReadable {
  return typeof value === "object" &&
    value !== null &&
    typeof (value as BinaryReadable).read === "function";
}

function isBinaryWritable(value: unknown): value is BinaryWritable {
  return typeof value === "object" &&
    value !== null &&
    typeof (value as BinaryWritable).write === "function" &&
    typeof (value as BinaryWritable).close === "function";
}

function isReadableStream(
  value: BinaryReadableLike,
): value is ReadableStream<Uint8Array> {
  return typeof ReadableStream !== "undefined" &&
    value instanceof ReadableStream;
}

function isWritableStream(
  value: BinaryWritableLike,
): value is WritableStream<Uint8Array> {
  return typeof WritableStream !== "undefined" &&
    value instanceof WritableStream;
}
