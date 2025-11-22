/**
 * Represents a readable stream for binary data in the transport layer.
 * Used for asynchronously reading chunks of binary data from a transport.
 */
export interface BinaryReadable {
  /**
   * Reads the next chunk of binary data from the transport.
   * Returns a Uint8Array if data is available, or null if the stream is ended.
   */
  read(): Promise<Uint8Array | null>;
}

/**
 * Represents a writable stream for binary data in the transport layer.
 * Used for asynchronously writing chunks of binary data to a transport.
 */
export interface BinaryWritable {
  /**
   * Writes a chunk of binary data to the transport.
   * @param chunk The binary data to write.
   */
  write(chunk: Uint8Array): Promise<void>;
  /**
   * Closes the writable stream, signaling the end of data transmission.
   */
  close(): Promise<void>;
}

/**
 * Represents a duplex stream for binary data in the transport layer.
 * Combines readable and writable streams for bidirectional communication.
 */
export interface BinaryDuplex {
  /** The readable side of the duplex stream. */
  readable: BinaryReadable;
  /** The writable side of the duplex stream. */
  writable: BinaryWritable;
}

/**
 * A type that can be either a BinaryReadable interface or a standard ReadableStream.
 * Used to accept various readable transport implementations in the transport layer.
 */
export type BinaryReadableLike = BinaryReadable | ReadableStream<Uint8Array>;
/**
 * A type that can be either a BinaryWritable interface or a standard WritableStream.
 * Used to accept various writable transport implementations in the transport layer.
 */
export type BinaryWritableLike = BinaryWritable | WritableStream<Uint8Array>;

/**
 * Represents a duplex-like stream that can be composed of different readable and writable types.
 * Used for flexible duplex transport configurations in the transport layer.
 */
export interface BinaryDuplexLike {
  /** The readable side, which can be a BinaryReadable or ReadableStream. */
  readable: BinaryReadableLike;
  /** The writable side, which can be a BinaryWritable or WritableStream. */
  writable: BinaryWritableLike;
}

/**
 * A factory function for creating stateless transport duplex streams.
 * Returns a promise that resolves to a BinaryDuplexLike for establishing connections in the transport layer.
 */
export type StatelessTransportFactory = () => Promise<BinaryDuplexLike>;

/**
 * Options for configuring fetch-based transports in the transport layer.
 * Allows customization of HTTP requests for RPC communication.
 */
export interface FetchTransportOptions {
  /** The HTTP method to use for the request (e.g., 'POST'). */
  method?: string;
  /** Headers to include in the request. */
  headers?: HeadersInit;
  /** Additional initialization options for the fetch request. */
  init?: RequestInit;
  /** Custom fetch function to use instead of the global fetch. */
  fetch?: typeof fetch;
}

/**
 * Options for configuring WebSocket-based transports in the transport layer.
 * Allows customization of WebSocket connections for RPC communication.
 */
export interface WebSocketTransportOptions {
  /** Subprotocols to negotiate during the WebSocket handshake. */
  protocols?: string | string[];
  /** The type of binary data to expect from the WebSocket (e.g., 'arraybuffer', 'blob'). */
  binaryType?: BinaryType;
  /** Timeout in milliseconds for establishing the WebSocket connection. */
  connectTimeoutMs?: number;
  /** Factory function to create a custom WebSocket instance. */
  socketFactory?: () => WebSocket;
}

const textEncoder = new TextEncoder();

/**
 * Converts a BinaryDuplexLike to a standardized BinaryDuplex.
 * Ensures consistent interface for duplex transports in the transport layer.
 * @param input The duplex-like object to convert.
 * @returns A BinaryDuplex with standardized readable and writable properties.
 */
export function toBinaryDuplex(input: BinaryDuplexLike): BinaryDuplex {
  return {
    readable: toBinaryReadable(input.readable),
    writable: toBinaryWritable(input.writable),
  };
}

/**
 * Converts a BinaryReadableLike to a standardized BinaryReadable.
 * Wraps ReadableStream or returns the interface directly for consistent reading in the transport layer.
 * @param source The readable-like object to convert.
 * @returns A BinaryReadable interface for asynchronous binary data reading.
 */
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

/**
 * Converts a BinaryWritableLike to a standardized BinaryWritable.
 * Wraps WritableStream or returns the interface directly for consistent writing in the transport layer.
 * @param target The writable-like object to convert.
 * @returns A BinaryWritable interface for asynchronous binary data writing.
 */
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

/**
 * Extracts binary data from various input types into a Uint8Array.
 * Used in the transport layer to normalize WebSocket message data for processing.
 * @param data The input data to extract binary from (Uint8Array, ArrayBuffer, ArrayBufferView, string, etc.).
 * @returns A Uint8Array representation of the binary data.
 * @throws Error if the data type is unsupported or Blob (not supported for WebSocket).
 */
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
