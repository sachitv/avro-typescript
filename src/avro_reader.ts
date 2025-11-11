import { type IReadableBuffer } from "./internal/serialization/buffers/buffer.ts";
import { BlobReadableBuffer } from "./internal/serialization/buffers/blob_readable_buffer.ts";
import { StreamReadableBuffer } from "./internal/serialization/streams/stream_readable_buffer.ts";
import { FixedSizeStreamReadableBufferAdapter } from "./internal/serialization/streams/fixed_size_stream_readable_buffer_adapter.ts";
import { ForwardOnlyStreamReadableBufferAdapter } from "./internal/serialization/streams/forward_only_stream_readable_buffer_adapter.ts";
import {
  AvroFileParser,
  type ParsedAvroHeader,
} from "./internal/serialization/avro_file_parser.ts";

// Re-export for test utilities
export type { ParsedAvroHeader };

interface ReaderSchemaOptions {
  /** Optional reader schema used to resolve records written with a different schema. */
  readerSchema?: unknown;
}

/**
 * Options for URL-based reading.
 */
export interface FromUrlOptions extends ReaderSchemaOptions {
  /** Cache size for stream buffering in bytes (default: 0 = unlimited) */
  cacheSize?: number;
  /** Custom fetch init options (optional) */
  fetchInit?: RequestInit;
}

/**
 * Options for stream-based reading.
 */
export interface FromStreamOptions extends ReaderSchemaOptions {
  /** Cache size for stream buffering in bytes (default: 0 = unlimited) */
  cacheSize?: number;
}

/**
 * Options for buffer- or blob-based reading.
 */
export type FromBufferOptions = ReaderSchemaOptions;

/**
 * Interface for Avro reader instances that provide access to header and records.
 */
export interface AvroReaderInstance extends AsyncIterableIterator<unknown> {
  /**
   * Gets the parsed Avro file header with proper typing.
   *
   * @returns Promise that resolves to the parsed header information.
   */
  getHeader(): Promise<ParsedAvroHeader>;
}

/**
 * Default cache size: 0 (unlimited, uses forward-only adapter)
 */
const DEFAULT_CACHE_SIZE = 0;

/**
 * Internal implementation of AvroReaderInstance that wraps AvroFileParser.
 */
class AvroReaderInstanceImpl implements AvroReaderInstance {
  #parser: AvroFileParser;
  #recordIterator: AsyncIterableIterator<unknown>;

  constructor(parser: AvroFileParser) {
    this.#parser = parser;
    this.#recordIterator = parser.iterRecords();
  }

  async getHeader(): Promise<ParsedAvroHeader> {
    return await this.#parser.getHeader();
  }

  async next(): Promise<IteratorResult<unknown>> {
    return await this.#recordIterator.next();
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<unknown> {
    return this;
  }
}

/**
 * Public API for reading Avro object container files from various sources.
 *
 * This class provides static factory methods for creating Avro readers from
 * different data sources while maintaining a consistent interface.
 *
 * @example
 * ```typescript
 * const readerSchema = {
 *   type: "record",
 *   name: "test.Weather",
 *   fields: [
 *     { name: "station", type: "string" },
 *     { name: "temp", type: "int" },
 *   ],
 * };
 *
 * // Read from a buffer (for example when the Avro file is already in memory)
 * const bufferReader = AvroReader.fromBuffer(buffer, { readerSchema });
 * for await (const record of bufferReader) {
 *   console.log(record);
 * }
 *
 * // Read from a blob (e.g., file input)
 * const blobReader = AvroReader.fromBlob(blob, { readerSchema });
 * for await (const record of blobReader) {
 *   console.log(record);
 * }
 *
 * // Read from a URL with unlimited buffering
 * const urlReader = await AvroReader.fromUrl("data.avro", { readerSchema });
 * for await (const record of urlReader) {
 *   console.log(record);
 * }
 *
 * // Read from a URL with limited memory usage
 * const cachedUrlReader = await AvroReader.fromUrl("data.avro", {
 *   cacheSize: 1024 * 1024,
 *   readerSchema,
 * });
 * for await (const record of cachedUrlReader) {
 *   console.log(record);
 * }
 *
 * // Read from a stream with optional caching
 * const streamReader = AvroReader.fromStream(stream, {
 *   cacheSize: 1024,
 *   readerSchema,
 * });
 * for await (const record of streamReader) {
 *   console.log(record);
 * }
 * ```
 */
export class AvroReader {
  /**
   * Creates an Avro reader from any readable buffer.
   *
   * This is the core method that all other factory methods delegate to.
   *
   * @param buffer The readable buffer containing Avro data.
   * @returns AvroReaderInstance that provides access to header and records.
   */
  public static fromBuffer(
    buffer: IReadableBuffer,
    options?: FromBufferOptions,
  ): AvroReaderInstance {
    const parser = new AvroFileParser(buffer, {
      readerSchema: options?.readerSchema,
    });
    return new AvroReaderInstanceImpl(parser);
  }

  /**
   * Creates an Avro reader from a Blob.
   *
   * Uses BlobReadableBuffer for efficient random access without loading
   * the entire blob into memory.
   *
   * @param blob The blob containing Avro data.
   * @returns AvroReaderInstance that provides access to header and records.
   */
  public static fromBlob(
    blob: Blob,
    options?: FromBufferOptions,
  ): AvroReaderInstance {
    const buffer = new BlobReadableBuffer(blob);
    return AvroReader.fromBuffer(buffer, options);
  }

  /**
   * Creates an Avro reader from a URL.
   *
   * Fetches the URL and creates a stream-based reader. The caching strategy
   * depends on the cacheSize option:
   * - cacheSize = 0 (default): Uses ForwardOnlyStreamReadableBufferAdapter for unlimited buffering
   * - cacheSize > 0: Uses FixedSizeStreamReadableBufferAdapter with the specified window size
   *
   * @param url The URL to fetch Avro data from.
   * @param options Configuration options for URL reading.
   * @returns Promise that resolves to AvroReaderInstance providing access to header and records.
   */
  public static async fromUrl(
    url: string,
    options?: FromUrlOptions,
  ): Promise<AvroReaderInstance> {
    const response = await fetch(url, options?.fetchInit);
    if (!response.ok) {
      throw new Error(
        `Failed to fetch ${url}: ${response.status} ${response.statusText}`,
      );
    }

    if (!response.body) {
      throw new Error(`Response body is null for ${url}`);
    }

    const stream = response.body;
    return AvroReader.fromStream(stream, {
      cacheSize: options?.cacheSize,
      readerSchema: options?.readerSchema,
    });
  }

  /**
   * Creates an Avro reader from a ReadableStream.
   *
   * Creates a stream-based reader with configurable caching. The caching strategy
   * depends on the cacheSize option:
   * - cacheSize = 0 (default): Uses ForwardOnlyStreamReadableBufferAdapter for unlimited buffering
   * - cacheSize > 0: Uses FixedSizeStreamReadableBufferAdapter with the specified window size
   *
   * @param stream The readable stream containing Avro data.
   * @param options Configuration options for stream reading.
   * @returns AvroReaderInstance that provides access to header and records.
   */
  public static fromStream(
    stream: ReadableStream,
    options?: FromStreamOptions,
  ): AvroReaderInstance {
    const cacheSize = options?.cacheSize ?? DEFAULT_CACHE_SIZE;
    const streamBuffer = new StreamReadableBuffer(stream);

    let buffer: IReadableBuffer;
    if (cacheSize > 0) {
      // Use fixed-size adapter for limited memory usage
      buffer = new FixedSizeStreamReadableBufferAdapter(
        streamBuffer,
        cacheSize,
      );
    } else {
      // Use forward-only adapter for unlimited buffering
      buffer = new ForwardOnlyStreamReadableBufferAdapter(streamBuffer);
    }

    return AvroReader.fromBuffer(buffer, {
      readerSchema: options?.readerSchema,
    });
  }
}
