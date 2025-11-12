import { createType, type SchemaLike } from "../createType/mod.ts";
import type { Resolver } from "../schemas/resolver.ts";
import { Type } from "../schemas/type.ts";
import { ReadableTap } from "./tap.ts";
import type { IReadableBuffer } from "./buffers/buffer.ts";
import {
  type Decoder,
  type DecoderRegistry,
  DeflateDecoder,
  NullDecoder,
} from "./decoders/mod.ts";
import { BLOCK_TYPE, HEADER_TYPE, MAGIC_BYTES } from "./avro_constants.ts";

// Re-export types for backward compatibility
export type { Decoder, DecoderRegistry };

// Re-export constants for backward compatibility
export { BLOCK_TYPE, HEADER_TYPE, MAGIC_BYTES };

/**
 * Internal interface for Avro file header information.
 */
export interface AvroHeader {
  magic: Uint8Array;
  meta: Map<string, Uint8Array>;
  sync: Uint8Array;
  schemaType: Type;
}

/**
 * Public interface for parsed Avro file header with proper typing.
 */
export interface ParsedAvroHeader {
  magic: Uint8Array;
  meta: Map<string, Uint8Array>;
  sync: Uint8Array;
}

/**
 * Internal parser for Avro object container files.
 * Handles header parsing and record iteration.
 */
export interface AvroFileParserOptions {
  /** Optional reader schema used to resolve records written with a different schema. */
  readerSchema?: unknown;
  /** Custom codec decoders. Cannot include "null" or "deflate" as they are built-in. */
  decoders?: DecoderRegistry;
}

export class AvroFileParser {
  #buffer: IReadableBuffer;
  #header: AvroHeader | undefined;
  #headerTap: ReadableTap | undefined;
  #readerSchema: unknown;
  #readerType: Type | undefined;
  #resolver: Resolver | undefined;
  #decoders: DecoderRegistry;
  #builtInDecoders: DecoderRegistry;

  /**
   * Creates a new AvroFileParser.
   *
   * @param buffer The readable buffer containing Avro data.
   * @param options Configuration options for parsing.
   */
  public constructor(buffer: IReadableBuffer, options?: AvroFileParserOptions) {
    this.#buffer = buffer;
    this.#readerSchema = options?.readerSchema;

    // Initialize built-in decoders
    this.#builtInDecoders = {
      "null": new NullDecoder(),
      "deflate": new DeflateDecoder(),
    };

    // Validate custom decoders (cannot override built-ins)
    const customDecoders = options?.decoders || {};
    for (const codec of Object.keys(customDecoders)) {
      if (codec in this.#builtInDecoders) {
        throw new Error(`Cannot override built-in decoder for codec: ${codec}`);
      }
    }

    this.#decoders = { ...customDecoders };
  }

  /**
   * Gets the parsed Avro file header with proper typing.
   *
   * @returns Promise that resolves to the parsed header information.
   * @throws Error if the file is not a valid Avro file.
   */
  public async getHeader(): Promise<ParsedAvroHeader> {
    const header = await this.#parseHeader();

    return {
      magic: header.magic,
      meta: header.meta,
      sync: header.sync,
    };
  }

  /**
   * Gets the decoder for the specified codec.
   *
   * @param codec The codec name (e.g., "null", "deflate", or custom codec)
   * @returns The decoder instance
   * @throws Error if codec is not supported
   */
  #getDecoder(codec: string): Decoder {
    // Check built-in decoders first
    if (codec in this.#builtInDecoders) {
      return this.#builtInDecoders[codec];
    }

    // Check custom decoders
    if (codec in this.#decoders) {
      return this.#decoders[codec];
    }

    throw new Error(`Unsupported codec: ${codec}. Provide a custom decoder.`);
  }

  /**
   * Asynchronously iterates over all records in the Avro file.
   *
   * @returns AsyncIterableIterator that yields each record.
   */
  public async *iterRecords(): AsyncIterableIterator<unknown> {
    const header = await this.#parseHeader();
    const { schemaType, meta } = header;
    const resolver = this.#getResolver(schemaType);

    // Get the codec from metadata
    const codecBytes = meta.get("avro.codec");
    const codecStr = (() => {
      if (codecBytes === undefined) {
        return "null";
      }
      const decoded = new TextDecoder().decode(codecBytes);
      return decoded.length === 0 ? "null" : decoded;
    })();
    const decoder = this.#getDecoder(codecStr);

    // Use the tap that's positioned after the header
    const tap = this.#headerTap!;

    while (true) {
      try {
        const block = await BLOCK_TYPE.read(tap) as {
          count: bigint;
          data: Uint8Array;
          sync: Uint8Array;
        };

        // Decompress block data if needed
        const decompressedData = await decoder.decode(block.data);
        const arrayBuffer = new ArrayBuffer(decompressedData.length);
        new Uint8Array(arrayBuffer).set(decompressedData);
        const recordTap = new ReadableTap(arrayBuffer);

        // Yield each record in the block
        for (let i = 0n; i < block.count; i += 1n) {
          const record = resolver
            ? await resolver.read(recordTap)
            : await schemaType.read(recordTap);
          yield record;
        }
      } catch (_error) {
        // No more blocks or invalid data
        break;
      }
    }
  }

  /**
   * Private method to parse the Avro file header and cache it.
   *
   * @returns Promise that resolves to the parsed header information.
   * @throws Error if the file is not a valid Avro file.
   */
  async #parseHeader(): Promise<AvroHeader> {
    if (this.#header) {
      return this.#header;
    }

    const tap = new ReadableTap(this.#buffer);
    const header = await HEADER_TYPE.read(tap);

    // Validate magic bytes
    const magic = (header as Record<string, unknown>).magic as Uint8Array;
    for (let i = 0; i < MAGIC_BYTES.length; i++) {
      if (magic[i] !== MAGIC_BYTES[i]) {
        throw new Error("Invalid AVRO file: incorrect magic bytes");
      }
    }

    // Extract metadata
    const meta = (header as Record<string, unknown>).meta as Map<
      string,
      Uint8Array
    >;

    // Read and parse the schema
    const schemaJson = meta.get("avro.schema");
    if (!schemaJson) {
      throw new Error("AVRO schema not found in metadata");
    }
    const schemaStr = new TextDecoder().decode(schemaJson);
    const schemaType = createType(JSON.parse(schemaStr));

    // Validate that we have a decoder for the codec
    const codec = meta.get("avro.codec");
    if (codec && codec.length > 0) {
      const codecStr = new TextDecoder().decode(codec);
      // This will throw if codec is not supported
      this.#getDecoder(codecStr);
    }

    const sync = (header as Record<string, unknown>).sync as Uint8Array;

    this.#header = {
      magic,
      meta,
      sync,
      schemaType,
    };

    // Store the tap at its current position for reading blocks
    this.#headerTap = tap;

    return this.#header;
  }

  #getResolver(writerType: Type): Resolver | undefined {
    if (this.#readerSchema === undefined || this.#readerSchema === null) {
      return undefined;
    }

    if (!this.#readerType) {
      this.#readerType = this.#createReaderType(
        this.#readerSchema as unknown as SchemaLike,
      );
    }

    if (!this.#resolver) {
      this.#resolver = this.#readerType.createResolver(writerType);
    }

    return this.#resolver;
  }

  #createReaderType(schema: SchemaLike): Type {
    if (schema instanceof Type) {
      return schema;
    }

    if (typeof schema === "string") {
      const trimmed = schema.trim();
      if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
        const parsed = JSON.parse(trimmed);
        return createType(parsed);
      }
      return createType(schema);
    }

    return createType(schema);
  }
}
