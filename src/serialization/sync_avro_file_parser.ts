import { createType, type SchemaLike } from "../type/create_type.ts";
import type { Resolver } from "../schemas/resolver.ts";
import { Type } from "../schemas/type.ts";
import { BLOCK_TYPE, HEADER_TYPE, MAGIC_BYTES } from "./avro_constants.ts";
import type { AvroHeader, ParsedAvroHeader } from "./avro_file_parser.ts";
import type { ISyncReadable } from "./buffers/sync_buffer.ts";
import type {
  SyncDecoder,
  SyncDecoderRegistry,
} from "./decoders/sync_decoder.ts";
import { NullSyncDecoder } from "./decoders/null_sync_decoder.ts";
import { SyncReadableTap } from "./sync_tap.ts";

/**
 * Options for configuring the synchronous Avro file parser.
 */
export interface SyncAvroFileParserOptions {
  /** Optional reader schema used when resolving writer data to a different schema. */
  readerSchema?: unknown;
  /** Custom codec decoders. Cannot include "null" as it is built-in. */
  decoders?: SyncDecoderRegistry;
}

/**
 * Parser for Avro object container files that operates purely synchronously.
 * It mirrors {@link AvroFileParser} but leverages {@link SyncReadableTap} and
 * {@link ISyncReadable} buffers so callers can avoid async control flow.
 */
export class SyncAvroFileParser {
  #buffer: ISyncReadable;
  #header: AvroHeader | undefined;
  #headerTap: SyncReadableTap | undefined;
  #readerSchema: unknown;
  #readerType: Type | undefined;
  #resolver: Resolver | undefined;
  #customDecoders: SyncDecoderRegistry;
  #builtInDecoders: SyncDecoderRegistry;

  constructor(buffer: ISyncReadable, options?: SyncAvroFileParserOptions) {
    this.#buffer = buffer;
    this.#readerSchema = options?.readerSchema;
    this.#builtInDecoders = {
      "null": new NullSyncDecoder(),
    };

    const custom = options?.decoders ?? {};
    for (const codec of Object.keys(custom)) {
      if (codec in this.#builtInDecoders) {
        throw new Error(`Cannot override built-in decoder for codec: ${codec}`);
      }
    }

    this.#customDecoders = { ...custom };
  }

  /**
   * Returns the parsed Avro header. Header parsing happens at most once.
   */
  public getHeader(): ParsedAvroHeader {
    const header = this.#parseHeader();
    return {
      magic: header.magic,
      meta: header.meta,
      sync: header.sync,
    };
  }

  /**
   * Iterates synchronously over every record contained in the Avro file.
   */
  public *iterRecords(): IterableIterator<unknown> {
    const header = this.#parseHeader();
    const { schemaType, meta } = header;
    const resolver = this.#getResolver(schemaType);

    const codecBytes = meta.get("avro.codec");
    const codec = (() => {
      if (codecBytes === undefined) {
        return "null";
      }
      const decoded = new TextDecoder().decode(codecBytes);
      return decoded.length === 0 ? "null" : decoded;
    })();
    const decoder = this.#getDecoder(codec);

    const tap = this.#headerTap!;
    while (tap.canReadMore()) {
      const block = BLOCK_TYPE.readSync(tap) as {
        count: bigint;
        data: Uint8Array;
        sync: Uint8Array;
      };

      const decompressed = decoder.decode(block.data);
      const arrayBuffer = new ArrayBuffer(decompressed.length);
      new Uint8Array(arrayBuffer).set(decompressed);
      const recordTap = new SyncReadableTap(arrayBuffer);

      for (let i = 0n; i < block.count; i += 1n) {
        const value = resolver
          ? resolver.readSync(recordTap)
          : schemaType.readSync(recordTap);
        yield value;
      }
    }
  }

  /**
   * Parses the Avro header and caches it. Subsequent calls reuse cached data.
   */
  #parseHeader(): AvroHeader {
    if (this.#header) {
      return this.#header;
    }

    const tap = new SyncReadableTap(this.#buffer);
    const header = HEADER_TYPE.readSync(tap);

    const magic = (header as Record<string, unknown>).magic as Uint8Array;
    for (let i = 0; i < MAGIC_BYTES.length; i++) {
      if (magic[i] !== MAGIC_BYTES[i]) {
        throw new Error("Invalid AVRO file: incorrect magic bytes");
      }
    }

    const meta = (header as Record<string, unknown>).meta as Map<
      string,
      Uint8Array
    >;

    const schemaJson = meta.get("avro.schema");
    if (!schemaJson) {
      throw new Error("AVRO schema not found in metadata");
    }
    const schemaStr = new TextDecoder().decode(schemaJson);
    const schemaType = createType(JSON.parse(schemaStr));

    const codec = meta.get("avro.codec");
    if (codec && codec.length > 0) {
      const codecStr = new TextDecoder().decode(codec);
      this.#getDecoder(codecStr);
    }

    const sync = (header as Record<string, unknown>).sync as Uint8Array;

    this.#header = {
      magic,
      meta,
      sync,
      schemaType,
    };
    this.#headerTap = tap;
    return this.#header;
  }

  #getResolver(writerType: Type): Resolver | undefined {
    if (this.#readerSchema === undefined || this.#readerSchema === null) {
      return undefined;
    }

    if (!this.#readerType) {
      this.#readerType = this.#createReaderType(
        this.#readerSchema as SchemaLike,
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

  #getDecoder(codec: string): SyncDecoder {
    if (codec in this.#builtInDecoders) {
      return this.#builtInDecoders[codec]!;
    }

    if (codec in this.#customDecoders) {
      return this.#customDecoders[codec]!;
    }

    throw new Error(`Unsupported codec: ${codec}. Provide a custom decoder.`);
  }
}
