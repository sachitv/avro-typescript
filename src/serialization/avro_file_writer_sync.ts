import { createType, type SchemaLike } from "../type/create_type.ts";
import type { Type } from "../schemas/type.ts";
import type { MetadataInit } from "./avro_file_writer.ts";
import { BLOCK_TYPE, HEADER_TYPE, MAGIC_BYTES } from "./avro_constants.ts";
import type { ISyncWritable } from "./buffers/buffer_sync.ts";
import type {
  SyncEncoder,
  SyncEncoderRegistry,
} from "./encoders/encoder_sync.ts";
import { NullEncoderSync } from "./encoders/encoder_null_sync.ts";
import { encode as encodeString } from "./text_encoding.ts";
import { SyncWritableTap } from "./tap_sync.ts";

const DEFAULT_BLOCK_SIZE_BYTES = 64000;
const SYNC_MARKER_SIZE = 16;
const RESERVED_METADATA_KEYS = new Set(["avro.schema", "avro.codec"]);

/**
 * Options accepted by {@link SyncAvroFileWriter}.
 */
export interface SyncAvroWriterOptions {
  schema: SchemaLike;
  codec?: string;
  blockSize?: number;
  syncMarker?: Uint8Array;
  metadata?: MetadataInit;
  encoders?: SyncEncoderRegistry;
}

/**
 * Fully synchronous Avro container file writer.
 * Mirrors {@link AvroFileWriter} but relies on {@link SyncWritableTap} so it
 * can run where async primitives are unavailable.
 */
export class SyncAvroFileWriter {
  #tap: SyncWritableTap;
  #schemaType: Type;
  #codec: string;
  #encoder: SyncEncoder;
  #blockSize: number;
  #syncMarker: Uint8Array;
  #metadata: Map<string, Uint8Array>;
  #pendingRecords: Uint8Array[] = [];
  #pendingBytes = 0;
  #pendingCount = 0;
  #headerWritten = false;
  #closed = false;
  #builtInEncoders: SyncEncoderRegistry;
  #customEncoders: SyncEncoderRegistry;

  constructor(buffer: ISyncWritable, options: SyncAvroWriterOptions) {
    if (!options || !options.schema) {
      throw new Error("Avro writer requires a schema.");
    }

    this.#tap = new SyncWritableTap(buffer);
    this.#schemaType = createType(options.schema);
    this.#codec = options.codec ?? "null";
    this.#blockSize = this.#validateBlockSize(options.blockSize);
    this.#syncMarker = this.#initializeSyncMarker(options.syncMarker);

    this.#builtInEncoders = {
      "null": new NullEncoderSync(),
    };
    this.#customEncoders = this.#validateCustomEncoders(options.encoders);
    this.#encoder = this.#resolveEncoder(this.#codec);

    this.#metadata = this.#buildMetadata(options.metadata);
    this.#metadata.set(
      "avro.schema",
      encodeString(JSON.stringify(this.#schemaType.toJSON())),
    );
    if (this.#codec !== "null") {
      this.#metadata.set("avro.codec", encodeString(this.#codec));
    }
  }

  /**
   * Appends a record to the pending block, flushing when the block size is met.
   */
  public append(record: unknown): void {
    this.#ensureOpen();
    this.#ensureHeaderWritten();

    if (!this.#schemaType.isValid(record)) {
      throw new Error("Record does not conform to the schema.");
    }

    const recordBytes = this.#encodeRecord(record);
    this.#pendingRecords.push(recordBytes);
    this.#pendingBytes += recordBytes.length;
    this.#pendingCount += 1;

    if (this.#pendingBytes >= this.#blockSize) {
      this.flushBlock();
    }
  }

  /**
   * Flushes any pending data and prevents further writes.
   */
  public close(): void {
    if (this.#closed) {
      return;
    }

    this.#ensureHeaderWritten();
    this.flushBlock();
    this.#closed = true;
  }

  /**
   * Forces pending data into a block immediately.
   */
  public flushBlock(): void {
    this.#ensureOpen();
    this.#ensureHeaderWritten();

    if (this.#pendingCount === 0) {
      return;
    }

    const combined = this.#combinePending();
    const encoded = this.#encoder.encode(combined);
    const block = {
      count: BigInt(this.#pendingCount),
      data: encoded,
      sync: this.#syncMarker,
    };
    BLOCK_TYPE.writeSync(this.#tap, block);

    this.#pendingRecords = [];
    this.#pendingBytes = 0;
    this.#pendingCount = 0;
  }

  #ensureOpen(): void {
    if (this.#closed) {
      throw new Error("Avro writer is already closed.");
    }
  }

  #ensureHeaderWritten(): void {
    if (this.#headerWritten) {
      return;
    }
    this.#writeHeader();
  }

  #writeHeader(): void {
    const header = {
      magic: MAGIC_BYTES,
      meta: this.#metadata,
      sync: this.#syncMarker,
    };
    HEADER_TYPE.writeSync(this.#tap, header);
    this.#headerWritten = true;
  }

  #validateBlockSize(blockSize?: number): number {
    const size = blockSize ?? DEFAULT_BLOCK_SIZE_BYTES;
    if (!Number.isFinite(size) || !Number.isInteger(size) || size <= 0) {
      throw new RangeError("blockSize must be a positive integer byte count.");
    }
    return size;
  }

  #initializeSyncMarker(marker?: Uint8Array): Uint8Array {
    if (!marker) {
      const generated = new Uint8Array(SYNC_MARKER_SIZE);
      crypto.getRandomValues(generated);
      return generated;
    }

    if (marker.length !== SYNC_MARKER_SIZE) {
      throw new Error(`Sync marker must be ${SYNC_MARKER_SIZE} bytes long.`);
    }

    return marker.slice();
  }

  #validateCustomEncoders(encoders?: SyncEncoderRegistry): SyncEncoderRegistry {
    if (!encoders) {
      return {};
    }

    for (const codec of Object.keys(encoders)) {
      if (codec in this.#builtInEncoders) {
        throw new Error(
          `Cannot override built-in encoder for codec: ${codec}`,
        );
      }
    }

    return { ...encoders };
  }

  #resolveEncoder(codec: string): SyncEncoder {
    if (codec in this.#builtInEncoders) {
      return this.#builtInEncoders[codec]!;
    }
    if (codec in this.#customEncoders) {
      return this.#customEncoders[codec]!;
    }
    throw new Error(`Unsupported codec: ${codec}. Provide a custom encoder.`);
  }

  #buildMetadata(input?: MetadataInit): Map<string, Uint8Array> {
    const metadata = new Map<string, Uint8Array>();
    if (!input) {
      return metadata;
    }

    const setEntry = (key: string, value: Uint8Array | string): void => {
      this.#assertMetadataKey(key);
      if (value instanceof Uint8Array) {
        metadata.set(key, value.slice());
      } else {
        metadata.set(key, encodeString(value));
      }
    };

    if (input instanceof Map) {
      for (const [key, value] of input.entries()) {
        setEntry(key, value);
      }
    } else {
      for (const [key, value] of Object.entries(input)) {
        setEntry(key, value);
      }
    }

    return metadata;
  }

  #assertMetadataKey(key: string): void {
    if (typeof key !== "string" || key.length === 0) {
      throw new Error("Metadata keys must be non-empty strings.");
    }
    if (RESERVED_METADATA_KEYS.has(key)) {
      throw new Error(
        `Metadata key "${key}" is reserved and managed by the Avro writer.`,
      );
    }
  }

  #encodeRecord(record: unknown): Uint8Array {
    const buffer = this.#schemaType.toSyncBuffer(record);
    return new Uint8Array(buffer);
  }

  #combinePending(): Uint8Array {
    const combined = new Uint8Array(this.#pendingBytes);
    let offset = 0;
    for (const record of this.#pendingRecords) {
      combined.set(record, offset);
      offset += record.length;
    }
    return combined;
  }
}
