import { createType, type SchemaLike } from "../type/create_type.ts";
import type { Type } from "../schemas/type.ts";
import type { IWritableBuffer } from "./buffers/buffer.ts";
import { WritableTap } from "./tap.ts";
import { BLOCK_TYPE, HEADER_TYPE, MAGIC_BYTES } from "./avro_constants.ts";
import type { Encoder, EncoderRegistry } from "./encoders/encoder.ts";
import { DeflateEncoder } from "./encoders/deflate_encoder.ts";
import { NullEncoder } from "./encoders/null_encoder.ts";
import { encode as encodeString } from "./text_encoding.ts";

/**
 * Default block size in bytes for Avro data blocks.
 * Set to 64000 to match the default used by the Java Avro libraries.
 */
const DEFAULT_BLOCK_SIZE_BYTES = 64000;
const SYNC_MARKER_SIZE = 16;
const RESERVED_METADATA_KEYS = new Set(["avro.schema", "avro.codec"]);

export type MetadataInit =
  | Map<string, Uint8Array>
  | Record<string, string | Uint8Array>;

export interface AvroWriterOptions {
  schema: SchemaLike;
  codec?: string;
  blockSize?: number;
  syncMarker?: Uint8Array;
  metadata?: MetadataInit;
  encoders?: EncoderRegistry;
}

/**
 * Internal writer responsible for emitting Avro object container files.
 */
export class AvroFileWriter {
  #tap: WritableTap;
  #schemaType: Type;
  #codec: string;
  #encoder: Encoder;
  #blockSize: number;
  #syncMarker: Uint8Array;
  #metadata: Map<string, Uint8Array>;

  #pendingRecords: Uint8Array[] = [];
  #pendingBytes = 0;
  #pendingCount = 0;

  #headerPromise?: Promise<void>;
  #headerWritten = false;
  #closed = false;

  #builtInEncoders: EncoderRegistry;
  #customEncoders: EncoderRegistry;

  constructor(buffer: IWritableBuffer, options: AvroWriterOptions) {
    if (!options || !options.schema) {
      throw new Error("Avro writer requires a schema.");
    }

    this.#tap = new WritableTap(buffer);
    this.#schemaType = createType(options.schema);
    this.#codec = options.codec ?? "null";
    this.#blockSize = this.#validateBlockSize(options.blockSize);
    this.#syncMarker = this.#initializeSyncMarker(options.syncMarker);

    this.#builtInEncoders = {
      "null": new NullEncoder(),
      "deflate": new DeflateEncoder(),
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
   * Append a single record to the file.
   */
  public async append(record: unknown): Promise<void> {
    this.#ensureOpen();
    await this.#ensureHeaderWritten();

    if (!this.#schemaType.isValid(record)) {
      throw new Error("Record does not conform to the schema.");
    }

    const recordBytes = await this.#encodeRecord(record);
    this.#pendingRecords.push(recordBytes);
    this.#pendingBytes += recordBytes.length;
    this.#pendingCount += 1;

    if (this.#pendingBytes >= this.#blockSize) {
      await this.flushBlock();
    }
  }

  /**
   * Flush pending data and prevent further writes.
   */
  public async close(): Promise<void> {
    if (this.#closed) {
      return;
    }

    await this.#ensureHeaderWritten();
    await this.flushBlock();
    this.#closed = true;
  }

  /**
   * Ensures the writer is not closed.
   * @throws Error if the writer is already closed.
   */
  #ensureOpen(): void {
    if (this.#closed) {
      throw new Error("Avro writer is already closed.");
    }
  }

  /**
   * Validates and normalizes the block size.
   * @param blockSize The block size to validate, or undefined for default.
   * @returns The validated block size.
   * @throws RangeError if blockSize is not a positive integer.
   */
  #validateBlockSize(blockSize?: number): number {
    const size = blockSize ?? DEFAULT_BLOCK_SIZE_BYTES;
    if (!Number.isFinite(size) || !Number.isInteger(size) || size <= 0) {
      throw new RangeError("blockSize must be a positive integer byte count.");
    }
    return size;
  }

  /**
   * Initializes the sync marker, generating a random one if not provided.
   * @param marker Optional sync marker to use.
   * @returns The sync marker to use.
   * @throws Error if the provided marker is not the correct length.
   */
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

  /**
   * Validates custom encoders, ensuring they don't override built-ins.
   * @param encoders Optional custom encoders to validate.
   * @returns The validated custom encoders.
   * @throws Error if attempting to override a built-in encoder.
   */
  #validateCustomEncoders(encoders?: EncoderRegistry): EncoderRegistry {
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

  /**
   * Resolves the encoder for the given codec.
   * @param codec The codec name.
   * @returns The encoder instance.
   * @throws Error if the codec is unsupported.
   */
  #resolveEncoder(codec: string): Encoder {
    if (codec in this.#builtInEncoders) {
      return this.#builtInEncoders[codec]!;
    }

    if (codec in this.#customEncoders) {
      return this.#customEncoders[codec]!;
    }

    throw new Error(`Unsupported codec: ${codec}. Provide a custom encoder.`);
  }

  /**
   * Builds the metadata map from the input.
   * @param input The metadata input to process.
   * @returns The processed metadata map.
   */
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

  /**
   * Asserts that a metadata key is valid.
   * @param key The key to validate.
   * @throws Error if the key is invalid or reserved.
   */
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

  /**
   * Ensures the header has been written to the output.
   */
  async #ensureHeaderWritten(): Promise<void> {
    if (this.#headerWritten) {
      return;
    }

    if (!this.#headerPromise) {
      this.#headerPromise = this.#writeHeader();
    }

    await this.#headerPromise;
  }

  /**
   * Writes the Avro file header to the output.
   */
  async #writeHeader(): Promise<void> {
    const header = {
      magic: MAGIC_BYTES,
      meta: this.#metadata,
      sync: this.#syncMarker,
    };
    await HEADER_TYPE.write(this.#tap, header);
    this.#headerWritten = true;
  }

  /**
   * Encodes a record to bytes using the schema.
   * @param record The record to encode.
   * @returns The encoded record bytes.
   */
  async #encodeRecord(record: unknown): Promise<Uint8Array> {
    const buffer = await this.#schemaType.toBuffer(record);
    return new Uint8Array(buffer);
  }

  /**
   * Manually flush pending records to a block.
   * This can be called to force writing of accumulated records before the block size threshold is reached.
   */
  public async flushBlock(): Promise<void> {
    this.#ensureOpen();
    await this.#ensureHeaderWritten();

    if (this.#pendingCount === 0) {
      return;
    }

    const combined = this.#combinePending();
    const encoded = await this.#encoder.encode(combined);
    const block = {
      count: BigInt(this.#pendingCount),
      data: encoded,
      sync: this.#syncMarker,
    };
    await BLOCK_TYPE.write(this.#tap, block);

    this.#pendingRecords = [];
    this.#pendingBytes = 0;
    this.#pendingCount = 0;
  }

  /**
   * Combines all pending record bytes into a single buffer.
   * @returns The combined record bytes.
   */
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
