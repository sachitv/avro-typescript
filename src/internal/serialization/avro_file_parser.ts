import { createType } from "../createType/mod.ts";
import { Type } from "../schemas/type.ts";
import { ReadableTap } from "./tap.ts";
import { type IReadableBuffer } from "./buffers/buffer.ts";

// Type of Avro header.
const HEADER_TYPE = createType({
  type: "record",
  name: "org.apache.avro.file.Header",
  fields: [
    { name: "magic", type: { type: "fixed", name: "Magic", size: 4 } },
    { name: "meta", type: { type: "map", values: "bytes" } },
    { name: "sync", type: { type: "fixed", name: "Sync", size: 16 } },
  ],
});

// Type of each block.
const BLOCK_TYPE = createType({
  type: "record",
  name: "org.apache.avro.file.Block",
  fields: [
    { name: "count", type: "long" },
    { name: "data", type: "bytes" },
    { name: "sync", type: { type: "fixed", name: "Sync", size: 16 } },
  ],
});

// First 4 bytes of an Avro object container file.
const MAGIC_BYTES = new Uint8Array([0x4F, 0x62, 0x6A, 0x01]); // 'Obj\x01'

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
  schemaType: Type;
  schema: unknown; // Parsed JSON schema
  codec?: string; // Codec name if present
}

/**
 * Internal parser for Avro object container files.
 * Handles header parsing and record iteration.
 */
export class AvroFileParser {
  #buffer: IReadableBuffer;
  #header: AvroHeader | undefined;
  #headerTap: ReadableTap | undefined;

  /**
   * Creates a new AvroFileParser.
   *
   * @param buffer The readable buffer containing Avro data.
   */
  public constructor(buffer: IReadableBuffer) {
    this.#buffer = buffer;
  }

  /**
   * Gets the parsed Avro file header with proper typing.
   *
   * @returns Promise that resolves to the parsed header information.
   * @throws Error if the file is not a valid Avro file.
   */
  public async getHeader(): Promise<ParsedAvroHeader> {
    const header = await this.#parseHeader();

    // Extract codec information
    const codec = header.meta.get("avro.codec");
    let codecStr: string | undefined;
    if (codec) {
      codecStr = new TextDecoder().decode(codec);
    }

    // Parse schema JSON for public access
    const schemaJson = header.meta.get("avro.schema");
    let schema: unknown;
    if (schemaJson) {
      const schemaStr = new TextDecoder().decode(schemaJson);
      schema = JSON.parse(schemaStr);
    }

    return {
      magic: header.magic,
      meta: header.meta,
      sync: header.sync,
      schemaType: header.schemaType,
      schema,
      codec: codecStr,
    };
  }

  /**
   * Asynchronously iterates over all records in the Avro file.
   *
   * @returns AsyncIterableIterator that yields each record.
   */
  public async *iterRecords(): AsyncIterableIterator<unknown> {
    const header = await this.#parseHeader();
    const { schemaType } = header;

    // Use the tap that's positioned after the header
    const tap = this.#headerTap!;

    while (true) {
      try {
        const block = await BLOCK_TYPE.read(tap) as {
          count: bigint;
          data: Uint8Array;
          sync: Uint8Array;
        };

        // Create a tap for the block data
        const blockData = block.data.slice();
        const recordTap = new ReadableTap(blockData.buffer);

        // Yield each record in the block
        for (let i = 0n; i < block.count; i += 1n) {
          const record = await schemaType.read(recordTap);
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

    // For simplicity, we assume null codec (no compression).
    const codec = meta.get("avro.codec");
    if (codec) {
      const codecStr = new TextDecoder().decode(codec);
      if (codecStr !== "null") {
        throw new Error(`Unsupported codec: ${codecStr}`);
      }
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
}
