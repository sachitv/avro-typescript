import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type { IWritableBuffer } from "../buffers/buffer.ts";
import { InMemoryReadableBuffer } from "../buffers/in_memory_buffer.ts";
import { AvroFileParser } from "../avro_file_parser.ts";
import { AvroFileWriter, type AvroWriterOptions } from "../avro_file_writer.ts";
import {
  decode as decodeString,
  encode as encodeString,
} from "../../serialization/text_encoding.ts";

const SIMPLE_SCHEMA = {
  type: "record",
  name: "test.Simple",
  fields: [
    { name: "id", type: "int" },
    { name: "name", type: "string" },
  ],
} as const;

const SAMPLE_RECORDS = [
  { id: 1, name: "alpha" },
  { id: 2, name: "beta" },
  { id: 3, name: "gamma" },
];

class CollectingWritableBuffer implements IWritableBuffer {
  #chunks: Uint8Array[] = [];

  // deno-lint-ignore require-await
  async appendBytes(data: Uint8Array): Promise<void> {
    if (data.length === 0) {
      return;
    }
    this.#chunks.push(data.slice());
  }

  // deno-lint-ignore require-await
  async isValid(): Promise<boolean> {
    return true;
  }

  // deno-lint-ignore require-await
  async canAppendMore(_size: number): Promise<boolean> {
    return true;
  }

  toArrayBuffer(): ArrayBuffer {
    const total = this.#chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const combined = new Uint8Array(total);
    let offset = 0;
    for (const chunk of this.#chunks) {
      combined.set(chunk, offset);
      offset += chunk.length;
    }
    return combined.buffer.slice(
      combined.byteOffset,
      combined.byteOffset + combined.byteLength,
    );
  }
}

function createParser(
  buffer: CollectingWritableBuffer,
): AvroFileParser {
  const readable = new InMemoryReadableBuffer(
    buffer.toArrayBuffer() as ArrayBuffer,
  );
  return new AvroFileParser(readable);
}

async function collectRecords(parser: AvroFileParser): Promise<unknown[]> {
  const records: unknown[] = [];
  for await (const record of parser.iterRecords()) {
    records.push(record);
  }
  return records;
}

describe("AvroFileWriter", () => {
  describe("Constructor validation tests", () => {
    it("requires a schema", () => {
      const buffer = new CollectingWritableBuffer();
      assertThrows(
        () =>
          new AvroFileWriter(
            buffer,
            undefined as unknown as AvroWriterOptions,
          ),
        Error,
        "requires a schema",
      );
    });

    it("rejects negative block size", () => {
      const buffer = new CollectingWritableBuffer();
      assertThrows(
        () =>
          new AvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            blockSize: -1,
          }),
        RangeError,
        "blockSize must be a positive integer byte count.",
      );
    });

    it("rejects fractional block size", () => {
      const buffer = new CollectingWritableBuffer();
      assertThrows(
        () =>
          new AvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            blockSize: 1.5,
          }),
        RangeError,
        "blockSize must be a positive integer byte count.",
      );
    });

    it("rejects zero block size", () => {
      const buffer = new CollectingWritableBuffer();
      assertThrows(
        () =>
          new AvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            blockSize: 0,
          }),
        RangeError,
        "blockSize must be a positive integer byte count.",
      );
    });

    it("rejects infinite block size", () => {
      const buffer = new CollectingWritableBuffer();
      assertThrows(
        () =>
          new AvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            blockSize: Infinity,
          }),
        RangeError,
        "blockSize must be a positive integer byte count.",
      );
    });

    it("throws when sync marker size is invalid", () => {
      const buffer = new CollectingWritableBuffer();
      assertThrows(
        () =>
          new AvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            syncMarker: new Uint8Array(4),
          }),
        Error,
        "Sync marker must be",
      );
    });

    it("rejects overriding built-in encoders", () => {
      const buffer = new CollectingWritableBuffer();
      assertThrows(
        () =>
          new AvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            // deno-lint-ignore require-await
            encoders: { "null": { encode: async () => new Uint8Array() } },
          }),
        Error,
        "Cannot override built-in encoder",
      );
    });

    it("throws when codec is not supported", async () => {
      const buffer = new CollectingWritableBuffer();
      await assertRejects(
        async () => {
          const writer = new AvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            codec: "nonexistent",
            encoders: {
              "custom": {
                // deno-lint-ignore require-await
                encode: async (data: Uint8Array) => data,
              },
            },
          });
          await writer.close(); // Close to trigger header write
        },
        Error,
        "Unsupported codec: nonexistent. Provide a custom encoder.",
      );
    });
  });

  describe("Metadata handling tests", () => {
    it("supports additional metadata entries", async () => {
      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, {
        schema: SIMPLE_SCHEMA,
        metadata: { "app.name": "writer-test" },
      });
      await writer.close();

      const parser = createParser(buffer);
      const header = await parser.getHeader();
      const appName = header.meta.get("app.name");
      assert(appName, "app.name metadata should be present");
      assertEquals(decodeString(appName), "writer-test");
    });

    it("supports Uint8Array metadata values", async () => {
      const customData = new Uint8Array([1, 2, 3, 4]);
      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, {
        schema: SIMPLE_SCHEMA,
        metadata: { "custom.key": customData },
      });
      await writer.close();

      const parser = createParser(buffer);
      const header = await parser.getHeader();
      const storedData = header.meta.get("custom.key");
      assert(storedData, "custom.key metadata should be present");
      assertEquals(storedData, customData);
    });

    it("supports Map metadata input", async () => {
      const metadataMap = new Map<string, Uint8Array>();
      metadataMap.set("app.version", encodeString("1.0.0"));
      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, {
        schema: SIMPLE_SCHEMA,
        metadata: metadataMap,
      });
      await writer.close();

      const parser = createParser(buffer);
      const header = await parser.getHeader();
      const version = header.meta.get("app.version");
      assert(version, "app.version metadata should be present");
      assertEquals(decodeString(version), "1.0.0");
    });

    it("rejects reserved metadata keys", () => {
      const buffer = new CollectingWritableBuffer();
      assertThrows(
        () =>
          new AvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            metadata: { "avro.schema": "override" },
          }),
        Error,
        "reserved",
      );
    });

    it("rejects invalid metadata keys", () => {
      const buffer = new CollectingWritableBuffer();
      assertThrows(
        () =>
          new AvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            metadata: { "": "empty key" },
          }),
        Error,
        "Metadata keys must be non-empty strings.",
      );
    });
  });

  describe("Record writing tests", () => {
    it("writes header and records with default codec", async () => {
      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, { schema: SIMPLE_SCHEMA });
      for (const record of SAMPLE_RECORDS) {
        await writer.append(record);
      }
      await writer.close();

      const parser = createParser(buffer);
      const header = await parser.getHeader();
      assertEquals(header.meta.get("avro.codec"), undefined);

      const records = await collectRecords(parser);
      assertEquals(records, SAMPLE_RECORDS);
    });

    it("supports deflate codec when compression APIs are available", async () => {
      if (
        typeof CompressionStream === "undefined" ||
        typeof DecompressionStream === "undefined"
      ) {
        // Environment does not support compression streams; skip test.
        return;
      }

      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, {
        schema: SIMPLE_SCHEMA,
        codec: "deflate",
      });
      for (const record of SAMPLE_RECORDS) {
        await writer.append(record);
      }
      await writer.close();

      const parser = createParser(buffer);
      const header = await parser.getHeader();
      const codec = header.meta.get("avro.codec");
      assert(codec, "avro.codec metadata should be present for deflate");
      assertEquals(decodeString(codec), "deflate");

      const records = await collectRecords(parser);
      assertEquals(records, SAMPLE_RECORDS);
    });

    it("automatically flushes when record exceeds block size", async () => {
      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, {
        schema: SIMPLE_SCHEMA,
        blockSize: 1, // Very small block size
      });
      await writer.append(SAMPLE_RECORDS[0]);
      await writer.close();

      const parser = createParser(buffer);
      const records = await collectRecords(parser);
      assertEquals(records, [SAMPLE_RECORDS[0]]);
    });

    it("throws when appending invalid record", async () => {
      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, { schema: SIMPLE_SCHEMA });
      await assertRejects(
        () => writer.append({ invalidField: "value" }),
        Error,
        "Record does not conform to the schema.",
      );
    });

    it("accepts custom encoders", async () => {
      let encoderCalled = false;
      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, {
        schema: SIMPLE_SCHEMA,
        codec: "custom",
        encoders: {
          "custom": {
            // deno-lint-ignore require-await
            encode: async (data: Uint8Array) => {
              encoderCalled = true;
              return data; // Pass through
            },
          },
        },
      });
      for (const record of SAMPLE_RECORDS) {
        await writer.append(record);
      }
      await writer.close();

      assert(encoderCalled, "Custom encoder should have been called");

      const readable = new InMemoryReadableBuffer(
        buffer.toArrayBuffer() as ArrayBuffer,
      );
      const parser = new AvroFileParser(readable, {
        decoders: {
          "custom": {
            // deno-lint-ignore require-await
            decode: async (data: Uint8Array) => data, // Pass through
          },
        },
      });
      const records = await collectRecords(parser);
      assertEquals(records, SAMPLE_RECORDS);
    });
  });

  describe("Lifecycle tests", () => {
    it("handles double close gracefully", async () => {
      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, { schema: SIMPLE_SCHEMA });
      for (const record of SAMPLE_RECORDS) {
        await writer.append(record);
      }
      await writer.close();
      await writer.close(); // Should not error

      const parser = createParser(buffer);
      const records = await collectRecords(parser);
      assertEquals(records, SAMPLE_RECORDS);
    });

    it("throws when appending after close", async () => {
      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, { schema: SIMPLE_SCHEMA });
      await writer.close();
      await assertRejects(
        () => writer.append(SAMPLE_RECORDS[0]),
        Error,
        "Avro writer is already closed.",
      );
    });
  });

  describe("Sync marker tests", () => {
    it("uses predefined sync marker", async () => {
      const customSyncMarker = new Uint8Array(16);
      customSyncMarker.set([
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
      ]);

      const buffer = new CollectingWritableBuffer();
      const writer = new AvroFileWriter(buffer, {
        schema: SIMPLE_SCHEMA,
        syncMarker: customSyncMarker,
      });
      for (const record of SAMPLE_RECORDS) {
        await writer.append(record);
      }
      await writer.close();

      const parser = createParser(buffer);
      const header = await parser.getHeader();
      assertEquals(header.sync, customSyncMarker);
    });
  });
});
