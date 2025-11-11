import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { AvroReader } from "./avro_reader.ts";
import { type Decoder } from "./internal/serialization/decoders/decoder.ts";
import { InMemoryReadableBuffer } from "./internal/serialization/buffers/in_memory_buffer.ts";

describe("AvroReader deflate codec support", () => {
  it("supports deflate codec", async () => {
    // Read the deflate-compressed test file
    const deflateFile = await Deno.readFile(
      "/workspaces/avro/share/test/data/weather-deflate.avro",
    );
    const buffer = new InMemoryReadableBuffer(deflateFile.buffer);

    // Create reader and read all records
    const reader = AvroReader.fromBuffer(buffer);
    const records: unknown[] = [];

    for await (const record of reader) {
      records.push(record);
    }

    // Should have read some records
    assertEquals(records.length > 0, true);

    // Check that records have expected structure (weather data)
    const firstRecord = records[0] as Record<string, unknown>;
    assertEquals(typeof firstRecord.station, "string");
    assertEquals(typeof firstRecord.time, "bigint");
    assertEquals(typeof firstRecord.temp, "number");
  });

  it("supports custom decoders", async () => {
    // Create a simple custom decoder that just passes data through (for testing)
    const passthroughDecoder: Decoder = {
      decode(compressedData: Uint8Array): Promise<Uint8Array> {
        return Promise.resolve(compressedData);
      },
    };

    // Read the regular (non-compressed) test file
    const regularFile = await Deno.readFile(
      "/workspaces/avro/share/test/data/weather.avro",
    );
    const buffer = new InMemoryReadableBuffer(regularFile.buffer);

    // Should throw error when trying to override built-in decoder at construction time
    try {
      AvroReader.fromBuffer(buffer, {
        decoders: {
          "null": passthroughDecoder, // This should throw since we can't override built-in
        },
      });
      assertEquals(
        false,
        true,
        "Should have thrown error for overriding built-in decoder",
      );
    } catch (error) {
      assertEquals(
        (error as Error).message,
        "Cannot override built-in decoder for codec: null",
      );
    }
  });

  it("handles unsupported codec gracefully", async () => {
    // Create a mock file with unsupported codec in metadata
    // For this test, we'll try to read a file with a codec we don't support
    const zstdFile = await Deno.readFile(
      "/workspaces/avro/share/test/data/weather-zstd.avro",
    );
    const buffer = new InMemoryReadableBuffer(zstdFile.buffer);

    const reader = AvroReader.fromBuffer(buffer);

    try {
      for await (const _record of reader) {
        // Should not reach here
      }
      assertEquals(
        false,
        true,
        "Should have thrown error for unsupported codec",
      );
    } catch (error) {
      assertEquals(
        (error as Error).message.includes("Unsupported codec: zstandard"),
        true,
      );
    }
  });

  it("maintains backward compatibility", async () => {
    // Test that existing code without decoders still works
    const regularFile = await Deno.readFile(
      "/workspaces/avro/share/test/data/weather.avro",
    );
    const buffer = new InMemoryReadableBuffer(regularFile.buffer);

    const reader = AvroReader.fromBuffer(buffer);
    const records: unknown[] = [];

    for await (const record of reader) {
      records.push(record);
    }

    assertEquals(records.length > 0, true);

    const firstRecord = records[0] as Record<string, unknown>;
    assertEquals(typeof firstRecord.station, "string");
    assertEquals(typeof firstRecord.time, "bigint");
    assertEquals(typeof firstRecord.temp, "number");
  });
});
