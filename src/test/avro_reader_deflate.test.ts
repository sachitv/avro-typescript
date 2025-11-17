import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { AvroReader } from "../avro_reader.ts";
import type { Decoder } from "../serialization/decoders/decoder.ts";
import { InMemoryReadableBuffer } from "../serialization/buffers/in_memory_buffer.ts";

/**
 * Expected weather records from the weather.avro test file.
 * Note: Avro long type is decoded as BigInt in TypeScript.
 */
const EXPECTED_WEATHER_RECORDS = [
  { station: "011990-99999", time: -619524000000n, temp: 0 },
  { station: "011990-99999", time: -619506000000n, temp: 22 },
  { station: "011990-99999", time: -619484400000n, temp: -11 },
  { station: "012650-99999", time: -655531200000n, temp: 111 },
  { station: "012650-99999", time: -655509600000n, temp: 78 },
];

/**
 * Verify that records match expected weather data.
 */
function assertWeatherRecords(records: unknown[]): void {
  assertEquals(records.length, 5);

  for (let i = 0; i < records.length; i++) {
    const record = records[i] as Record<string, unknown>;
    const expected = EXPECTED_WEATHER_RECORDS[i];
    assertEquals(record.station, expected.station);
    assertEquals(record.time, expected.time);
    assertEquals(record.temp, expected.temp);
  }
}

describe("AvroReader deflate codec support", () => {
  it("supports deflate codec", async () => {
    // Read the deflate-compressed test file
    const deflateFile = await Deno.readFile(
      "test-data/weather-deflate.avro",
    );
    const buffer = new InMemoryReadableBuffer(deflateFile.buffer);

    // Create reader and read all records
    const reader = AvroReader.fromBuffer(buffer);
    const records: unknown[] = [];

    for await (const record of reader.iterRecords()) {
      records.push(record);
    }

    // Should have read all records
    assertWeatherRecords(records);
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
      "test-data/weather.avro",
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
      "test-data/weather-zstd.avro",
    );
    const buffer = new InMemoryReadableBuffer(zstdFile.buffer);

    const reader = AvroReader.fromBuffer(buffer);

    try {
      for await (const _record of reader.iterRecords()) {
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
      "test-data/weather.avro",
    );
    const buffer = new InMemoryReadableBuffer(regularFile.buffer);

    const reader = AvroReader.fromBuffer(buffer);
    const records: unknown[] = [];

    for await (const record of reader.iterRecords()) {
      records.push(record);
    }

    assertWeatherRecords(records);
  });
});
