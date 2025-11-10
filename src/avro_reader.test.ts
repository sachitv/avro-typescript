import { assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { AvroReader } from "./avro_reader.ts";
import { InMemoryReadableBuffer } from "./internal/serialization/buffers/in_memory_buffer.ts";
import type { AvroReaderInstance, ParsedAvroHeader } from "./avro_reader.ts";
import type { Type } from "./internal/schemas/type.ts";

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
 * Load the weather.avro test file data.
 */
async function loadWeatherAvroFile(): Promise<Uint8Array> {
  return await Deno.readFile("../../share/test/data/weather.avro");
}

/**
 * Create an InMemoryReadableBuffer from the weather.avro file.
 */
async function createWeatherAvroBuffer(): Promise<InMemoryReadableBuffer> {
  const fileData = await loadWeatherAvroFile();
  return new InMemoryReadableBuffer(fileData.buffer as ArrayBuffer);
}

/**
 * Create a Blob from the weather.avro file.
 */
async function createWeatherAvroBlob(): Promise<Blob> {
  const fileData = await loadWeatherAvroFile();
  return new Blob([fileData.buffer as ArrayBuffer]);
}

/**
 * Create a ReadableStream from the weather.avro file.
 */
async function createWeatherAvroStream(): Promise<ReadableStream> {
  const fileData = await loadWeatherAvroFile();
  return new ReadableStream({
    start(controller) {
      controller.enqueue(fileData);
      controller.close();
    },
  });
}

/**
 * Assert that a reader has the expected interface.
 */
function assertReaderInterface(reader: AvroReaderInstance): void {
  assertEquals(typeof reader[Symbol.asyncIterator], "function");
  assertEquals(typeof reader.getHeader, "function");
}

/**
 * Read all records from a reader.
 */
async function readAllRecords(reader: AvroReaderInstance): Promise<unknown[]> {
  const records = [];
  for await (const record of reader) {
    records.push(record);
  }
  return records;
}

/**
 * Verify that header has expected weather.avro properties.
 */
function assertWeatherHeader(header: ParsedAvroHeader): void {
  assertEquals(typeof header, "object");
  assertEquals(header.magic.length, 4);
  assertEquals(header.sync.length, 16);
  assertEquals(typeof header.schema, "object");
  assertEquals(header.codec, "null"); // Weather file uses null codec
}

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

describe("AvroReader", () => {
  it("should read weather.avro file and verify header", async () => {
    const buffer = await createWeatherAvroBuffer();
    const reader = AvroReader.fromBuffer(buffer);

    // Verify header access
    const header = await reader.getHeader();
    assertWeatherHeader(header);
  });

  it("should read weather.avro records and verify data", async () => {
    const buffer = await createWeatherAvroBuffer();
    const reader = AvroReader.fromBuffer(buffer);

    // Read all records and verify correctness
    const records = await readAllRecords(reader);
    assertWeatherRecords(records);
  });

  it("should create from blob", async () => {
    const blob = await createWeatherAvroBlob();
    const reader = AvroReader.fromBlob(blob);

    assertReaderInterface(reader);

    // Verify header and records
    const header = await reader.getHeader();
    assertWeatherHeader(header);

    const records = await readAllRecords(reader);
    assertWeatherRecords(records);
  });

  it("should create from stream with default options", async () => {
    const stream = await createWeatherAvroStream();
    const reader = AvroReader.fromStream(stream);

    assertReaderInterface(reader);

    // Verify header and records
    const header = await reader.getHeader();
    assertWeatherHeader(header);

    const records = await readAllRecords(reader);
    assertWeatherRecords(records);
  });

  it("should create from stream with cache size", async () => {
    const stream = await createWeatherAvroStream();
    const reader = AvroReader.fromStream(stream, { cacheSize: 1024 });

    assertReaderInterface(reader);

    // Verify header (limited buffering may not support reading all records)
    const header = await reader.getHeader();
    assertWeatherHeader(header);
  });

  it("should reject invalid URL", async () => {
    await assertRejects(
      async () => {
        await AvroReader.fromUrl("invalid-url");
      },
      Error,
      "Invalid URL",
    );
  });

  it("should reject HTTP error responses", async () => {
    // Mock fetch to return a 404 error
    const originalFetch = globalThis.fetch;
    globalThis.fetch = () => {
      return Promise.resolve(
        new Response("Not Found", { status: 404, statusText: "Not Found" }),
      );
    };

    try {
      await assertRejects(
        async () => {
          await AvroReader.fromUrl("https://example.com/missing.avro");
        },
        Error,
        "Failed to fetch https://example.com/missing.avro: 404 Not Found",
      );
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it("should reject responses with no body", async () => {
    // Mock fetch to return a response with no body
    const originalFetch = globalThis.fetch;
    globalThis.fetch = () => {
      return Promise.resolve(
        new Response(null, { status: 200, statusText: "OK" }),
      );
    };

    try {
      await assertRejects(
        async () => {
          await AvroReader.fromUrl("https://example.com/empty.avro");
        },
        Error,
        "Response body is null for https://example.com/empty.avro",
      );
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it("should pass cacheSize option to fromStream", async () => {
    // Mock fetch to return a successful response with body
    const originalFetch = globalThis.fetch;
    let capturedCacheSize: number | undefined;

    // Mock the fromStream method to capture the cacheSize parameter
    const originalFromStream = AvroReader.fromStream;
    AvroReader.fromStream = (
      _stream: ReadableStream,
      options?: { cacheSize?: number },
    ) => {
      capturedCacheSize = options?.cacheSize;
      // Return a mock reader with proper ParsedAvroHeader
      const mockHeader: ParsedAvroHeader = {
        magic: new Uint8Array(4),
        meta: new Map(),
        sync: new Uint8Array(16),
        schemaType: {} as Type, // Mock schema type
        schema: {},
        codec: "null",
      };
      return {
        getHeader: () => Promise.resolve(mockHeader),
        next: () => Promise.resolve({ done: true, value: undefined }),
        [Symbol.asyncIterator]: function () {
          return this;
        },
      } as AvroReaderInstance;
    };

    globalThis.fetch = () => {
      return Promise.resolve(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new Uint8Array([0x4F, 0x62, 0x6A, 0x01])); // Mock Avro header
              controller.close();
            },
          }),
          { status: 200, statusText: "OK" },
        ),
      );
    };

    try {
      await AvroReader.fromUrl("https://example.com/test.avro", {
        cacheSize: 2048,
      });
      assertEquals(capturedCacheSize, 2048);
    } finally {
      globalThis.fetch = originalFetch;
      AvroReader.fromStream = originalFromStream;
    }
  });

  it("should create from URL", async () => {
    const blob = await createWeatherAvroBlob();
    const url = URL.createObjectURL(blob);

    try {
      const reader = await AvroReader.fromUrl(url);
      assertReaderInterface(reader);

      // Verify header and records
      const header = await reader.getHeader();
      assertWeatherHeader(header);

      const records = await readAllRecords(reader);
      assertWeatherRecords(records);
    } finally {
      URL.revokeObjectURL(url);
    }
  });
});
