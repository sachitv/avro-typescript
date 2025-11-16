import { assert, assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { AvroReader } from "./avro_reader.ts";
import { InMemoryReadableBuffer } from "./internal/serialization/buffers/in_memory_buffer.ts";
import type {
  AvroReaderInstance,
  FromBufferOptions,
  FromStreamOptions,
  ParsedAvroHeader,
} from "./avro_reader.ts";

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

const WEATHER_READER_SCHEMA = {
  type: "record",
  name: "test.Weather",
  fields: [
    { name: "station", type: "string" },
    { name: "temp", type: "int" },
  ],
} as const;

const EXPECTED_WEATHER_STATION_TEMP_RECORDS = EXPECTED_WEATHER_RECORDS.map(
  ({ station, temp }) => ({ station, temp }),
);

/**
 * Load the weather.avro test file data.
 */
async function loadWeatherAvroFile(): Promise<Uint8Array> {
  return await Deno.readFile("test-data/weather.avro");
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
  assertEquals(typeof reader.iterRecords, "function");
  assertEquals(typeof reader.getHeader, "function");
}

/**
 * Read all records from a reader.
 */
async function readAllRecords(reader: AvroReaderInstance): Promise<unknown[]> {
  const records = [];
  for await (const record of reader.iterRecords()) {
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
  const schemaJson = header.meta.get("avro.schema");
  assert(schemaJson);
  const schema = JSON.parse(new TextDecoder().decode(schemaJson));
  assertEquals(typeof schema, "object");
  const codec = header.meta.get("avro.codec");
  assertEquals(codec ? new TextDecoder().decode(codec) : undefined, "null");
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

function assertWeatherStationTempRecords(records: unknown[]): void {
  assertEquals(records.length, 5);

  for (let i = 0; i < records.length; i++) {
    const record = records[i] as Record<string, unknown>;
    const expected = EXPECTED_WEATHER_STATION_TEMP_RECORDS[i];
    assertEquals(Object.keys(record).sort(), ["station", "temp"]);
    assertEquals(record.station, expected.station);
    assertEquals(record.temp, expected.temp);
  }
}

function createMockReaderInstance(): AvroReaderInstance {
  const mockHeader: ParsedAvroHeader = {
    magic: new Uint8Array(4),
    meta: new Map<string, Uint8Array>(),
    sync: new Uint8Array(16),
  };

  const mockIterator: AsyncIterableIterator<unknown> = {
    next: () => Promise.resolve({ done: true, value: undefined }),
    [Symbol.asyncIterator]() {
      return this;
    },
  };

  return {
    getHeader: () => Promise.resolve(mockHeader),
    close: () => Promise.resolve(),
    iterRecords: () => mockIterator,
  } as AvroReaderInstance;
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

  it("should read weather.avro records with a reader schema from buffer", async () => {
    const buffer = await createWeatherAvroBuffer();
    const reader = AvroReader.fromBuffer(buffer, {
      readerSchema: WEATHER_READER_SCHEMA,
    });

    const records = await readAllRecords(reader);
    assertWeatherStationTempRecords(records);
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

  it("should create from blob with reader schema", async () => {
    const blob = await createWeatherAvroBlob();
    const reader = AvroReader.fromBlob(blob, {
      readerSchema: WEATHER_READER_SCHEMA,
    });

    const records = await readAllRecords(reader);
    assertWeatherStationTempRecords(records);
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

  it("should create from stream with reader schema and cache size", async () => {
    const stream = await createWeatherAvroStream();
    const reader = AvroReader.fromStream(stream, {
      cacheSize: 1024,
      readerSchema: WEATHER_READER_SCHEMA,
    });

    const records = await readAllRecords(reader);
    assertWeatherStationTempRecords(records);
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
    let capturedOptions: FromStreamOptions | undefined;

    // Mock the fromStream method to capture the cacheSize parameter
    const originalFromStream = AvroReader.fromStream;
    AvroReader.fromStream = (
      _stream: ReadableStream,
      options?: FromStreamOptions,
    ) => {
      capturedOptions = options;
      // Return a mock reader with proper ParsedAvroHeader
      const mockHeader: ParsedAvroHeader = {
        magic: new Uint8Array(4),
        meta: new Map(),
        sync: new Uint8Array(16),
      };
      const mockIterator: AsyncIterableIterator<unknown> = {
        next: () => Promise.resolve({ done: true, value: undefined }),
        [Symbol.asyncIterator]: function () {
          return this;
        },
      };
      return {
        getHeader: () => Promise.resolve(mockHeader),
        close: () => Promise.resolve(),
        iterRecords: () => mockIterator,
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
      assertEquals(capturedOptions?.cacheSize, 2048);
      assertEquals(capturedOptions?.readerSchema, undefined);
    } finally {
      globalThis.fetch = originalFetch;
      AvroReader.fromStream = originalFromStream;
    }
  });

  it("should forward decoders option when creating from stream", async () => {
    const stream = await createWeatherAvroStream();
    const passthroughDecoder = {
      decode(compressedData: Uint8Array): Promise<Uint8Array> {
        return Promise.resolve(compressedData);
      },
    };

    let capturedOptions: FromBufferOptions | undefined;
    const originalFromBuffer = AvroReader.fromBuffer;
    AvroReader.fromBuffer = (_buffer, options) => {
      capturedOptions = options;
      return createMockReaderInstance();
    };

    try {
      AvroReader.fromStream(stream, {
        decoders: { custom: passthroughDecoder },
      });
      assertEquals(capturedOptions?.decoders?.custom, passthroughDecoder);
    } finally {
      AvroReader.fromBuffer = originalFromBuffer;
    }
  });

  it("should forward decoders option when creating from URL", async () => {
    const passthroughDecoder = {
      decode(compressedData: Uint8Array): Promise<Uint8Array> {
        return Promise.resolve(compressedData);
      },
    };

    const originalFetch = globalThis.fetch;
    let capturedOptions: FromStreamOptions | undefined;
    const originalFromStream = AvroReader.fromStream;
    AvroReader.fromStream = (_stream, options) => {
      capturedOptions = options;
      return createMockReaderInstance();
    };

    globalThis.fetch = () => {
      return Promise.resolve(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new Uint8Array([0x4F, 0x62, 0x6A, 0x01]));
              controller.close();
            },
          }),
          { status: 200, statusText: "OK" },
        ),
      );
    };

    try {
      await AvroReader.fromUrl("https://example.com/test.avro", {
        decoders: { custom: passthroughDecoder },
      });
      assertEquals(capturedOptions?.decoders?.custom, passthroughDecoder);
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

  it("should create from URL with reader schema", async () => {
    const blob = await createWeatherAvroBlob();
    const url = URL.createObjectURL(blob);

    try {
      const reader = await AvroReader.fromUrl(url, {
        readerSchema: WEATHER_READER_SCHEMA,
      });

      const records = await readAllRecords(reader);
      assertWeatherStationTempRecords(records);
    } finally {
      URL.revokeObjectURL(url);
    }
  });

  it("should support close() method", async () => {
    const buffer = await createWeatherAvroBuffer();
    const reader = AvroReader.fromBuffer(buffer);

    // Should be able to close without error
    await reader.close();

    // Should be able to close multiple times without error
    await reader.close();
    await reader.close();
  });

  it("should call closeHook when closing", async () => {
    const buffer = await createWeatherAvroBuffer();
    let closeHookCalled = false;

    const reader = AvroReader.fromBuffer(buffer, {
      closeHook: () => {
        closeHookCalled = true;
      },
    });

    await reader.close();
    assertEquals(closeHookCalled, true);
  });

  it("should handle async closeHook", async () => {
    const buffer = await createWeatherAvroBuffer();
    let closeHookCalled = false;

    const reader = AvroReader.fromBuffer(buffer, {
      closeHook: async () => {
        await new Promise((resolve) => setTimeout(resolve, 1));
        closeHookCalled = true;
      },
    });

    await reader.close();
    assertEquals(closeHookCalled, true);
  });

  it("should handle closeHook that throws", async () => {
    const buffer = await createWeatherAvroBuffer();

    const reader = AvroReader.fromBuffer(buffer, {
      closeHook: () => {
        throw new Error("Close hook error");
      },
    });

    // Should not throw, close hook errors are ignored
    await reader.close();
  });

  it("should call closeHook only once", async () => {
    const buffer = await createWeatherAvroBuffer();
    let callCount = 0;

    const reader = AvroReader.fromBuffer(buffer, {
      closeHook: () => {
        callCount++;
      },
    });

    await reader.close();
    await reader.close();
    await reader.close();

    assertEquals(callCount, 1);
  });

  it("should support closeHook in fromStream", async () => {
    const stream = await createWeatherAvroStream();
    let closeHookCalled = false;

    const reader = AvroReader.fromStream(stream, {
      closeHook: () => {
        closeHookCalled = true;
      },
    });

    await reader.close();
    assertEquals(closeHookCalled, true);
  });

  it("should call closeHook in fromUrl", async () => {
    let cancelCalled = false;

    class MockReadableStream extends ReadableStream<Uint8Array> {
      override cancel() {
        cancelCalled = true;
        return Promise.resolve();
      }
    }

    const mockStream = new MockReadableStream();

    // Mock fetch to return a response with the mock stream
    const originalFetch = globalThis.fetch;
    globalThis.fetch = () =>
      Promise.resolve({
        ok: true,
        body: mockStream,
      } as Response);

    try {
      const reader = await AvroReader.fromUrl("http://example.com/test.avro");
      await reader.close();
      assertEquals(cancelCalled, true);
    } finally {
      // Restore original fetch
      globalThis.fetch = originalFetch;
    }
  });

  describe("Binary Compatibility Tests", () => {
    /**
     * Load the syncInMeta.avro test file data.
     */
    async function loadSyncInMetaAvroFile(): Promise<Uint8Array> {
      return await Deno.readFile("test-data/syncInMeta.avro");
    }

    /**
     * Create an InMemoryReadableBuffer from the syncInMeta.avro file.
     */
    async function createSyncInMetaAvroBuffer(): Promise<
      InMemoryReadableBuffer
    > {
      const fileData = await loadSyncInMetaAvroFile();
      return new InMemoryReadableBuffer(fileData.buffer as ArrayBuffer);
    }

    /**
     * Create a Blob from the syncInMeta.avro file.
     */
    async function createSyncInMetaAvroBlob(): Promise<Blob> {
      const fileData = await loadSyncInMetaAvroFile();
      return new Blob([fileData.buffer as ArrayBuffer]);
    }

    /**
     * Create a ReadableStream from the syncInMeta.avro file.
     */
    async function createSyncInMetaAvroStream(): Promise<ReadableStream> {
      const fileData = await loadSyncInMetaAvroFile();
      return new ReadableStream({
        start(controller) {
          controller.enqueue(fileData);
          controller.close();
        },
      });
    }

    /**
     * Verify that header has expected properties for syncInMeta.avro.
     */
    function assertSyncInMetaHeader(header: ParsedAvroHeader): void {
      assertEquals(typeof header, "object");
      assertEquals(header.magic.length, 4);
      assertEquals(header.sync.length, 16);
      const schemaJson = header.meta.get("avro.schema");
      assert(schemaJson);
      const schema = JSON.parse(new TextDecoder().decode(schemaJson));
      assertEquals(typeof schema, "object");
      // The file should have sync metadata
      const syncMeta = header.meta.get("avro.sync");
      assert(syncMeta);
      assertEquals(syncMeta.length, 16);
    }

    /**
     * Verify that records match expected syncInMeta data structure.
     */
    function assertSyncInMetaRecords(records: unknown[]): void {
      assertEquals(records.length, 6001);

      for (let i = 0; i < records.length; i++) {
        const record = records[i] as Record<string, unknown>;
        const expectedFields = ["Age", "First", "ID", "Last", "Phone"];
        assertEquals(Object.keys(record).sort(), expectedFields);

        // Verify that all expected fields exist
        assertEquals(typeof record.Age, "number");
        assertEquals(typeof record.First, "string");
        assertEquals(typeof record.Last, "string");
        assertEquals(typeof record.ID, "bigint");
        assertEquals(typeof record.Phone, "string");
      }
    }

    it("should read syncInMeta.avro file header and records from buffer", async () => {
      const buffer = await createSyncInMetaAvroBuffer();
      const reader = AvroReader.fromBuffer(buffer);

      // Verify header access - should not fail
      const header = await reader.getHeader();
      assertSyncInMetaHeader(header);

      // Read all records and verify structure
      const records = await readAllRecords(reader);
      assertSyncInMetaRecords(records);
    });

    it("should read syncInMeta.avro file header and records from blob", async () => {
      const blob = await createSyncInMetaAvroBlob();
      const reader = AvroReader.fromBlob(blob);

      // Verify header access - should not fail
      const header = await reader.getHeader();
      assertSyncInMetaHeader(header);

      // Read all records and verify structure
      const records = await readAllRecords(reader);
      assertSyncInMetaRecords(records);
    });

    it("should read syncInMeta.avro file header and records from stream", async () => {
      const stream = await createSyncInMetaAvroStream();
      const reader = AvroReader.fromStream(stream);

      // Verify header access - should not fail
      const header = await reader.getHeader();
      assertSyncInMetaHeader(header);

      // Read all records and verify structure
      const records = await readAllRecords(reader);
      assertSyncInMetaRecords(records);
    });
  });
});
