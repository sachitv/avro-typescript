import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  AvroFileParser,
  BLOCK_TYPE,
  HEADER_TYPE,
} from "../avro_file_parser.ts";
import {
  InMemoryReadableBuffer,
  InMemoryWritableBuffer,
} from "../buffers/in_memory_buffer.ts";
import { WritableTap } from "../tap.ts";
import { createType } from "../../type/create_type.ts";
import type { ParsedAvroHeader } from "../avro_file_parser.ts";

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

describe("AvroFileParser", () => {
  it("should parse header from weather.avro file", async () => {
    const buffer = await createWeatherAvroBuffer();
    const parser = new AvroFileParser(buffer);

    const header = await parser.getHeader();
    assertWeatherHeader(header);
  });

  it("should iterate records from weather.avro file", async () => {
    const buffer = await createWeatherAvroBuffer();
    const parser = new AvroFileParser(buffer);

    const records = [];
    for await (const record of parser.iterRecords()) {
      records.push(record);
    }
    assertWeatherRecords(records);
  });

  it("should iterate records with a reader schema", async () => {
    const buffer = await createWeatherAvroBuffer();
    const parser = new AvroFileParser(buffer, {
      readerSchema: WEATHER_READER_SCHEMA,
    });

    const records = [];
    for await (const record of parser.iterRecords()) {
      records.push(record);
    }
    assertWeatherStationTempRecords(records);
  });

  it("should iterate records with a reader schema as JSON string", async () => {
    const buffer = await createWeatherAvroBuffer();
    const parser = new AvroFileParser(buffer, {
      readerSchema: JSON.stringify(WEATHER_READER_SCHEMA),
    });

    const records = [];
    for await (const record of parser.iterRecords()) {
      records.push(record);
    }
    assertWeatherStationTempRecords(records);
  });

  it("should iterate records with null reader schema", async () => {
    const buffer = await createWeatherAvroBuffer();
    const parser = new AvroFileParser(buffer, {
      readerSchema: null,
    });

    const records = [];
    for await (const record of parser.iterRecords()) {
      records.push(record);
    }
    assertWeatherRecords(records);
  });

  it("should iterate records with reader schema as Type instance", async () => {
    const buffer = await createWeatherAvroBuffer();
    const readerType = createType(WEATHER_READER_SCHEMA);
    const parser = new AvroFileParser(buffer, {
      readerSchema: readerType,
    });

    const records = [];
    for await (const record of parser.iterRecords()) {
      records.push(record);
    }
    assertWeatherStationTempRecords(records);
  });

  it("should use cached reader type and resolver on subsequent iterRecords calls", async () => {
    const buffer = await createWeatherAvroBuffer();
    const parser = new AvroFileParser(buffer, {
      readerSchema: WEATHER_READER_SCHEMA,
    });

    // First iterRecords call creates and caches readerType and resolver
    const _iter1 = parser.iterRecords();

    // Second iterRecords call should use cached readerType and resolver
    const iter2 = parser.iterRecords();
    const records = [];
    for await (const record of iter2) {
      records.push(record);
    }
    assertWeatherStationTempRecords(records);
  });

  it("should use cached reader type when reader schema is a Type instance", async () => {
    const buffer = await createWeatherAvroBuffer();
    const readerType = createType(WEATHER_READER_SCHEMA);
    const parser = new AvroFileParser(buffer, {
      readerSchema: readerType,
    });

    // First iterRecords call with Type instance
    const _iter1 = parser.iterRecords();

    // Second iterRecords call should use cached readerType
    const iter2 = parser.iterRecords();
    const records = [];
    for await (const record of iter2) {
      records.push(record);
    }
    assertWeatherStationTempRecords(records);
  });

  it("should throw error for incompatible reader schema", async () => {
    const buffer = await createWeatherAvroBuffer();
    const parser = new AvroFileParser(buffer, {
      readerSchema: "string",
    });

    await assertRejects(
      async () => {
        for await (const _record of parser.iterRecords()) {
          // Should not reach here
        }
      },
      Error,
      "Schema evolution not supported",
    );
  });

  it("should throw error for bytes reader schema on record data", async () => {
    const buffer = await createWeatherAvroBuffer();
    const parser = new AvroFileParser(buffer, {
      readerSchema: "bytes",
    });

    await assertRejects(
      async () => {
        for await (const _record of parser.iterRecords()) {
          // Should not reach here
        }
      },
      Error,
      "Schema evolution not supported",
    );
  });
});

it("should reject invalid magic bytes", async () => {
  const fileData = await loadWeatherAvroFile();
  // Modify magic bytes to invalid
  fileData[0] = 0;
  fileData[1] = 0;
  fileData[2] = 0;
  fileData[3] = 0;
  const buffer = new InMemoryReadableBuffer(fileData.buffer as ArrayBuffer);
  const parser = new AvroFileParser(buffer);

  await assertRejects(
    async () => {
      await parser.getHeader();
    },
    Error,
    "Invalid AVRO file: incorrect magic bytes",
  );
});

it("should reject missing avro.schema in meta", async () => {
  // Create a buffer with valid magic, empty meta, and dummy sync
  const magic = new Uint8Array([0x4F, 0x62, 0x6A, 0x01]); // 'Obj\x01'
  const emptyMeta = new Uint8Array([0]); // varint 0 for empty map
  const dummySync = new Uint8Array(16); // dummy sync
  const headerData = new Uint8Array(
    magic.length + emptyMeta.length + dummySync.length,
  );
  headerData.set(magic, 0);
  headerData.set(emptyMeta, magic.length);
  headerData.set(dummySync, magic.length + emptyMeta.length);
  const buffer = new InMemoryReadableBuffer(headerData.buffer as ArrayBuffer);
  const parser = new AvroFileParser(buffer);

  await assertRejects(
    async () => {
      await parser.getHeader();
    },
    Error,
    "AVRO schema not found in metadata",
  );
});

it("should cache header on multiple calls", async () => {
  const buffer = await createWeatherAvroBuffer();
  const parser = new AvroFileParser(buffer);

  const header1 = await parser.getHeader();
  const header2 = await parser.getHeader();

  assertEquals(header1, header2); // Same object or equal
  assertWeatherHeader(header1);
});

/**
 * Load the weather-deflate.avro test file data.
 */
async function loadWeatherDeflateAvroFile(): Promise<Uint8Array> {
  return await Deno.readFile("test-data/weather-deflate.avro");
}

/**
 * Load the weather-zstd.avro test file data.
 */
async function loadWeatherZstdAvroFile(): Promise<Uint8Array> {
  return await Deno.readFile("test-data/weather-zstd.avro");
}

/**
 * Create an InMemoryReadableBuffer from the weather-deflate.avro file.
 */
async function createWeatherDeflateAvroBuffer(): Promise<
  InMemoryReadableBuffer
> {
  const fileData = await loadWeatherDeflateAvroFile();
  return new InMemoryReadableBuffer(fileData.buffer as ArrayBuffer);
}

/**
 * Create an InMemoryReadableBuffer from the weather-zstd.avro file.
 */
async function createWeatherZstdAvroBuffer(): Promise<InMemoryReadableBuffer> {
  const fileData = await loadWeatherZstdAvroFile();
  return new InMemoryReadableBuffer(fileData.buffer as ArrayBuffer);
}

it("should parse header from weather-deflate.avro file", async () => {
  const buffer = await createWeatherDeflateAvroBuffer();
  const parser = new AvroFileParser(buffer);

  const header = await parser.getHeader();
  assertEquals(typeof header, "object");
  assertEquals(header.magic.length, 4);
  assertEquals(header.sync.length, 16);
  const schemaJson = header.meta.get("avro.schema");
  assert(schemaJson);
  const schema = JSON.parse(new TextDecoder().decode(schemaJson));
  assertEquals(typeof schema, "object");
  const codec = header.meta.get("avro.codec");
  assertEquals(codec ? new TextDecoder().decode(codec) : undefined, "deflate");
});

it("should iterate records from weather-deflate.avro file", async () => {
  const buffer = await createWeatherDeflateAvroBuffer();
  const parser = new AvroFileParser(buffer);

  const records = [];
  for await (const record of parser.iterRecords()) {
    records.push(record);
  }
  assert(records.length > 0, "Should have at least one record");
  // Check that records have the expected structure
  for (const record of records) {
    assert(typeof record === "object" && record !== null);
    assert("station" in record);
    assert("time" in record);
    assert("temp" in record);
  }
});

it("should reject unsupported zstd codec", async () => {
  const buffer = await createWeatherZstdAvroBuffer();
  const parser = new AvroFileParser(buffer);

  await assertRejects(
    async () => {
      await parser.getHeader();
    },
    Error,
    "Unsupported codec: zstandard",
  );
});

it("should use custom decoders for zstandard codec", async () => {
  const buffer = await createWeatherZstdAvroBuffer();
  const customDecoders = {
    "zstandard": { decode: (data: Uint8Array) => Promise.resolve(data) }, // Mock decoder
  };
  const parser = new AvroFileParser(buffer, { decoders: customDecoders });

  // getHeader should succeed because custom decoder is provided
  const header = await parser.getHeader();
  assertEquals(typeof header, "object");
  assertEquals(header.magic.length, 4);
  assertEquals(header.sync.length, 16);
  const schemaJson = header.meta.get("avro.schema");
  assert(schemaJson);
  const schema = JSON.parse(new TextDecoder().decode(schemaJson));
  assertEquals(typeof schema, "object");
  const codec = header.meta.get("avro.codec");
  assertEquals(
    codec ? new TextDecoder().decode(codec) : undefined,
    "zstandard",
  );
});

it("should handle truncated file gracefully in iterRecords", async () => {
  const fullData = await loadWeatherAvroFile();
  // Truncate the file after the header (use enough bytes for header but not full file)
  const truncatedData = fullData.slice(0, 300);
  const arrayBuffer = new ArrayBuffer(truncatedData.length);
  new Uint8Array(arrayBuffer).set(truncatedData);
  const buffer = new InMemoryReadableBuffer(arrayBuffer);
  const parser = new AvroFileParser(buffer);

  // Should be able to get header
  const header = await parser.getHeader();
  assertWeatherHeader(header);

  // iterRecords should handle the truncation gracefully (no records yielded)
  const records = [];
  for await (const record of parser.iterRecords()) {
    records.push(record);
  }
  assertEquals(records.length, 0);
});

it("should reject custom decoders that override built-in decoders", async () => {
  const buffer = await createWeatherAvroBuffer();
  const customDecoders = {
    "null": { decode: () => Promise.resolve(new Uint8Array()) }, // Trying to override built-in
  };

  assertThrows(
    () => new AvroFileParser(buffer, { decoders: customDecoders }),
    Error,
    "Cannot override built-in decoder for codec: null",
  );
});

/**
 * Helper function to create a test buffer with boolean data.
 * @param includeEmptyCodec If true, sets avro.codec to empty Uint8Array; if false, omits avro.codec
 */
async function createTestBuffer(
  includeEmptyCodec: boolean,
): Promise<InMemoryReadableBuffer> {
  const arrayBuffer = new ArrayBuffer(1024);
  const writableBuffer = new InMemoryWritableBuffer(arrayBuffer);
  const writeTap = new WritableTap(writableBuffer);

  // Constants
  const magicBytes = new Uint8Array([0x4F, 0x62, 0x6A, 0x01]); // 'Obj\x01'
  const syncBytes = new Uint8Array(16);
  syncBytes.fill(0x42); // Fill with pattern for easy identification

  // Create metadata with boolean schema
  const meta = new Map<string, Uint8Array>();
  meta.set("avro.schema", new TextEncoder().encode('"boolean"'));
  if (includeEmptyCodec) {
    meta.set("avro.codec", new Uint8Array(0)); // Empty codec
  }

  // Write header using HEADER_TYPE
  const header = {
    magic: magicBytes,
    meta: meta,
    sync: syncBytes,
  };
  await HEADER_TYPE.write(writeTap, header);

  // Create boolean data (true = 1 byte)
  const booleanType = createType("boolean");
  const dataBuffer = new ArrayBuffer(1);
  const dataWriteTap = new WritableTap(dataBuffer);
  await booleanType.write(dataWriteTap, true);
  const booleanData = new Uint8Array(dataBuffer);

  // Write block using BLOCK_TYPE
  const block = {
    count: 1n, // 1 record
    data: booleanData,
    sync: syncBytes,
  };
  await BLOCK_TYPE.write(writeTap, block);

  return new InMemoryReadableBuffer(arrayBuffer);
}

it("should handle empty avro.codec in meta", async () => {
  const buffer = await createTestBuffer(true);
  const parser = new AvroFileParser(buffer);

  // Verify header
  const parsedHeader = await parser.getHeader();
  assertEquals(parsedHeader.magic.length, 4);
  assertEquals(parsedHeader.sync.length, 16);
  const codecJson = parsedHeader.meta.get("avro.codec");
  assertEquals(codecJson, new Uint8Array(0));

  // Verify records
  const records = [];
  for await (const record of parser.iterRecords()) {
    records.push(record);
  }
  assertEquals(records.length, 1);
  assertEquals(records[0], true);
});

it("should handle missing avro.codec in meta", async () => {
  const buffer = await createTestBuffer(false);
  const parser = new AvroFileParser(buffer);

  // Verify header
  const parsedHeader = await parser.getHeader();
  const codecJson = parsedHeader.meta.get("avro.codec");
  assertEquals(codecJson, undefined);

  // Verify records
  const records = [];
  for await (const record of parser.iterRecords()) {
    records.push(record);
  }
  assertEquals(records.length, 1);
  assertEquals(records[0], true);
});
