import { assert, assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { AvroFileParser } from "./avro_file_parser.ts";
import { InMemoryReadableBuffer } from "./buffers/in_memory_buffer.ts";
import { createType } from "../createType/mod.ts";
import type { ParsedAvroHeader } from "./avro_file_parser.ts";

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
 * Load the weather-zstd.avro test file data.
 */
async function loadWeatherZstdAvroFile(): Promise<Uint8Array> {
  return await Deno.readFile("../../share/test/data/weather-zstd.avro");
}

/**
 * Create an InMemoryReadableBuffer from the weather-zstd.avro file.
 */
async function createWeatherZstdAvroBuffer(): Promise<InMemoryReadableBuffer> {
  const fileData = await loadWeatherZstdAvroFile();
  return new InMemoryReadableBuffer(fileData.buffer as ArrayBuffer);
}

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
