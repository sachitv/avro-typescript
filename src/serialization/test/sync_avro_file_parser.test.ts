import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { createType } from "../../type/create_type.ts";
import {
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "../buffers/sync_in_memory_buffer.ts";
import { SyncAvroFileParser } from "../sync_avro_file_parser.ts";
import type { SyncAvroFileParserOptions } from "../sync_avro_file_parser.ts";
import { SyncWritableTap } from "../sync_tap.ts";
import { BLOCK_TYPE, HEADER_TYPE, MAGIC_BYTES } from "../avro_constants.ts";
import { SyncFixedSizeStreamReader } from "../streams/sync_fixed_size_stream_reader.ts";
import { SyncStreamReadableBufferAdapter } from "../streams/sync_stream_readable_buffer_adapter.ts";

const WEATHER_RECORDS = [
  { station: "011990-99999", time: -619524000000n, temp: 0 },
  { station: "011990-99999", time: -619506000000n, temp: 22 },
  { station: "011990-99999", time: -619484400000n, temp: -11 },
  { station: "012650-99999", time: -655531200000n, temp: 111 },
  { station: "012650-99999", time: -655509600000n, temp: 78 },
] as const;

const WEATHER_READER_SCHEMA = {
  type: "record",
  name: "test.Weather",
  fields: [
    { name: "station", type: "string" },
    { name: "temp", type: "int" },
  ],
} as const;

const BOOLEAN_TYPE = createType("boolean");
const TEXT_ENCODER = new TextEncoder();

function toArrayBuffer(data: Uint8Array): ArrayBuffer {
  const copy = data.slice();
  return copy.buffer;
}

async function loadWeatherBuffer(): Promise<SyncInMemoryReadableBuffer> {
  const fileData = await Deno.readFile("test-data/weather.avro");
  return new SyncInMemoryReadableBuffer(toArrayBuffer(fileData));
}

async function loadWeatherZstdBuffer(): Promise<SyncInMemoryReadableBuffer> {
  const fileData = await Deno.readFile("test-data/weather-zstd.avro");
  return new SyncInMemoryReadableBuffer(toArrayBuffer(fileData));
}

function assertWeatherRecords(records: unknown[]): void {
  assertEquals(records.length, WEATHER_RECORDS.length);
  for (let i = 0; i < records.length; i++) {
    const actual = records[i] as Record<string, unknown>;
    const expected = WEATHER_RECORDS[i];
    assertEquals(actual.station, expected.station);
    assertEquals(actual.time, expected.time);
    assertEquals(actual.temp, expected.temp);
  }
}

function assertWeatherStationTempRecords(records: unknown[]): void {
  assertEquals(records.length, WEATHER_RECORDS.length);
  for (let i = 0; i < records.length; i++) {
    const actual = records[i] as Record<string, unknown>;
    const expected = WEATHER_RECORDS[i];
    assertEquals(Object.keys(actual).sort(), ["station", "temp"]);
    assertEquals(actual.station, expected.station);
    assertEquals(actual.temp, expected.temp);
  }
}

function readAll(parser: SyncAvroFileParser): unknown[] {
  return Array.from(parser.iterRecords());
}

function encodeBooleanValues(values: boolean[]): Uint8Array {
  if (values.length === 0) {
    return new Uint8Array();
  }
  const dataBuffer = new ArrayBuffer(values.length);
  const writable = new SyncInMemoryWritableBuffer(dataBuffer);
  const tap = new SyncWritableTap(writable);
  for (const value of values) {
    BOOLEAN_TYPE.writeSync(tap, value);
  }
  return new Uint8Array(writable.getBufferCopy());
}

interface BooleanBufferOptions {
  codec?: string;
  values?: boolean[];
  includeBlock?: boolean;
}

function createBooleanContainer(
  { codec, values = [true], includeBlock = true }: BooleanBufferOptions = {},
): SyncInMemoryReadableBuffer {
  const arrayBuffer = new ArrayBuffer(2048);
  const writable = new SyncInMemoryWritableBuffer(arrayBuffer);
  const tap = new SyncWritableTap(writable);
  const syncBytes = new Uint8Array(16);
  syncBytes.fill(0x42);
  const meta = new Map<string, Uint8Array>();
  meta.set("avro.schema", TEXT_ENCODER.encode('"boolean"'));
  if (codec !== undefined) {
    meta.set("avro.codec", TEXT_ENCODER.encode(codec));
  }
  const header = {
    magic: MAGIC_BYTES.slice(),
    meta,
    sync: syncBytes,
  };
  HEADER_TYPE.writeSync(tap, header);
  if (includeBlock) {
    const block = {
      count: BigInt(values.length),
      data: encodeBooleanValues(values),
      sync: syncBytes,
    };
    BLOCK_TYPE.writeSync(tap, block);
  }
  return new SyncInMemoryReadableBuffer(writable.getBufferCopy());
}

function createMultiBlockBuffer(): SyncInMemoryReadableBuffer {
  const arrayBuffer = new ArrayBuffer(2048);
  const writable = new SyncInMemoryWritableBuffer(arrayBuffer);
  const tap = new SyncWritableTap(writable);
  const syncBytes = new Uint8Array(16);
  syncBytes.fill(0x33);
  const meta = new Map<string, Uint8Array>();
  meta.set("avro.schema", TEXT_ENCODER.encode('"boolean"'));
  const header = {
    magic: MAGIC_BYTES.slice(),
    meta,
    sync: syncBytes,
  };
  HEADER_TYPE.writeSync(tap, header);

  const firstBlock = {
    count: 1n,
    data: encodeBooleanValues([true]),
    sync: syncBytes,
  };
  const secondBlock = {
    count: 1n,
    data: encodeBooleanValues([false]),
    sync: syncBytes,
  };
  BLOCK_TYPE.writeSync(tap, firstBlock);
  BLOCK_TYPE.writeSync(tap, secondBlock);
  return new SyncInMemoryReadableBuffer(writable.getBufferCopy());
}

function createHeaderWithoutSchema(): SyncInMemoryReadableBuffer {
  const arrayBuffer = new ArrayBuffer(256);
  const writable = new SyncInMemoryWritableBuffer(arrayBuffer);
  const tap = new SyncWritableTap(writable);
  const meta = new Map<string, Uint8Array>();
  const header = {
    magic: MAGIC_BYTES.slice(),
    meta,
    sync: new Uint8Array(16),
  };
  HEADER_TYPE.writeSync(tap, header);
  return new SyncInMemoryReadableBuffer(writable.getBufferCopy());
}

describe("SyncAvroFileParser", () => {
  it("parses header and records from weather.avro", async () => {
    const buffer = await loadWeatherBuffer();
    const parser = new SyncAvroFileParser(buffer);

    const header1 = parser.getHeader();
    assertEquals(header1.magic.length, 4);
    assertEquals(header1.sync.length, 16);
    assert(header1.meta.get("avro.schema"));

    const header2 = parser.getHeader();
    assertEquals(header1, header2);

    const records = readAll(parser);
    assertWeatherRecords(records);
  });

  it("supports reader schema object", async () => {
    const buffer = await loadWeatherBuffer();
    const parser = new SyncAvroFileParser(buffer, {
      readerSchema: WEATHER_READER_SCHEMA,
    });
    const records = readAll(parser);
    assertWeatherStationTempRecords(records);
  });

  it("supports reader schema as JSON string", async () => {
    const buffer = await loadWeatherBuffer();
    const parser = new SyncAvroFileParser(buffer, {
      readerSchema: JSON.stringify(WEATHER_READER_SCHEMA),
    });
    const records = readAll(parser);
    assertWeatherStationTempRecords(records);
  });

  it("supports reader schema as Type instance", async () => {
    const buffer = await loadWeatherBuffer();
    const parser = new SyncAvroFileParser(buffer, {
      readerSchema: createType(WEATHER_READER_SCHEMA),
    });
    const records = readAll(parser);
    assertWeatherStationTempRecords(records);
  });

  it("treats null reader schema like undefined", async () => {
    const buffer = await loadWeatherBuffer();
    const parser = new SyncAvroFileParser(buffer, { readerSchema: null });
    const records = readAll(parser);
    assertWeatherRecords(records);
  });

  it("caches reader type and resolver across iterators", async () => {
    const buffer = await loadWeatherBuffer();
    const parser = new SyncAvroFileParser(buffer, {
      readerSchema: WEATHER_READER_SCHEMA,
    });
    // fully consume to populate caches
    readAll(parser);

    const second = parser.iterRecords();
    const { value, done } = second.next();
    assertEquals(done, true);
    assertEquals(value, undefined);
  });

  it("throws for incompatible reader schema", async () => {
    const buffer = await loadWeatherBuffer();
    const parser = new SyncAvroFileParser(buffer, { readerSchema: "string" });
    assertThrows(() => readAll(parser));
  });

  it("throws for invalid reader schema type", () => {
    const buffer = createBooleanContainer();
    const parser = new SyncAvroFileParser(buffer, {
      readerSchema: 123 as unknown,
    });
    assertThrows(() => readAll(parser));
  });

  it("throws for invalid magic bytes", async () => {
    const data = await Deno.readFile("test-data/weather.avro");
    const corrupted = data.slice();
    corrupted[0] = 0;
    corrupted[1] = 0;
    const buffer = new SyncInMemoryReadableBuffer(toArrayBuffer(corrupted));
    const parser = new SyncAvroFileParser(buffer);
    assertThrows(() => parser.getHeader(), Error, "Invalid AVRO file");
  });

  it("throws when schema metadata missing", () => {
    const buffer = createHeaderWithoutSchema();
    const parser = new SyncAvroFileParser(buffer);
    assertThrows(() => parser.getHeader(), Error, "AVRO schema not found");
  });

  it("handles header without blocks", () => {
    const buffer = createBooleanContainer({ includeBlock: false });
    const parser = new SyncAvroFileParser(buffer);
    parser.getHeader();
    const records = readAll(parser);
    assertEquals(records.length, 0);
  });

  it("handles zero-record block", () => {
    const buffer = createBooleanContainer({ values: [] });
    const parser = new SyncAvroFileParser(buffer);
    const records = readAll(parser);
    assertEquals(records.length, 0);
  });

  it("handles multiple blocks", () => {
    const buffer = createMultiBlockBuffer();
    const parser = new SyncAvroFileParser(buffer);
    const records = readAll(parser);
    assertEquals(records, [true, false]);
  });

  it("handles empty and missing codec metadata", () => {
    const bufferMissing = createBooleanContainer();
    const missingCodecParser = new SyncAvroFileParser(bufferMissing);
    const missingHeader = missingCodecParser.getHeader();
    assertEquals(missingHeader.meta.get("avro.codec"), undefined);
    assertEquals(readAll(missingCodecParser), [true]);

    const bufferEmpty = createBooleanContainer({ codec: "" });
    const emptyParser = new SyncAvroFileParser(bufferEmpty);
    const emptyHeader = emptyParser.getHeader();
    assertEquals(
      new TextDecoder().decode(emptyHeader.meta.get("avro.codec")),
      "",
    );
    assertEquals(readAll(emptyParser), [true]);
  });

  it("handles explicit null codec", () => {
    const buffer = createBooleanContainer({ codec: "null" });
    const parser = new SyncAvroFileParser(buffer);
    const header = parser.getHeader();
    assertEquals(
      new TextDecoder().decode(header.meta.get("avro.codec")),
      "null",
    );
    assertEquals(readAll(parser), [true]);
  });

  it("supports reading via stream adapter", async () => {
    const file = await Deno.readFile("test-data/weather.avro");
    const reader = new SyncFixedSizeStreamReader(file, 128);
    const adapter = new SyncStreamReadableBufferAdapter(reader);
    const parser = new SyncAvroFileParser(adapter);
    const records = readAll(parser);
    assertWeatherRecords(records);
  });

  it("throws for truncated files", async () => {
    const data = await Deno.readFile("test-data/weather.avro");
    const truncated = data.slice(0, 256);
    const buffer = new SyncInMemoryReadableBuffer(toArrayBuffer(truncated));
    const parser = new SyncAvroFileParser(buffer);
    // header may succeed but iterating should fail
    parser.getHeader();
    assertThrows(() => readAll(parser));
  });

  it("throws for unsupported codec", async () => {
    const buffer = await loadWeatherZstdBuffer();
    const parser = new SyncAvroFileParser(buffer);
    assertThrows(
      () => parser.getHeader(),
      Error,
      "Unsupported codec: zstandard",
    );
  });

  it("supports custom decoders", () => {
    const buffer = createBooleanContainer({ codec: "custom" });
    const parser = new SyncAvroFileParser(buffer, {
      decoders: { "custom": { decode: (data) => data } },
    });
    const header = parser.getHeader();
    assertEquals(
      new TextDecoder().decode(header.meta.get("avro.codec")),
      "custom",
    );
    assertEquals(readAll(parser), [true]);
  });

  it("rejects overriding built-in decoders", () => {
    const buffer = createBooleanContainer();
    assertThrows(
      () =>
        new SyncAvroFileParser(buffer, {
          decoders: { "null": { decode: (data) => data } },
        } as SyncAvroFileParserOptions),
      Error,
      "Cannot override built-in decoder",
    );
  });
});
