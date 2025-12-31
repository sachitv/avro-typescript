import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  SyncAvroReader,
  type SyncAvroReaderInstance,
} from "../avro_reader_sync.ts";
import {
  SyncInMemoryReadableBuffer,
} from "../serialization/buffers/in_memory_buffer_sync.ts";
import { SyncFixedSizeStreamReader } from "../serialization/streams/fixed_size_stream_reader_sync.ts";
import type {
  ISyncStreamReadableBuffer,
} from "../serialization/streams/streams_sync.ts";

const EXPECTED_WEATHER_RECORDS = [
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

const TEXT_DECODER = new TextDecoder();

function toArrayBuffer(data: Uint8Array): ArrayBuffer {
  const copy = data.slice();
  return copy.buffer;
}

async function loadWeatherBuffer(): Promise<SyncInMemoryReadableBuffer> {
  const fileData = await Deno.readFile("test-data/weather.avro");
  return new SyncInMemoryReadableBuffer(toArrayBuffer(fileData));
}

function readAll(reader: SyncAvroReaderInstance): unknown[] {
  return Array.from(reader.iterRecords());
}

function assertWeatherRecords(records: unknown[]): void {
  assertEquals(records.length, EXPECTED_WEATHER_RECORDS.length);
  for (let i = 0; i < records.length; i++) {
    const actual = records[i] as Record<string, unknown>;
    const expected = EXPECTED_WEATHER_RECORDS[i];
    assertEquals(actual.station, expected.station);
    assertEquals(actual.time, expected.time);
    assertEquals(actual.temp, expected.temp);
  }
}

function assertStationTempRecords(records: unknown[]): void {
  assertEquals(records.length, EXPECTED_WEATHER_RECORDS.length);
  for (let i = 0; i < records.length; i++) {
    const record = records[i] as Record<string, unknown>;
    assertEquals(Object.keys(record).sort(), ["station", "temp"]);
    assertEquals(record.station, EXPECTED_WEATHER_RECORDS[i].station);
    assertEquals(record.temp, EXPECTED_WEATHER_RECORDS[i].temp);
  }
}

class FsSyncStreamReader implements ISyncStreamReadableBuffer {
  #file: Deno.FsFile;
  #chunkSize: number;
  #closed = false;

  constructor(path: string, chunkSize = 512) {
    this.#file = Deno.openSync(path, { read: true });
    this.#chunkSize = chunkSize;
  }

  readNext(): Uint8Array | undefined {
    if (this.#closed) {
      return undefined;
    }
    const buffer = new Uint8Array(this.#chunkSize);
    const bytesRead = this.#file.readSync(buffer);
    if (bytesRead === null || bytesRead === 0) {
      this.close();
      return undefined;
    }
    return buffer.slice(0, bytesRead);
  }

  close(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.#file.close();
    }
  }

  isClosed(): boolean {
    return this.#closed;
  }
}

class TrackingStreamReader extends SyncFixedSizeStreamReader {
  #closed = false;

  constructor(source: Uint8Array, chunkSize: number) {
    super(source, chunkSize);
  }

  override close(): void {
    this.#closed = true;
    super.close();
  }

  public closed(): boolean {
    return this.#closed;
  }
}

describe("SyncAvroReader", () => {
  it("reads weather.avro via sync buffer", async () => {
    const buffer = await loadWeatherBuffer();
    const reader = SyncAvroReader.fromBuffer(buffer);

    const header = reader.getHeader();
    const schemaJson = header.meta.get("avro.schema");
    assertEquals(Boolean(schemaJson), true);
    const schema = JSON.parse(TEXT_DECODER.decode(schemaJson!));
    assertEquals(schema.name, "Weather");
    assertEquals(schema.namespace, "test");

    const records = readAll(reader);
    assertWeatherRecords(records);
  });

  it("applies reader schema when provided", async () => {
    const buffer = await loadWeatherBuffer();
    const reader = SyncAvroReader.fromBuffer(buffer, {
      readerSchema: WEATHER_READER_SCHEMA,
    });

    const records = readAll(reader);
    assertStationTempRecords(records);
  });

  it("reads using fixed-size sync stream adapter", async () => {
    const fileBytes = await Deno.readFile("test-data/weather.avro");
    const stream = new SyncFixedSizeStreamReader(fileBytes, 256);
    const reader = SyncAvroReader.fromStream(stream);
    const records = readAll(reader);
    assertWeatherRecords(records);
  });

  it("closes the underlying stream and user hook exactly once", async () => {
    const fileBytes = await Deno.readFile("test-data/weather.avro");
    const trackingStream = new TrackingStreamReader(fileBytes, 128);
    let closeHookCalls = 0;
    const reader = SyncAvroReader.fromStream(trackingStream, {
      readerSchema: WEATHER_READER_SCHEMA,
      closeHook: () => {
        closeHookCalls++;
        if (closeHookCalls === 1) {
          throw new Error("close hook failure");
        }
      },
    });

    const records = readAll(reader);
    assertStationTempRecords(records);

    reader.close();
    reader.close();
    assertEquals(closeHookCalls, 1);
    assertEquals(trackingStream.closed(), true);
  });

  it("supports FsSyncStreamReader for Deno files", () => {
    const stream = new FsSyncStreamReader("test-data/weather.avro", 320);
    const reader = SyncAvroReader.fromStream(stream);
    const records = readAll(reader);
    assertWeatherRecords(records);

    reader.close();
    assertEquals(stream.isClosed(), true);
  });

  it("ignores close hook errors for buffer readers", async () => {
    const buffer = await loadWeatherBuffer();
    let callCount = 0;
    const reader = SyncAvroReader.fromBuffer(buffer, {
      closeHook: () => {
        callCount++;
        throw new Error("boom");
      },
    });

    reader.close();
    reader.close();
    assertEquals(callCount, 1);
  });
});
