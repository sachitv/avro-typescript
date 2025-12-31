import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { SyncAvroWriter } from "../avro_writer_sync.ts";
import { SyncAvroReader } from "../avro_reader_sync.ts";
import {
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "../serialization/buffers/in_memory_buffer_sync.ts";
import { SyncFixedSizeStreamWriter } from "../serialization/streams/fixed_size_stream_writer_sync.ts";

const SIMPLE_SCHEMA = {
  type: "record",
  name: "example.Event",
  fields: [
    { name: "id", type: "int" },
    { name: "message", type: "string" },
  ],
} as const;

const SAMPLE_RECORDS = [
  { id: 1, message: "one" },
  { id: 2, message: "two" },
  { id: 3, message: "three" },
];

function toArrayBuffer(data: Uint8Array): ArrayBuffer {
  const copy = data.slice();
  return copy.buffer;
}

function readAllRecords(buffer: ArrayBuffer): unknown[] {
  const reader = SyncAvroReader.fromBuffer(
    new SyncInMemoryReadableBuffer(buffer),
  );
  return Array.from(reader.iterRecords());
}

class TrackingFixedSizeStreamWriter extends SyncFixedSizeStreamWriter {
  public closeCount = 0;

  constructor(size: number) {
    super(new Uint8Array(size));
  }

  override close(): void {
    this.closeCount++;
    super.close();
  }
}

describe("SyncAvroWriter", () => {
  it("writes records to a sync buffer", () => {
    const arrayBuffer = new ArrayBuffer(4096);
    const writable = new SyncInMemoryWritableBuffer(arrayBuffer);
    const writer = SyncAvroWriter.toBuffer(writable, { schema: SIMPLE_SCHEMA });

    for (const record of SAMPLE_RECORDS) {
      writer.append(record);
    }
    writer.close();

    const data = writable.getBufferCopy();
    const records = readAllRecords(data);
    assertEquals(records, SAMPLE_RECORDS);
  });

  it("respects manual flushBlock calls", () => {
    const arrayBuffer = new ArrayBuffer(4096);
    const writable = new SyncInMemoryWritableBuffer(arrayBuffer);
    const writer = SyncAvroWriter.toBuffer(writable, {
      schema: SIMPLE_SCHEMA,
      blockSize: 1,
    });

    for (const record of SAMPLE_RECORDS) {
      writer.append(record);
      writer.flushBlock();
    }
    writer.close();

    const records = readAllRecords(writable.getBufferCopy());
    assertEquals(records, SAMPLE_RECORDS);
  });

  it("allows close to be called multiple times", () => {
    const arrayBuffer = new ArrayBuffer(4096);
    const writable = new SyncInMemoryWritableBuffer(arrayBuffer);
    const writer = SyncAvroWriter.toBuffer(writable, { schema: SIMPLE_SCHEMA });

    writer.append(SAMPLE_RECORDS[0]);
    writer.close();
    writer.close();

    const records = readAllRecords(writable.getBufferCopy());
    assertEquals(records.length, 1);
  });

  it("writes to a synchronous stream sink and closes it once", () => {
    const stream = new TrackingFixedSizeStreamWriter(8192);
    const writer = SyncAvroWriter.toStream(stream, { schema: SIMPLE_SCHEMA });

    for (const record of SAMPLE_RECORDS) {
      writer.append(record);
    }
    writer.close();
    writer.close();
    assertEquals(stream.closeCount, 1);

    const bytes = stream.toUint8Array();
    const records = readAllRecords(toArrayBuffer(bytes));
    assertEquals(records, SAMPLE_RECORDS);
  });
});
