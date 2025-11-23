import { assert, assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { AvroWriter } from "../avro_writer.ts";
import { AvroReader } from "../avro_reader.ts";
import type { IWritableBuffer } from "../serialization/buffers/buffer.ts";
import { InMemoryReadableBuffer } from "../serialization/buffers/in_memory_buffer.ts";
import { concatUint8Arrays } from "../internal/collections/array_utils.ts";
import { BLOCK_TYPE } from "../serialization/avro_constants.ts";
import { WritableTap } from "../serialization/tap.ts";

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

async function readAllRecords(
  buffer: ArrayBuffer | ArrayBufferLike,
): Promise<unknown[]> {
  const reader = AvroReader.fromBuffer(
    new InMemoryReadableBuffer(buffer as ArrayBuffer),
  );
  const records: unknown[] = [];
  for await (const record of reader.iterRecords()) {
    records.push(record);
  }
  return records;
}

describe("AvroWriter", () => {
  it("writes records to a writable buffer", async () => {
    const buffer = new CollectingWritableBuffer();
    const writer = AvroWriter.toBuffer(buffer, { schema: SIMPLE_SCHEMA });
    for (const record of SAMPLE_RECORDS) {
      await writer.append(record);
    }
    await writer.close();

    const bufferData = buffer.toArrayBuffer() as ArrayBuffer;
    const records = await readAllRecords(bufferData);
    assertEquals(records, SAMPLE_RECORDS);
  });

  it("writes to a WritableStream and closes it", async () => {
    const chunks: Uint8Array[] = [];
    let closed = false;
    const stream = new WritableStream<Uint8Array>({
      write(chunk) {
        chunks.push(chunk.slice());
      },
      close() {
        closed = true;
      },
    });

    const writer = AvroWriter.toStream(stream, { schema: SIMPLE_SCHEMA });
    for (const record of SAMPLE_RECORDS) {
      await writer.append(record);
    }
    await writer.close();

    assert(closed);
    const totalBytes = concatChunks(chunks);
    const records = await readAllRecords(totalBytes.buffer);
    assertEquals(records, SAMPLE_RECORDS);
  });

  it("supports manual flushBlock calls", async () => {
    const buffer = new CollectingWritableBuffer();
    const writer = AvroWriter.toBuffer(buffer, {
      schema: SIMPLE_SCHEMA,
      blockSize: 1,
    });

    // Append records and manually flush
    await writer.append(SAMPLE_RECORDS[0]);
    await writer.flushBlock();
    await writer.append(SAMPLE_RECORDS[1]);
    await writer.flushBlock();
    await writer.append(SAMPLE_RECORDS[2]);
    await writer.close();

    const bufferData = buffer.toArrayBuffer();
    const records = await readAllRecords(bufferData);
    assertEquals(records, SAMPLE_RECORDS);
  });

  it("handles double close gracefully", async () => {
    const buffer = new CollectingWritableBuffer();
    const writer = AvroWriter.toBuffer(buffer, { schema: SIMPLE_SCHEMA });

    for (const record of SAMPLE_RECORDS) {
      await writer.append(record);
    }
    await writer.close();
    await writer.close(); // Should not error

    const bufferData = buffer.toArrayBuffer();
    const records = await readAllRecords(bufferData);
    assertEquals(records, SAMPLE_RECORDS);
  });

  it("handles files with empty blocks", async () => {
    // This test verifies that the Avro writer and reader can handle files containing empty blocks.
    // Empty blocks can occur in Avro files and should be skipped by readers without issues.
    // The test writes some records, manually inserts empty blocks, writes more records, and ensures all records are read back correctly.
    // Use a fixed sync marker for testing
    const syncMarker = new Uint8Array(16);
    for (let i = 0; i < 16; i++) {
      syncMarker[i] = i;
    }

    const buffer = new CollectingWritableBuffer();
    const writer = AvroWriter.toBuffer(buffer, {
      schema: SIMPLE_SCHEMA,
      syncMarker,
    });

    // Write first record
    await writer.append(SAMPLE_RECORDS[0]);
    await writer.flushBlock();

    // Manually write an empty block
    const tap = new WritableTap(buffer);
    const emptyBlock = {
      count: 0n,
      data: new Uint8Array(0),
      sync: syncMarker,
    };
    await BLOCK_TYPE.write(tap, emptyBlock);

    // Write second record
    await writer.append(SAMPLE_RECORDS[1]);
    await writer.flushBlock();

    // Manually write another empty block
    await BLOCK_TYPE.write(tap, emptyBlock);

    // Write remaining records
    for (let i = 2; i < SAMPLE_RECORDS.length; i++) {
      await writer.append(SAMPLE_RECORDS[i]);
    }
    await writer.close();

    const bufferData = buffer.toArrayBuffer();
    const records = await readAllRecords(bufferData);
    assertEquals(records, SAMPLE_RECORDS);
  });
});

function concatChunks(chunks: Uint8Array[]): Uint8Array {
  return concatUint8Arrays(chunks);
}
