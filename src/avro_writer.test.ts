import { assert, assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { AvroWriter } from "./avro_writer.ts";
import { AvroReader } from "./avro_reader.ts";
import type { IWritableBuffer } from "./internal/serialization/buffers/buffer.ts";
import { InMemoryReadableBuffer } from "./internal/serialization/buffers/in_memory_buffer.ts";

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
});

function concatChunks(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const combined = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.length;
  }
  return combined;
}
