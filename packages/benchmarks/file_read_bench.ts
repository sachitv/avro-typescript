/**
 * Compares async and sync container-file reads over the same in-memory bytes,
 * isolating decode cost from file-system I/O.
 *
 * Run with: deno bench --no-config --allow-read file_read_bench.ts
 */
import {
  AvroReader,
  BlobReadableBuffer,
  InMemoryReadableBuffer,
  SyncAvroReader,
  SyncAvroWriter,
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "../../src/mod.ts";

const BENCH_SCHEMA = {
  type: "record",
  name: "FileReadBenchRecord",
  fields: [
    { name: "id", type: "long" },
    { name: "label", type: "string" },
    { name: "value", type: "double" },
    { name: "active", type: "boolean" },
    { name: "tags", type: { type: "array", items: "string" } },
  ],
} as const;

const RECORD_COUNTS = [10_000, 100_000];

/** Writes `count` benchmark records to an in-memory Avro container file. */
function writeFile(count: number): Uint8Array<ArrayBuffer> {
  // The in-memory writable buffer has a fixed capacity and does not grow.
  // Each record encodes to ~38 bytes on average (long id, "record-N" label,
  // double, boolean, two-string array), so 128 bytes per record leaves ample
  // headroom for the file header and block framing.
  const buffer = new SyncInMemoryWritableBuffer(new ArrayBuffer(count * 128));
  const writer = SyncAvroWriter.toBuffer(buffer, { schema: BENCH_SCHEMA });
  for (let i = 0; i < count; i++) {
    writer.append({
      id: BigInt(i),
      label: `record-${i}`,
      value: i * Math.PI,
      active: i % 2 === 0,
      tags: ["alpha", "beta"],
    });
  }
  writer.close();
  return new Uint8Array(buffer.getBufferCopy());
}

for (const count of RECORD_COUNTS) {
  const bytes = writeFile(count);
  // Built once outside the timed functions: `new Blob()` copies its input.
  const blob = new Blob([bytes]);
  const group = `file read (${count} records)`;

  Deno.bench(`sync reader (${count})`, { group, baseline: true }, () => {
    const reader = SyncAvroReader.fromBuffer(
      new SyncInMemoryReadableBuffer(bytes.buffer as ArrayBuffer),
    );
    let n = 0;
    for (const _ of reader.iterRecords()) n++;
    if (n !== count) throw new Error(`expected ${count}, got ${n}`);
  });

  Deno.bench(`async reader, in-memory (${count})`, { group }, async () => {
    const reader = AvroReader.fromBuffer(
      new InMemoryReadableBuffer(bytes.buffer as ArrayBuffer),
    );
    let n = 0;
    for await (const _ of reader.iterRecords()) n++;
    if (n !== count) throw new Error(`expected ${count}, got ${n}`);
  });

  Deno.bench(`async reader, blob (${count})`, { group }, async () => {
    const reader = AvroReader.fromBuffer(
      new BlobReadableBuffer(blob),
    );
    let n = 0;
    for await (const _ of reader.iterRecords()) n++;
    if (n !== count) throw new Error(`expected ${count}, got ${n}`);
  });
}
