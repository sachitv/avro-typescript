import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { createType } from "../../type/create_type.ts";
import { AvroWriter } from "../../avro_writer.ts";
import { AvroReader } from "../../avro_reader.ts";
import { concatUint8Arrays } from "../../internal/collections/array_utils.ts";
import { ReadableTap, WritableTap } from "../tap.ts";
import { SyncReadableTap, SyncWritableTap } from "../tap_sync.ts";
import type { ISyncWritable } from "../buffers/buffer_sync.ts";
import { StreamWritableBuffer } from "../streams/stream_writable_buffer.ts";
import { StreamWritableBufferAdapter } from "../streams/stream_writable_buffer_adapter.ts";
import { SyncStreamWritableBufferAdapter } from "../streams/stream_writable_buffer_adapter_sync.ts";
import type { ISyncStreamWritableBuffer } from "../streams/streams_sync.ts";

/**
 * Regression tests for scratch-buffer aliasing in the write path. Stream sinks
 * keep chunks by reference, so any memory the tap (or a caller) reuses after a
 * write resolves must never reach the sink.
 */

const POINT_SCHEMA = {
  type: "record",
  name: "test.Point",
  fields: [
    { name: "x", type: "double" },
    { name: "n", type: "int" },
  ],
} as const;

const WIDE_SCHEMA = {
  type: "record",
  name: "test.Wide",
  fields: [
    { name: "b", type: "boolean" },
    { name: "i", type: "int" },
    { name: "l", type: "long" },
    { name: "f", type: "float" },
    { name: "d", type: "double" },
    { name: "s", type: "string" },
    { name: "raw", type: "bytes" },
    { name: "fx", type: { type: "fixed", name: "test.Four", size: 4 } },
  ],
} as const;

interface WideRecord {
  b: boolean;
  i: number;
  l: bigint;
  f: number;
  d: number;
  s: string;
  raw: Uint8Array;
  fx: Uint8Array;
}

function wideRecord(seed: number): WideRecord {
  return {
    b: seed % 2 === 0,
    i: seed * 1000 - 7,
    l: BigInt(seed) * 1_000_000_000_000n - 3n,
    f: seed + 0.5,
    d: seed * 1.25,
    s: `rec-${seed}`,
    raw: new Uint8Array([seed & 0xff, 1, 2]),
    fx: new Uint8Array([seed & 0xff, 9, 8, 7]),
  };
}

/** Creates a sink that keeps every chunk by reference, optionally after a delay. */
function retainingSink(delayMs?: number): {
  stream: WritableStream<Uint8Array>;
  chunks: Uint8Array[];
} {
  const chunks: Uint8Array[] = [];
  const stream = new WritableStream<Uint8Array>({
    async write(chunk) {
      if (delayMs !== undefined) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
      chunks.push(chunk);
    },
  });
  return { stream, chunks };
}

function streamTap(stream: WritableStream<Uint8Array>): {
  tap: WritableTap;
  adapter: StreamWritableBufferAdapter;
} {
  const adapter = new StreamWritableBufferAdapter(
    new StreamWritableBuffer(stream),
  );
  return { tap: new WritableTap(adapter), adapter };
}

async function decodeAll<T>(
  type: { fromBuffer(buf: ArrayBuffer): Promise<T> },
  bytes: Uint8Array,
  recordSize: number,
): Promise<T[]> {
  const out: T[] = [];
  for (let offset = 0; offset < bytes.length; offset += recordSize) {
    const slice = bytes.slice(offset, offset + recordSize);
    out.push(await type.fromBuffer(slice.buffer));
  }
  return out;
}

async function readContainer(bytes: Uint8Array): Promise<unknown[]> {
  const reader = AvroReader.fromBlob(new Blob([bytes as BlobPart]));
  const records: unknown[] = [];
  for await (const record of reader.iterRecords()) {
    records.push(record);
  }
  await reader.close();
  return records;
}

describe("write path buffer aliasing", () => {
  describe("WritableTap over a retaining stream sink", () => {
    it("keeps earlier records intact for a single writer", async () => {
      const type = createType(POINT_SCHEMA);
      const { stream, chunks } = retainingSink();
      const { tap, adapter } = streamTap(stream);
      const values = [
        { x: 1.5, n: 1 },
        { x: 2.5, n: 2 },
        { x: 3.5, n: 3 },
      ];
      for (const value of values) {
        await type.write(tap, value);
      }
      await adapter.close();

      // Each record is 8 bytes (double) + 1 byte (small int).
      const decoded = await decodeAll(type, concatUint8Arrays(chunks), 9);
      assertEquals(decoded, values);
    });

    it("keeps two parallel writers with slow sinks independent", async () => {
      const type = createType(POINT_SCHEMA);
      const sinkA = retainingSink(1);
      const sinkB = retainingSink(1);
      const a = streamTap(sinkA.stream);
      const b = streamTap(sinkB.stream);
      const valuesA = [1, 2, 3].map((n) => ({ x: n + 0.5, n }));
      const valuesB = [4, 5, 6].map((n) => ({ x: n + 0.5, n }));

      const run = async (
        target: { tap: WritableTap; adapter: StreamWritableBufferAdapter },
        values: { x: number; n: number }[],
      ) => {
        for (const value of values) {
          await type.write(target.tap, value);
        }
        await target.adapter.close();
      };
      await Promise.all([run(a, valuesA), run(b, valuesB)]);

      assertEquals(
        await decodeAll(type, concatUint8Arrays(sinkA.chunks), 9),
        valuesA,
      );
      assertEquals(
        await decodeAll(type, concatUint8Arrays(sinkB.chunks), 9),
        valuesB,
      );
    });

    it("keeps every primitive intact across many parallel writers", async () => {
      const type = createType(WIDE_SCHEMA);
      const writerCount = 4;
      const perWriter = 25;
      const sinks = Array.from(
        { length: writerCount },
        (_, w) => retainingSink(w % 2 === 0 ? 1 : undefined),
      );
      const expected = sinks.map((_, w) =>
        Array.from({ length: perWriter }, (_, r) => wideRecord(w * 100 + r))
      );

      await Promise.all(sinks.map(async (sink, w) => {
        const { tap, adapter } = streamTap(sink.stream);
        for (const value of expected[w]!) {
          await type.write(tap, value);
        }
        await adapter.close();
      }));

      for (let w = 0; w < writerCount; w++) {
        const bytes = concatUint8Arrays(sinks[w]!.chunks);
        const decoded: unknown[] = [];
        let offset = 0;
        for (const value of expected[w]!) {
          const size = (await type.toBuffer(value)).byteLength;
          decoded.push(
            await type.fromBuffer(bytes.slice(offset, offset + size).buffer),
          );
          offset += size;
        }
        assertEquals(offset, bytes.length);
        assertEquals(decoded, expected[w]);
      }
    });

    it("is not affected by callers mutating their arrays after write", async () => {
      const type = createType(WIDE_SCHEMA);
      const { stream, chunks } = retainingSink(1);
      const { tap, adapter } = streamTap(stream);
      const value = wideRecord(7);
      const expected = new Uint8Array(await type.toBuffer(value));

      await type.write(tap, value);
      value.raw.fill(0xee);
      value.fx.fill(0xdd);
      await tap.writeFixed(value.fx);
      value.fx.fill(0xcc);
      await adapter.close();

      assertEquals(
        concatUint8Arrays(chunks),
        concatUint8Arrays([expected, new Uint8Array([0xdd, 0xdd, 0xdd, 0xdd])]),
      );
    });
  });

  describe("WritableTap keeps no shared mutable state", () => {
    it("has no static fields holding objects", () => {
      const staticFields = Object.getOwnPropertyNames(WritableTap).filter(
        (name) => !["length", "name", "prototype"].includes(name),
      );
      for (const name of staticFields) {
        const value = (WritableTap as unknown as Record<string, unknown>)[name];
        assertEquals(
          typeof value === "object" && value !== null,
          false,
          `WritableTap.${name} is shared mutable state`,
        );
      }
    });

    it("gives each instance its own scratch memory", async () => {
      const bufA = new ArrayBuffer(16);
      const bufB = new ArrayBuffer(16);
      const tapA = new WritableTap(bufA);
      const tapB = new WritableTap(bufB);
      await Promise.all([tapA.writeDouble(1.5), tapB.writeDouble(2.5)]);
      await Promise.all([tapA.writeBoolean(true), tapB.writeBoolean(false)]);
      await Promise.all([tapA.writeFloat(0.25), tapB.writeFloat(0.75)]);

      const readerA = new ReadableTap(bufA);
      const readerB = new ReadableTap(bufB);
      assertEquals(
        [
          await readerA.readDouble(),
          await readerA.readBoolean(),
          await readerA.readFloat(),
        ],
        [1.5, true, 0.25],
      );
      assertEquals(
        [
          await readerB.readDouble(),
          await readerB.readBoolean(),
          await readerB.readFloat(),
        ],
        [2.5, false, 0.75],
      );
    });
  });

  describe("StreamWritableBuffer", () => {
    it("enqueues a copy of the caller's bytes", async () => {
      const { stream, chunks } = retainingSink();
      const buffer = new StreamWritableBuffer(stream);
      const data = new Uint8Array([1, 2, 3]);
      await buffer.writeBytes(data);
      data[0] = 99;
      await buffer.close();

      assertEquals(chunks, [new Uint8Array([1, 2, 3])]);
    });
  });

  describe("AvroWriter.toStream", () => {
    const schema = {
      type: "record",
      name: "test.Row",
      fields: [
        { name: "id", type: "int" },
        { name: "score", type: "double" },
        { name: "tag", type: "string" },
      ],
    } as const;
    const makeRows = (base: number, count: number) =>
      Array.from({ length: count }, (_, i) => ({
        id: base + i,
        score: (base + i) * 0.5,
        tag: `row-${base + i}`,
      }));

    it("produces a readable container through a TransformStream", async () => {
      const { readable, writable } = new TransformStream<
        Uint8Array,
        Uint8Array
      >();
      const blobPromise = new Response(readable).blob();
      const writer = AvroWriter.toStream(writable, { schema, blockSize: 256 });
      const rows = makeRows(0, 1000);
      for (const row of rows) {
        await writer.append(row);
      }
      await writer.close();

      const bytes = new Uint8Array(await (await blobPromise).arrayBuffer());
      assertEquals(await readContainer(bytes), rows);
    });

    it("keeps parallel writers with retaining and slow sinks independent", async () => {
      const sinks = [retainingSink(), retainingSink(1), retainingSink()];
      const expected = sinks.map((_, w) => makeRows(w * 10_000, 300));

      await Promise.all(sinks.map(async (sink, w) => {
        const writer = AvroWriter.toStream(sink.stream, {
          schema,
          blockSize: 128,
        });
        for (const row of expected[w]!) {
          await writer.append(row);
        }
        await writer.close();
      }));

      for (let w = 0; w < sinks.length; w++) {
        assertEquals(
          await readContainer(concatUint8Arrays(sinks[w]!.chunks)),
          expected[w],
        );
      }
    });
  });

  describe("SyncWritableTap", () => {
    it("interleaves writes across instances with copying buffers", () => {
      const bufA = new ArrayBuffer(64);
      const bufB = new ArrayBuffer(64);
      const tapA = new SyncWritableTap(bufA);
      const tapB = new SyncWritableTap(bufB);

      tapA.writeDouble(1.5);
      tapB.writeDouble(2.5);
      tapA.writeLong(-123456789012n);
      tapB.writeLong(987654321098n);
      tapA.writeString("alpha");
      tapB.writeString("beta");

      const readerA = new SyncReadableTap(bufA);
      const readerB = new SyncReadableTap(bufB);
      assertEquals(
        [readerA.readDouble(), readerA.readLong(), readerA.readString()],
        [1.5, -123456789012n, "alpha"],
      );
      assertEquals(
        [readerB.readDouble(), readerB.readLong(), readerB.readString()],
        [2.5, 987654321098n, "beta"],
      );
    });

    it("works with sinks that copy before returning", () => {
      const type = createType(WIDE_SCHEMA);
      const chunks: Uint8Array[] = [];
      const sink: ISyncStreamWritableBuffer = {
        writeBytes(data) {
          chunks.push(data.slice());
        },
        writeBytesFrom(data, offset, length) {
          chunks.push(data.slice(offset, offset + length));
        },
        close() {},
      };
      const adapter: ISyncWritable = new SyncStreamWritableBufferAdapter(sink);
      const tap = new SyncWritableTap(adapter);
      const values = [wideRecord(1), wideRecord(2), wideRecord(3)];
      for (const value of values) {
        type.writeSync(tap, value);
      }

      const expected = concatUint8Arrays(
        values.map((value) => new Uint8Array(type.toSyncBuffer(value))),
      );
      assertEquals(concatUint8Arrays(chunks), expected);
    });
  });
});
