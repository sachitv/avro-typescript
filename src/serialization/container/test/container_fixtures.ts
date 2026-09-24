import { SyncAvroFileWriter } from "../../avro_file_writer_sync.ts";
import { SyncInMemoryWritableBuffer } from "../../buffers/in_memory_buffer_sync.ts";

/** Container files from `test-data/` that the core is checked against. */
export const FIXTURE_FILES = [
  "test-data/weather.avro",
  "test-data/weather-deflate.avro",
  "test-data/weather-zstd.avro",
  "test-data/syncInMeta.avro",
];

/** Encodes `value` as an Avro zig-zag varint long. */
export function varint(value: bigint | number): number[] {
  const n = BigInt(value);
  let raw = BigInt.asUintN(64, (n << 1n) ^ (n >> 63n));
  const out: number[] = [];
  while (raw >= 0x80n) {
    out.push(Number(raw & 0x7fn) | 0x80);
    raw >>= 7n;
  }
  out.push(Number(raw));
  return out;
}

/** Encodes `text` as an Avro string: a varint length and UTF-8 bytes. */
export function avroString(text: string): number[] {
  const bytes = new TextEncoder().encode(text);
  return [...varint(bytes.length), ...bytes];
}

/** A distinctive sync marker for hand-built containers. */
export const SYNC = Uint8Array.from({ length: 16 }, (_, i) => 0xa0 + i);

/** Builds a header with the given metadata entries in a single map block. */
export function headerBytes(
  entries: [string, string][],
  sync: Uint8Array = SYNC,
): Uint8Array {
  const body = entries.flatMap((
    [k, v],
  ) => [...avroString(k), ...avroString(v)]);
  const map = entries.length === 0
    ? [0]
    : [...varint(entries.length), ...body, 0];
  return Uint8Array.from([0x4f, 0x62, 0x6a, 0x01, ...map, ...sync]);
}

const SCHEMA = {
  type: "record",
  name: "Row",
  fields: [{ name: "id", type: "long" }, { name: "label", type: "string" }],
} as const;

/** Writes `count` rows to a container file, returning the file bytes. */
export function writeContainer(
  count: number,
  options: {
    blockSize?: number;
    metadata?: Record<string, string>;
  } = {},
): Uint8Array {
  const buffer = new SyncInMemoryWritableBuffer(new ArrayBuffer(64 * 1024));
  const file = new SyncAvroFileWriter(buffer, {
    schema: SCHEMA,
    syncMarker: SYNC,
    ...options,
  });
  for (let i = 0; i < count; i++) {
    file.append({ id: BigInt(i), label: `row-${i}` });
  }
  file.close();
  return new Uint8Array(buffer.getBufferCopy());
}
