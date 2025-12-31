import { createType, type Type } from "../../src/mod.ts";
import type { SchemaLike } from "../../src/type/create_type.ts";
import avsc from "npm:avsc";
import avrojs from "npm:avro-js";
import { Buffer } from "node:buffer";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../src/serialization/tap_sync.ts";
import {
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "../../src/serialization/buffers/in_memory_buffer_sync.ts";

/** Identifies which Avro library implementation to benchmark. */
export type BenchmarkLibrary = "avro-typescript" | "avsc" | "avro-js";
type AvscSchema = Parameters<typeof avsc.Type.forSchema>[0];

/**
 * Abstraction for benchmarking serialization/deserialization across libraries.
 */
export interface SerializationTarget<TRecord> {
  /** Unique identifier for the library being benchmarked. */
  id: BenchmarkLibrary;
  /** Human-readable label for benchmark output. */
  label: string;
  /** Transforms input record before serialization (e.g., for library-specific formats). */
  prepareInput(record: TRecord): TRecord;
  /** Serializes a record to Avro binary format. */
  serialize(record: TRecord): Uint8Array;
  /** Deserializes Avro binary data back to a record. */
  deserialize(payload: Uint8Array): unknown;
}

/**
 * Per-library overrides for benchmark configuration.
 */
export type SerializationOverrides<TRecord> = {
  prepareInput?: (record: TRecord) => TRecord;
};

/**
 * Creates serialization targets for all supported Avro libraries.
 * @param schema The Avro schema to use for serialization.
 * @param overrides Optional per-library configuration overrides.
 * @returns Array of serialization targets for benchmarking.
 */
export function createSerializationTargets<TRecord>(
  schema: SchemaLike,
  overrides?: Partial<Record<BenchmarkLibrary, SerializationOverrides<TRecord>>>,
): SerializationTarget<TRecord>[] {
  const avroType = createType(schema);
  const avscSchema = schema as AvscSchema;
  const avscType = avsc.Type.forSchema(avscSchema);
  const avroJsType = avrojs.parse(schema as Parameters<typeof avrojs.parse>[0]);

  return [
    {
      id: "avro-typescript",
      label: "avro-typescript",
      prepareInput: overrides?.["avro-typescript"]?.prepareInput ??
        ((record: TRecord) => record),
      serialize: (record) => serializeViaTap(avroType, record),
      deserialize: (payload) => deserializeViaTap(avroType, payload),
    },
    {
      id: "avsc",
      label: "avsc",
      prepareInput: overrides?.avsc?.prepareInput ?? ((record: TRecord) => record),
      serialize: (record) => normalizeToUint8Array(avscType.toBuffer(record)),
      deserialize: (payload) => avscType.fromBuffer(Buffer.from(payload)),
    },
    {
      id: "avro-js",
      label: "avro-js",
      prepareInput: overrides?.["avro-js"]?.prepareInput ??
        ((record: TRecord) => record),
      serialize: (record) => normalizeToUint8Array(avroJsType.toBuffer(record)),
      deserialize: (payload) => avroJsType.fromBuffer(Buffer.from(payload)),
    },
  ];
}

function normalizeToUint8Array(buffer: Uint8Array | Buffer): Uint8Array {
  if (Buffer.isBuffer(buffer)) {
    return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  }
  return buffer;
}

function toArrayBuffer(payload: Uint8Array): ArrayBuffer {
  const { buffer, byteOffset, byteLength } = payload;
  if (buffer instanceof ArrayBuffer) {
    return buffer.slice(byteOffset, byteOffset + byteLength);
  }
  const copy = new Uint8Array(byteLength);
  copy.set(payload);
  return copy.buffer;
}

// We don't need too large buffers for benchmarking.
const kBuffer = new ArrayBuffer(1024);
function serializeViaTap<T>(type: Type<T>, value: T): Uint8Array {
  const writable = new SyncInMemoryWritableBuffer(kBuffer);
  const tap = new SyncWritableTap(writable);
  type.writeSync(tap, value);
  const length = tap.getPos();
  const buffer = writable._testOnlyBuffer();
  return new Uint8Array(buffer, 0, length);
}

function deserializeViaTap<T>(type: Type<T>, payload: Uint8Array): T {
  const readable = new SyncInMemoryReadableBuffer(payload.buffer as ArrayBuffer);
  const tap = new SyncReadableTap(readable);
  return type.readSync(tap);
}
