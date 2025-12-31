import { createType } from "../../src/mod.ts";
import type { SchemaLike } from "../../src/type/create_type.ts";
import avsc from "npm:avsc";
import avrojs from "npm:avro-js";
import { Buffer } from "node:buffer";
type AvscSchema = Parameters<typeof avsc.Type.forSchema>[0];
type RecordPayload = Uint8Array;

const SCHEMA: SchemaLike = {
  type: "record",
  name: "PerfRecord",
  fields: [
    { name: "id", type: "int" },
    { name: "label", type: "string" },
    { name: "value", type: "double" },
    { name: "active", type: "boolean" },
    { name: "payload", type: "bytes" },
  ],
};

const RECORD_COUNT = 1000;

type PerfRecord = {
  id: number;
  label: string;
  value: number;
  active: boolean;
  payload: Uint8Array;
};

const BASE_RECORD: Omit<PerfRecord, "id" | "label"> = {
  value: 3.14159,
  active: true,
  payload: new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]),
};

const avroType = createType(SCHEMA);
const avscType = avsc.Type.forSchema(SCHEMA as AvscSchema);
type AvroJsType = ReturnType<typeof avrojs.parse>;
const avroJsType: AvroJsType = avrojs.parse(
  SCHEMA as Parameters<typeof avrojs.parse>[0],
);

console.log("Preparing to profile serialization for 1,000 records per library...");

benchmark("avro-typescript", identity, (record) =>
  new Uint8Array(avroType.toSyncBuffer(record)),
);
benchmark("avsc", toNodeRecord, (record) =>
  normalizeToUint8Array(avscType.toBuffer(record)),
);
benchmark("avro-js", toNodeRecord, (record) =>
  normalizeToUint8Array(avroJsType.toBuffer(record)),
);

console.log("Benchmarks complete; you can now inspect the profiles under the DevTools Perf tab.");

function benchmark(
  label: string,
  prepare: (record: PerfRecord) => PerfRecord,
  serialize: (record: PerfRecord) => Uint8Array,
): void {
  console.log(`Starting ${label} (JavaScript console.profile will mark this in DevTools)`);
  console.profile(label);
  for (let index = 0; index < RECORD_COUNT; index++) {
    serialize(prepare(makeRecord(index)));
  }
  console.profileEnd(label);
  console.log(`Completed ${label}`);
}

function makeRecord(index: number): PerfRecord {
  return {
    id: index,
    label: `record-${index}`,
    value: BASE_RECORD.value,
    active: index % 2 === 0,
    payload: clonePayload(),
  };
}

function identity<T>(value: T): T {
  return value;
}

function toNodeRecord(record: PerfRecord): PerfRecord {
  return {
    ...record,
    payload: Buffer.from(record.payload),
  };
}

function clonePayload(): RecordPayload {
  const copy = new Uint8Array(BASE_RECORD.payload.length);
  copy.set(BASE_RECORD.payload);
  return copy;
}

function normalizeToUint8Array(buffer: Uint8Array | Buffer): Uint8Array {
  if (Buffer.isBuffer(buffer)) {
    return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  }
  return buffer;
}
