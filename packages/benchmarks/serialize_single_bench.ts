#!/usr/bin/env -S deno run --allow-read --allow-write

import { Buffer } from "node:buffer";
import type { SchemaLike } from "../../src/type/create_type.ts";
import {
  createSerializationTargets,
  type SerializationTarget,
} from "./library_targets.ts";
import {
  SyncInMemoryWritableBuffer,
} from "../../src/serialization/buffers/sync_in_memory_buffer.ts";
import {
  SyncWritableTap,
} from "../../src/serialization/sync_tap.ts";

/**
 * Focused benchmark script for serialize single record performance
 */

const schema: SchemaLike = {
  type: "record",
  name: "TestRecord",
  fields: [
    { name: "id", type: "int" },
    { name: "name", type: "string" },
    { name: "value", type: "double" },
    { name: "active", type: "boolean" },
    { name: "data", type: "bytes" },
  ],
};

const testData = {
  id: 12345,
  name: "benchmark test record",
  value: 3.14159,
  active: true,
  data: new Uint8Array([1, 2, 3, 4, 5]),
};

type TestRecord = typeof testData;

const serializationTargets: SerializationTarget<TestRecord>[] =
  createSerializationTargets<TestRecord>(schema, {
    avsc: { prepareInput: toNodeFriendlyRecord },
    "avro-js": { prepareInput: toNodeFriendlyRecord },
  });

// Benchmark string writing specifically
Deno.bench({
  name: "string writing benchmark",
  n: 100000,
}, () => {
  const buffer = new ArrayBuffer(1024);
  const writable = new SyncInMemoryWritableBuffer(buffer);
  const tap = new SyncWritableTap(writable);
  tap.writeString(testData.name);
});

// Run only serialize single record benchmarks
for (const target of serializationTargets) {
  const record = target.prepareInput(testData);
  Deno.bench({
    name: `serialize single record (${target.label})`,
    n: 100000, // More iterations for stable results
  }, () => {
    target.serialize(record);
  });
}

function toNodeFriendlyRecord(record: TestRecord): TestRecord {
  return {
    ...record,
    data: Buffer.from(record.data),
  };
}