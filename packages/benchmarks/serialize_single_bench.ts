#!/usr/bin/env -S deno run --allow-read --allow-write

import { Buffer } from "node:buffer";
import avsc from "npm:avsc";
import avrojs from "npm:avro-js";
import type { SchemaLike } from "../../src/type/create_type.ts";
import { createType } from "../../src/mod.ts";
import {
  SyncInMemoryWritableBuffer,
} from "../../src/serialization/buffers/sync_in_memory_buffer.ts";
import { SyncWritableTap } from "../../src/serialization/sync_tap.ts";

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
const avroTypeStrict = createType(schema);

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

// Avro TypeScript: validated write into a preallocated buffer (writer hot path).
{
  const record = testData;
  const buffer = new ArrayBuffer(1024);
  Deno.bench({
    name: "serialize single record (avro-typescript, writeSync, validate=true)",
    n: 100000,
  }, () => {
    const writable = new SyncInMemoryWritableBuffer(buffer);
    const tap = new SyncWritableTap(writable);
    avroTypeStrict.writeSync(tap, record);
  });
}

// Avro TypeScript: validation-free write into a preallocated buffer (writer hot path).
{
  const avroTypeUnchecked = createType(schema, { validate: false });
  const record = testData;
  const buffer = new ArrayBuffer(1024);
  Deno.bench({
    name: "serialize single record (avro-typescript, writeSync, validate=false)",
    n: 100000,
  }, () => {
    const writable = new SyncInMemoryWritableBuffer(buffer);
    const tap = new SyncWritableTap(writable);
    avroTypeUnchecked.writeSync(tap, record);
  });
}

// Avro TypeScript: toSyncBuffer (includes allocation), validated vs unchecked.
// This is a closer apples-to-apples comparison to `avsc` / `avro-js` `toBuffer()`.
{
  const record = testData;
  Deno.bench({
    name: "serialize single record (avro-typescript, toSyncBuffer, validate=true)",
    n: 100000,
  }, () => {
    avroTypeStrict.toSyncBuffer(record);
  });
}

{
  const avroTypeUnchecked = createType(schema, { validate: false });
  const record = testData;
  Deno.bench({
    name: "serialize single record (avro-typescript, toSyncBuffer, validate=false)",
    n: 100000,
  }, () => {
    avroTypeUnchecked.toSyncBuffer(record);
  });
}

// avsc + avro-js: provide "validate=false" baselines by skipping an explicit
// `isValid` pre-check. These libraries still perform their own internal checks
// during serialization; this benchmark isolates the overhead of an additional,
// explicit validation pass.
{
  const record = toNodeFriendlyRecord(testData);
  const avscType = avsc.Type.forSchema(
    schema as Parameters<
      typeof avsc.Type.forSchema
    >[0],
  );
  const avroJsType = avrojs.parse(schema as Parameters<typeof avrojs.parse>[0]);

  Deno.bench({
    name: "serialize single record (avsc, validate=false)",
    n: 100000,
  }, () => {
    avscType.toBuffer(record);
  });

  Deno.bench({
    name: "serialize single record (avsc, validate=true)",
    n: 100000,
  }, () => {
    if (!avscType.isValid(record)) {
      throw new Error("Benchmark record unexpectedly invalid for avsc.");
    }
    avscType.toBuffer(record);
  });

  Deno.bench({
    name: "serialize single record (avro-js, validate=false)",
    n: 100000,
  }, () => {
    avroJsType.toBuffer(record);
  });

  Deno.bench({
    name: "serialize single record (avro-js, validate=true)",
    n: 100000,
  }, () => {
    if (!avroJsType.isValid(record)) {
      throw new Error("Benchmark record unexpectedly invalid for avro-js.");
    }
    avroJsType.toBuffer(record);
  });
}

function toNodeFriendlyRecord(record: TestRecord): TestRecord {
  return {
    ...record,
    data: Buffer.from(record.data),
  };
}
