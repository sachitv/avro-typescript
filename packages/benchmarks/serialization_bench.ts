import { Buffer } from "node:buffer";
import type { SchemaLike } from "../../src/type/create_type.ts";
import {
  createSerializationTargets,
  type SerializationTarget,
} from "./library_targets.ts";

/**
 * Benchmark serialization performance
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

for (const target of serializationTargets) {
  Deno.bench(`serialize single record (${target.label})`, () => {
    const record = target.prepareInput(testData);
    target.serialize(record);
  });

  Deno.bench(`deserialize single record (${target.label})`, () => {
    const record = target.prepareInput(testData);
    const serialized = target.serialize(record);
    target.deserialize(serialized);
  });

  Deno.bench(`round-trip serialization (${target.label})`, () => {
    const record = target.prepareInput(testData);
    const serialized = target.serialize(record);
    const result = target.deserialize(serialized);
    if ((result as { id?: number }).id !== testData.id) {
      throw new Error(`Round-trip failed for ${target.label}`);
    }
  });
}

function toNodeFriendlyRecord(record: TestRecord): TestRecord {
  return {
    ...record,
    data: Buffer.from(record.data),
  };
}
