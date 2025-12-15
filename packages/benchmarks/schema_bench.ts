import { createType } from "../../src/mod.ts";
import type { SchemaLike } from "../../src/type/create_type.ts";
import avsc from "npm:avsc";
import avrojs from "npm:avro-js";
import {
  createSerializationTargets,
  type BenchmarkLibrary,
  type SerializationTarget,
} from "./library_targets.ts";
type AvscSchema = Parameters<typeof avsc.Type.forSchema>[0];

/**
 * Benchmark schema creation and type operations
 */

const simpleSchema: SchemaLike = {
  type: "record",
  name: "SimpleRecord",
  fields: [
    { name: "id", type: "int" },
    { name: "name", type: "string" },
  ],
};

const complexSchema: SchemaLike = {
  type: "record",
  name: "ComplexRecord",
  fields: [
    { name: "id", type: "long" },
    { name: "name", type: "string" },
    { name: "email", type: "string" },
    { name: "age", type: "int" },
    { name: "tags", type: { type: "array", items: "string" } },
  ],
};
interface SchemaFactory {
  id: BenchmarkLibrary;
  label: string;
  create(schema: SchemaLike): unknown;
}

const schemaFactories: SchemaFactory[] = [
  {
    id: "avro-typescript",
    label: "avro-typescript",
    create: (schema) => createType(schema),
  },
  {
    id: "avsc",
    label: "avsc",
    create: (schema) => avsc.Type.forSchema(schema as AvscSchema),
  },
  {
    id: "avro-js",
    label: "avro-js",
    create: (schema) => avrojs.parse(schema as Parameters<typeof avrojs.parse>[0]),
  },
];

for (const factory of schemaFactories) {
  Deno.bench(`create simple type (${factory.label})`, () => {
    factory.create(simpleSchema);
  });

  Deno.bench(`create complex type (${factory.label})`, () => {
    factory.create(complexSchema);
  });
}

type ComplexRecord = {
  id: number | bigint;
  name: string;
  email: string;
  age: number;
  tags: string[];
};

const complexRecordTargets: SerializationTarget<ComplexRecord>[] =
  createSerializationTargets<ComplexRecord>(complexSchema, {
    avsc: { prepareInput: toNodeFriendlyComplexRecord },
    "avro-js": { prepareInput: toNodeFriendlyComplexRecord },
  });

const complexRecord: ComplexRecord = {
  id: 123456789n,
  name: "John Doe",
  email: "john@example.com",
  age: 30,
  tags: ["developer", "typescript", "avro"],
};

for (const target of complexRecordTargets) {
  Deno.bench(`type serialization/deserialization (${target.label})`, () => {
    const record = target.prepareInput(complexRecord);
    const serialized = target.serialize(record);
    const deserialized = target.deserialize(serialized);
    const decodedId = (deserialized as { id?: bigint | number }).id;
    if (Number(decodedId ?? -1) !== Number(complexRecord.id)) {
      throw new Error(`Type round-trip failed for ${target.label}`);
    }
  });
}

function toNodeFriendlyComplexRecord(record: ComplexRecord): ComplexRecord {
  if (typeof record.id === "number") {
    return record;
  }
  return {
    ...record,
    id: Number(record.id),
  };
}
