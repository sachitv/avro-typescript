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
import {
  CompiledWriterStrategy,
  InterpretedWriterStrategy,
} from "../../src/schemas/complex/record_writer_strategy.ts";

/**
 * Comprehensive benchmark comparing avro-typescript vs avsc vs avro-js
 * across all Avro types with various settings.
 *
 * Data format differences:
 * - bytes: avro-typescript uses Uint8Array, avsc/avro-js use Buffer
 * - maps: avro-typescript uses Map, avsc/avro-js use plain objects
 * - unions: avro-typescript uses wrapped {type: value}, avsc/avro-js use unwrapped values
 * - long: avro-typescript uses bigint, avsc/avro-js use number (with precision loss)
 */

// =============================================================================
// SCHEMA DEFINITIONS
// =============================================================================

// --- 1. Primitive Type Schemas ---
const primitiveSchemas = {
  null: "null" as const,
  boolean: "boolean" as const,
  int: "int" as const,
  long: "long" as const,
  float: "float" as const,
  double: "double" as const,
  bytes: "bytes" as const,
  string: "string" as const,
};

// --- 2. Complex Type Schemas ---
const enumSchema: SchemaLike = {
  type: "enum",
  name: "Status",
  symbols: ["PENDING", "ACTIVE", "COMPLETED", "FAILED"],
};

const fixedSchema: SchemaLike = {
  type: "fixed",
  name: "UUID",
  size: 16,
};

const arrayOfIntsSchema: SchemaLike = {
  type: "array",
  items: "int",
};

const arrayOfStringsSchema: SchemaLike = {
  type: "array",
  items: "string",
};

const mapOfIntsSchema: SchemaLike = {
  type: "map",
  values: "int",
};

const mapOfStringsSchema: SchemaLike = {
  type: "map",
  values: "string",
};

// --- 3. Record Schemas (no maps/unions for cross-library comparison) ---
const simpleRecordSchema: SchemaLike = {
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

const deeplyNestedRecordSchema: SchemaLike = {
  type: "record",
  name: "Level1Record",
  fields: [
    { name: "id", type: "int" },
    { name: "name", type: "string" },
    {
      name: "level2",
      type: {
        type: "record",
        name: "Level2Record",
        fields: [
          { name: "id", type: "int" },
          { name: "description", type: "string" },
          {
            name: "level3",
            type: {
              type: "record",
              name: "Level3Record",
              fields: [
                { name: "id", type: "int" },
                { name: "value", type: "double" },
                {
                  name: "level4",
                  type: {
                    type: "record",
                    name: "Level4Record",
                    fields: [
                      { name: "id", type: "int" },
                      { name: "data", type: "bytes" },
                      { name: "tags", type: { type: "array", items: "string" } },
                    ],
                  },
                },
              ],
            },
          },
        ],
      },
    },
  ],
};

const recordWithArrayOfRecordsSchema: SchemaLike = {
  type: "record",
  name: "RecordWithArrayOfRecords",
  fields: [
    { name: "id", type: "int" },
    { name: "title", type: "string" },
    {
      name: "items",
      type: {
        type: "array",
        items: {
          type: "record",
          name: "ArrayInnerRecord",
          fields: [
            { name: "id", type: "int" },
            { name: "name", type: "string" },
            { name: "value", type: "double" },
          ],
        },
      },
    },
  ],
};

// =============================================================================
// TYPE CREATION
// =============================================================================

const compiledStrategy = new CompiledWriterStrategy();
const interpretedStrategy = new InterpretedWriterStrategy();

// deno-lint-ignore no-explicit-any
type AvscType = ReturnType<typeof avsc.Type.forSchema>;
// deno-lint-ignore no-explicit-any
type AvroJsType = ReturnType<typeof avrojs.parse>;

interface LibraryTypes {
  avroTs: ReturnType<typeof createType>;
  avroTsCompiled: ReturnType<typeof createType>;
  avroTsInterpreted: ReturnType<typeof createType>;
  avroTsUnchecked: ReturnType<typeof createType>;
  avroTsCompiledUnchecked: ReturnType<typeof createType>;
  avroTsInterpretedUnchecked: ReturnType<typeof createType>;
  avsc: AvscType;
  avroJs: AvroJsType;
}

function createLibraryTypes(schema: SchemaLike): LibraryTypes {
  // avro-js requires object schema format, convert string primitives
  const avroJsSchema =
    typeof schema === "string" ? { type: schema } : schema;

  return {
    avroTs: createType(schema),
    avroTsCompiled: createType(schema, { writerStrategy: compiledStrategy }),
    avroTsInterpreted: createType(schema, {
      writerStrategy: interpretedStrategy,
    }),
    avroTsUnchecked: createType(schema, { validate: false }),
    avroTsCompiledUnchecked: createType(schema, {
      writerStrategy: compiledStrategy,
      validate: false,
    }),
    avroTsInterpretedUnchecked: createType(schema, {
      writerStrategy: interpretedStrategy,
      validate: false,
    }),
    avsc: avsc.Type.forSchema(
      schema as Parameters<typeof avsc.Type.forSchema>[0],
    ),
    avroJs: avrojs.parse(avroJsSchema as Parameters<typeof avrojs.parse>[0]),
  };
}

// =============================================================================
// DATA CONVERSION HELPERS
// =============================================================================

/**
 * Convert avro-typescript data to avsc/avro-js compatible format
 * - Uint8Array -> Buffer
 * - bigint -> number (with precision loss warning for large values)
 * - Map -> plain object
 * - wrapped union -> unwrapped value
 */
// deno-lint-ignore no-explicit-any
function toNodeFormat(data: any): any {
  if (data === null || data === undefined) return data;
  if (data instanceof Uint8Array) return Buffer.from(data);
  if (typeof data === "bigint") return Number(data);
  if (data instanceof Map) {
    const obj: Record<string, unknown> = {};
    for (const [k, v] of data.entries()) {
      obj[k] = toNodeFormat(v);
    }
    return obj;
  }
  if (Array.isArray(data)) return data.map(toNodeFormat);
  if (typeof data === "object") {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(data)) {
      result[k] = toNodeFormat(v);
    }
    return result;
  }
  return data;
}

// =============================================================================
// BENCHMARK RUNNER
// =============================================================================

interface BenchmarkConfig {
  groupName: string;
  types: LibraryTypes;
  avroTsData: unknown;
  nodeData: unknown;
  bufferSize?: number;
}

function runFullComparisonBenchmark(config: BenchmarkConfig) {
  const { groupName, types, avroTsData, nodeData, bufferSize = 1024 } = config;

  // --- avsc ---
  Deno.bench({
    name: `${groupName} (avsc)`,
    group: groupName,
    baseline: true,
    n: 100000,
  }, () => {
    types.avsc.toBuffer(nodeData);
  });

  // --- avro-js ---
  Deno.bench({
    name: `${groupName} (avro-js)`,
    group: groupName,
    n: 100000,
  }, () => {
    types.avroJs.toBuffer(nodeData);
  });

  // --- avro-typescript: toSyncBuffer variants ---
  Deno.bench({
    name: `${groupName} (avro-ts, validate=true)`,
    group: groupName,
    n: 100000,
  }, () => {
    types.avroTs.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: `${groupName} (avro-ts, validate=false)`,
    group: groupName,
    n: 100000,
  }, () => {
    types.avroTsUnchecked.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: `${groupName} (avro-ts, compiled, validate=true)`,
    group: groupName,
    n: 100000,
  }, () => {
    types.avroTsCompiled.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: `${groupName} (avro-ts, compiled, validate=false)`,
    group: groupName,
    n: 100000,
  }, () => {
    types.avroTsCompiledUnchecked.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: `${groupName} (avro-ts, interpreted, validate=true)`,
    group: groupName,
    n: 100000,
  }, () => {
    types.avroTsInterpreted.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: `${groupName} (avro-ts, interpreted, validate=false)`,
    group: groupName,
    n: 100000,
  }, () => {
    types.avroTsInterpretedUnchecked.toSyncBuffer(avroTsData);
  });

  // --- avro-typescript: writeSync (pre-allocated buffer) variants ---
  {
    const buffer = new ArrayBuffer(bufferSize);
    Deno.bench({
      name: `${groupName} (avro-ts, writeSync, validate=true)`,
      group: groupName,
      n: 100000,
    }, () => {
      const writable = new SyncInMemoryWritableBuffer(buffer);
      const tap = new SyncWritableTap(writable);
      types.avroTs.writeSync(tap, avroTsData);
    });
  }

  {
    const buffer = new ArrayBuffer(bufferSize);
    Deno.bench({
      name: `${groupName} (avro-ts, writeSync, validate=false)`,
      group: groupName,
      n: 100000,
    }, () => {
      const writable = new SyncInMemoryWritableBuffer(buffer);
      const tap = new SyncWritableTap(writable);
      types.avroTsUnchecked.writeSync(tap, avroTsData);
    });
  }
}

// =============================================================================
// 1. PRIMITIVE TYPE BENCHMARKS
// =============================================================================

// --- Null ---
{
  const types = createLibraryTypes(primitiveSchemas.null);
  const data = types.avroTs.random();
  runFullComparisonBenchmark({
    groupName: "primitive: null",
    types,
    avroTsData: data,
    nodeData: data,
  });
}

// --- Boolean ---
{
  const types = createLibraryTypes(primitiveSchemas.boolean);
  const data = types.avroTs.random();
  runFullComparisonBenchmark({
    groupName: "primitive: boolean",
    types,
    avroTsData: data,
    nodeData: data,
  });
}

// --- Int ---
{
  const types = createLibraryTypes(primitiveSchemas.int);
  const data = types.avroTs.random();
  runFullComparisonBenchmark({
    groupName: "primitive: int",
    types,
    avroTsData: data,
    nodeData: data,
  });
}

// --- Long ---
{
  const types = createLibraryTypes(primitiveSchemas.long);
  const data = types.avroTs.random() as bigint;
  runFullComparisonBenchmark({
    groupName: "primitive: long",
    types,
    avroTsData: data,
    nodeData: Number(data), // avsc/avro-js use number
  });
}

// --- Float ---
{
  const types = createLibraryTypes(primitiveSchemas.float);
  const data = types.avroTs.random();
  runFullComparisonBenchmark({
    groupName: "primitive: float",
    types,
    avroTsData: data,
    nodeData: data,
  });
}

// --- Double ---
{
  const types = createLibraryTypes(primitiveSchemas.double);
  const data = types.avroTs.random();
  runFullComparisonBenchmark({
    groupName: "primitive: double",
    types,
    avroTsData: data,
    nodeData: data,
  });
}

// --- Bytes ---
{
  const types = createLibraryTypes(primitiveSchemas.bytes);
  const data = types.avroTs.random() as Uint8Array;
  runFullComparisonBenchmark({
    groupName: "primitive: bytes",
    types,
    avroTsData: data,
    nodeData: Buffer.from(data),
  });
}

// --- String ---
{
  const types = createLibraryTypes(primitiveSchemas.string);
  const data = types.avroTs.random();
  runFullComparisonBenchmark({
    groupName: "primitive: string",
    types,
    avroTsData: data,
    nodeData: data,
  });
}

// =============================================================================
// 2. COMPLEX TYPE BENCHMARKS
// =============================================================================

// --- Enum ---
{
  const types = createLibraryTypes(enumSchema);
  const data = types.avroTs.random();
  runFullComparisonBenchmark({
    groupName: "complex: enum",
    types,
    avroTsData: data,
    nodeData: data,
  });
}

// --- Fixed ---
{
  const types = createLibraryTypes(fixedSchema);
  const data = types.avroTs.random() as Uint8Array;
  runFullComparisonBenchmark({
    groupName: "complex: fixed",
    types,
    avroTsData: data,
    nodeData: Buffer.from(data),
  });
}

// --- Array<int> ---
{
  const types = createLibraryTypes(arrayOfIntsSchema);
  const data = types.avroTs.random();
  runFullComparisonBenchmark({
    groupName: "complex: array<int>",
    types,
    avroTsData: data,
    nodeData: data,
  });
}

// --- Array<string> ---
{
  const types = createLibraryTypes(arrayOfStringsSchema);
  const data = types.avroTs.random();
  runFullComparisonBenchmark({
    groupName: "complex: array<string>",
    types,
    avroTsData: data,
    nodeData: data,
  });
}

// --- Map<int> ---
// Note: avro-typescript uses Map, avsc/avro-js use plain objects
{
  const types = createLibraryTypes(mapOfIntsSchema);
  const avroTsData = types.avroTs.random() as Map<string, number>;
  const nodeData = Object.fromEntries(avroTsData.entries());

  Deno.bench({
    name: "complex: map<int> (avsc)",
    group: "complex: map<int>",
    baseline: true,
    n: 100000,
  }, () => {
    types.avsc.toBuffer(nodeData);
  });

  Deno.bench({
    name: "complex: map<int> (avro-js)",
    group: "complex: map<int>",
    n: 100000,
  }, () => {
    types.avroJs.toBuffer(nodeData);
  });

  Deno.bench({
    name: "complex: map<int> (avro-ts, validate=true)",
    group: "complex: map<int>",
    n: 100000,
  }, () => {
    types.avroTs.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: "complex: map<int> (avro-ts, validate=false)",
    group: "complex: map<int>",
    n: 100000,
  }, () => {
    types.avroTsUnchecked.toSyncBuffer(avroTsData);
  });
}

// --- Map<string> ---
{
  const types = createLibraryTypes(mapOfStringsSchema);
  const avroTsData = types.avroTs.random() as Map<string, string>;
  const nodeData = Object.fromEntries(avroTsData.entries());

  Deno.bench({
    name: "complex: map<string> (avsc)",
    group: "complex: map<string>",
    baseline: true,
    n: 100000,
  }, () => {
    types.avsc.toBuffer(nodeData);
  });

  Deno.bench({
    name: "complex: map<string> (avro-js)",
    group: "complex: map<string>",
    n: 100000,
  }, () => {
    types.avroJs.toBuffer(nodeData);
  });

  Deno.bench({
    name: "complex: map<string> (avro-ts, validate=true)",
    group: "complex: map<string>",
    n: 100000,
  }, () => {
    types.avroTs.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: "complex: map<string> (avro-ts, validate=false)",
    group: "complex: map<string>",
    n: 100000,
  }, () => {
    types.avroTsUnchecked.toSyncBuffer(avroTsData);
  });
}

// =============================================================================
// 3. RECORD BENCHMARKS
// =============================================================================

// --- Simple Record ---
{
  const types = createLibraryTypes(simpleRecordSchema);
  const avroTsData = types.avroTs.random() as {
    id: number;
    name: string;
    value: number;
    active: boolean;
    data: Uint8Array;
  };
  const nodeData = toNodeFormat(avroTsData);

  runFullComparisonBenchmark({
    groupName: "record: simple",
    types,
    avroTsData,
    nodeData,
    bufferSize: 1024,
  });
}

// --- Deeply Nested Record (4 levels) ---
{
  const types = createLibraryTypes(deeplyNestedRecordSchema);
  const avroTsData = types.avroTs.random();
  const nodeData = toNodeFormat(avroTsData);

  runFullComparisonBenchmark({
    groupName: "record: nested (4 levels)",
    types,
    avroTsData,
    nodeData,
    bufferSize: 2048,
  });
}

// --- Record with Array of Records ---
{
  const types = createLibraryTypes(recordWithArrayOfRecordsSchema);
  const avroTsData = types.avroTs.random();
  const nodeData = toNodeFormat(avroTsData);

  runFullComparisonBenchmark({
    groupName: "record: array of records",
    types,
    avroTsData,
    nodeData,
    bufferSize: 2048,
  });
}

// =============================================================================
// ADDITIONAL: TAP-LEVEL STRING WRITING
// =============================================================================

{
  const stringData = createType(primitiveSchemas.string).random() as string;
  Deno.bench({
    name: "tap: string writing (avro-ts)",
    group: "tap operations",
    n: 100000,
  }, () => {
    const buffer = new ArrayBuffer(1024);
    const writable = new SyncInMemoryWritableBuffer(buffer);
    const tap = new SyncWritableTap(writable);
    tap.writeString(stringData);
  });
}
