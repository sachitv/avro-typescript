#!/usr/bin/env -S deno run --allow-read --allow-write

import { Buffer } from "node:buffer";
import avsc from "npm:avsc";
import avrojs from "npm:avro-js";
import type { SchemaLike } from "../../src/type/create_type.ts";
import { createType } from "../../src/mod.ts";
import {
  SyncInMemoryWritableBuffer,
} from "../../src/serialization/buffers/in_memory_buffer_sync.ts";
import { SyncWritableTap } from "../../src/serialization/tap_sync.ts";
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
// BENCHMARK CONFIGURATION
// =============================================================================

/**
 * Number of iterations per benchmark.
 * Higher values = more accurate results but slower execution.
 * - 100000: High accuracy (slow, ~5-10 min)
 * - 10000: Good accuracy (moderate, ~30-60 sec)
 * - 1000: Quick comparison (fast, ~5-10 sec)
 */
const BENCH_ITERATIONS = 10000;

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

// --- 4. Array of Records with varying depths ---

// Array of records - 1 level deep
const arrayOfRecordsDepth1Schema: SchemaLike = {
  type: "array",
  items: {
    type: "record",
    name: "RecordDepth1",
    fields: [
      { name: "id", type: "int" },
      { name: "name", type: "string" },
      { name: "value", type: "double" },
    ],
  },
};

// Array of array of records - 2 levels deep
const arrayOfRecordsDepth2Schema: SchemaLike = {
  type: "array",
  items: {
    type: "array",
    items: {
      type: "record",
      name: "RecordDepth2",
      fields: [
        { name: "id", type: "int" },
        { name: "name", type: "string" },
        { name: "value", type: "double" },
      ],
    },
  },
};

// Array of array of array of records - 3 levels deep
const arrayOfRecordsDepth3Schema: SchemaLike = {
  type: "array",
  items: {
    type: "array",
    items: {
      type: "array",
      items: {
        type: "record",
        name: "RecordDepth3",
        fields: [
          { name: "id", type: "int" },
          { name: "name", type: "string" },
          { name: "value", type: "double" },
        ],
      },
    },
  },
};

// Array of array of array of array of records - 4 levels deep
const arrayOfRecordsDepth4Schema: SchemaLike = {
  type: "array",
  items: {
    type: "array",
    items: {
      type: "array",
      items: {
        type: "array",
        items: {
          type: "record",
          name: "RecordDepth4",
          fields: [
            { name: "id", type: "int" },
            { name: "name", type: "string" },
            { name: "value", type: "double" },
          ],
        },
      },
    },
  },
};

// --- 5. Array of Arrays (ints) with varying depths ---

// Array of arrays - 1 level deep (array of array of int)
const arrayOfArraysDepth1Schema: SchemaLike = {
  type: "array",
  items: {
    type: "array",
    items: "int",
  },
};

// Array of arrays - 2 levels deep
const arrayOfArraysDepth2Schema: SchemaLike = {
  type: "array",
  items: {
    type: "array",
    items: {
      type: "array",
      items: "int",
    },
  },
};

// Array of arrays - 3 levels deep
const arrayOfArraysDepth3Schema: SchemaLike = {
  type: "array",
  items: {
    type: "array",
    items: {
      type: "array",
      items: {
        type: "array",
        items: "int",
      },
    },
  },
};

// Array of arrays - 4 levels deep
const arrayOfArraysDepth4Schema: SchemaLike = {
  type: "array",
  items: {
    type: "array",
    items: {
      type: "array",
      items: {
        type: "array",
        items: {
          type: "array",
          items: "int",
        },
      },
    },
  },
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
    const keys = Object.keys(data);
    // Unwrap avro-typescript union format {type: value} -> value for avsc/avro-js
    // Only do this for single-key objects where the key is a primitive Avro type name
    if (
      keys.length === 1 &&
      ["null", "boolean", "int", "long", "float", "double", "bytes", "string"].includes(keys[0])
    ) {
      const value = data[keys[0]];
      return toNodeFormat(value);
    }
    // Regular object - recurse on all fields
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

const sharedWriteSyncBuffers = new Map<number, ArrayBuffer>();

function getSharedWriteSyncBuffer(size: number): ArrayBuffer {
  const existing = sharedWriteSyncBuffers.get(size);
  if (existing) {
    return existing;
  }
  const buffer = new ArrayBuffer(size);
  sharedWriteSyncBuffers.set(size, buffer);
  return buffer;
}

function runFullComparisonBenchmark(config: BenchmarkConfig) {
  const { groupName, types, avroTsData, nodeData, bufferSize = 1024 } = config;

  // --- avsc ---
  Deno.bench({
    name: `${groupName} (avsc)`,
    group: groupName,
    baseline: true,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avsc.toBuffer(nodeData);
  });

  // --- avro-js ---
  Deno.bench({
    name: `${groupName} (avro-js)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avroJs.toBuffer(nodeData);
  });

  // --- avro-typescript: toSyncBuffer variants ---
  Deno.bench({
    name: `${groupName} (avro-ts, validate=true)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avroTs.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: `${groupName} (avro-ts, validate=false)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avroTsUnchecked.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: `${groupName} (avro-ts, compiled, validate=true)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avroTsCompiled.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: `${groupName} (avro-ts, compiled, validate=false)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avroTsCompiledUnchecked.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: `${groupName} (avro-ts, interpreted, validate=true)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avroTsInterpreted.toSyncBuffer(avroTsData);
  });

  Deno.bench({
    name: `${groupName} (avro-ts, interpreted, validate=false)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avroTsInterpretedUnchecked.toSyncBuffer(avroTsData);
  });

  // --- avro-typescript: writeSync (pre-allocated buffer) variants ---
  {
    const buffer = new ArrayBuffer(bufferSize);
    Deno.bench({
      name: `${groupName} (avro-ts, writeSync, validate=true)`,
      group: groupName,
      n: BENCH_ITERATIONS,
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
      n: BENCH_ITERATIONS,
    }, () => {
      const writable = new SyncInMemoryWritableBuffer(buffer);
      const tap = new SyncWritableTap(writable);
      types.avroTsUnchecked.writeSync(tap, avroTsData);
    });
  }

  // --- avro-typescript: writeSync (shared pre-allocated buffer) variants ---
  {
    const buffer = getSharedWriteSyncBuffer(bufferSize);
    Deno.bench({
      name: `${groupName} (avro-ts, writeSync, reuse buffer, validate=true)`,
      group: groupName,
      n: BENCH_ITERATIONS,
    }, () => {
      const writable = new SyncInMemoryWritableBuffer(buffer);
      const tap = new SyncWritableTap(writable);
      types.avroTs.writeSync(tap, avroTsData);
    });
  }

  {
    const buffer = getSharedWriteSyncBuffer(bufferSize);
    Deno.bench({
      name: `${groupName} (avro-ts, writeSync, reuse buffer, validate=false)`,
      group: groupName,
      n: BENCH_ITERATIONS,
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

  runFullComparisonBenchmark({
    groupName: "complex: map<int>",
    types,
    avroTsData,
    nodeData,
    bufferSize: 512,
  });
}

// --- Map<string> ---
{
  const types = createLibraryTypes(mapOfStringsSchema);
  const avroTsData = types.avroTs.random() as Map<string, string>;
  const nodeData = Object.fromEntries(avroTsData.entries());

  runFullComparisonBenchmark({
    groupName: "complex: map<string>",
    types,
    avroTsData,
    nodeData,
    bufferSize: 512,
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
// 4. ARRAY OF RECORDS BENCHMARKS (by depth)
// =============================================================================

// --- Array of Records (depth 1) ---
{
  const types = createLibraryTypes(arrayOfRecordsDepth1Schema);
  const data = types.avroTs.random();
  const nodeData = toNodeFormat(data);

  runFullComparisonBenchmark({
    groupName: "array-of-records: depth 1",
    types,
    avroTsData: data,
    nodeData,
    bufferSize: 2048,
  });
}

// --- Array of Records (depth 2) ---
{
  const types = createLibraryTypes(arrayOfRecordsDepth2Schema);
  const data = types.avroTs.random();
  const nodeData = toNodeFormat(data);

  runFullComparisonBenchmark({
    groupName: "array-of-records: depth 2",
    types,
    avroTsData: data,
    nodeData,
    bufferSize: 4096,
  });
}

// --- Array of Records (depth 3) ---
{
  const types = createLibraryTypes(arrayOfRecordsDepth3Schema);
  const data = types.avroTs.random();
  const nodeData = toNodeFormat(data);

  runFullComparisonBenchmark({
    groupName: "array-of-records: depth 3",
    types,
    avroTsData: data,
    nodeData,
    bufferSize: 8192,
  });
}

// --- Array of Records (depth 4) ---
{
  const types = createLibraryTypes(arrayOfRecordsDepth4Schema);
  const data = types.avroTs.random();
  const nodeData = toNodeFormat(data);

  runFullComparisonBenchmark({
    groupName: "array-of-records: depth 4",
    types,
    avroTsData: data,
    nodeData,
    bufferSize: 16384,
  });
}

// =============================================================================
// 5. ARRAY OF ARRAYS BENCHMARKS (by depth)
// =============================================================================

// --- Array of Arrays (depth 1) ---
{
  const types = createLibraryTypes(arrayOfArraysDepth1Schema);
  const data = types.avroTs.random();
  const nodeData = toNodeFormat(data);

  runFullComparisonBenchmark({
    groupName: "array-of-arrays: depth 1",
    types,
    avroTsData: data,
    nodeData,
    bufferSize: 2048,
  });
}

// --- Array of Arrays (depth 2) ---
{
  const types = createLibraryTypes(arrayOfArraysDepth2Schema);
  const data = types.avroTs.random();
  const nodeData = toNodeFormat(data);

  runFullComparisonBenchmark({
    groupName: "array-of-arrays: depth 2",
    types,
    avroTsData: data,
    nodeData,
    bufferSize: 4096,
  });
}

// --- Array of Arrays (depth 3) ---
{
  const types = createLibraryTypes(arrayOfArraysDepth3Schema);
  const data = types.avroTs.random();
  const nodeData = toNodeFormat(data);

  runFullComparisonBenchmark({
    groupName: "array-of-arrays: depth 3",
    types,
    avroTsData: data,
    nodeData,
    bufferSize: 8192,
  });
}

// --- Array of Arrays (depth 4) ---
{
  const types = createLibraryTypes(arrayOfArraysDepth4Schema);
  const data = types.avroTs.random();
  const nodeData = toNodeFormat(data);

  runFullComparisonBenchmark({
    groupName: "array-of-arrays: depth 4",
    types,
    avroTsData: data,
    nodeData,
    bufferSize: 16384,
  });
}