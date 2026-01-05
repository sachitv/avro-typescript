#!/usr/bin/env -S deno run --allow-read --allow-write

import { Buffer } from "node:buffer";
import avsc from "npm:avsc";
import avrojs from "npm:avro-js";
import type { SchemaLike } from "../../src/type/create_type.ts";
import { createType } from "../../src/mod.ts";
import {
  SyncInMemoryReadableBuffer,
} from "../../src/serialization/buffers/in_memory_buffer_sync.ts";
import { SyncReadableTap } from "../../src/serialization/tap_sync.ts";
import { InMemoryReadableBuffer } from "../../src/serialization/buffers/in_memory_buffer.ts";
import { ReadableTap } from "../../src/serialization/tap.ts";

/**
 * Comprehensive benchmark comparing deserialization (read) performance
 * across avro-typescript vs avsc vs avro-js for all Avro types.
 *
 * This mirrors serialize_single_bench.ts but focuses on read operations.
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
 * - 100: Very quick (for initial testing)
 */
const BENCH_ITERATIONS = 100;

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

// deno-lint-ignore no-explicit-any
type AvscType = ReturnType<typeof avsc.Type.forSchema>;
// deno-lint-ignore no-explicit-any
type AvroJsType = ReturnType<typeof avrojs.parse>;

interface LibraryTypes {
  avroTs: ReturnType<typeof createType>;
  avsc: AvscType;
  avroJs: AvroJsType;
}

function createLibraryTypes(schema: SchemaLike): LibraryTypes {
  // avro-js requires object schema format, convert string primitives
  const avroJsSchema =
    typeof schema === "string" ? { type: schema } : schema;

  return {
    avroTs: createType(schema),
    avsc: avsc.Type.forSchema(
      schema as Parameters<typeof avsc.Type.forSchema>[0],
    ),
    avroJs: avrojs.parse(avroJsSchema as Parameters<typeof avrojs.parse>[0]),
  };
}

// =============================================================================
// DATA CONVERSION HELPERS
// =============================================================================

// =============================================================================
// BENCHMARK RUNNER
// =============================================================================

interface DeserializeBenchmarkConfig {
  groupName: string;
  types: LibraryTypes;
  avroTsData: unknown;
}

interface PreSerializedData {
  avroTsBuffer: ArrayBuffer;
  nodeBuffer: Buffer;
}

/**
 * Pre-serialize data once before benchmarking deserialization.
 * We use avro-ts to serialize since it handles bigints correctly.
 * The Avro binary format is the same across all libraries, so we can
 * use the same buffer for both avro-ts and avsc/avro-js deserialization.
 * This ensures we're only measuring read performance, not serialization overhead.
 */
function preSerializeData(
  types: LibraryTypes,
  avroTsData: unknown,
): PreSerializedData {
  const avroTsBuffer = types.avroTs.toSyncBuffer(avroTsData);
  // Convert to Node Buffer for avsc/avro-js
  const nodeBuffer = Buffer.from(avroTsBuffer);
  return { avroTsBuffer, nodeBuffer };
}

function runDeserializeBenchmark(config: DeserializeBenchmarkConfig) {
  const { groupName, types, avroTsData } = config;

  // Pre-serialize data once using avro-ts (binary format is the same)
  const { avroTsBuffer, nodeBuffer } = preSerializeData(types, avroTsData);

  // --- avsc (baseline) ---
  Deno.bench({
    name: `${groupName} (avsc)`,
    group: groupName,
    baseline: true,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avsc.fromBuffer(nodeBuffer);
  });

  // --- avro-js ---
  Deno.bench({
    name: `${groupName} (avro-js)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avroJs.fromBuffer(nodeBuffer);
  });

  // --- avro-typescript: fromSyncBuffer (convenience method) ---
  Deno.bench({
    name: `${groupName} (avro-ts, fromSyncBuffer)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, () => {
    types.avroTs.fromSyncBuffer(avroTsBuffer);
  });

  // --- avro-typescript: readSync with tap (manual setup) ---
  Deno.bench({
    name: `${groupName} (avro-ts, readSync)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, () => {
    const readable = new SyncInMemoryReadableBuffer(avroTsBuffer);
    const tap = new SyncReadableTap(readable);
    types.avroTs.readSync(tap);
  });

  // --- avro-typescript: async read (for comparison) ---
  Deno.bench({
    name: `${groupName} (avro-ts, read async)`,
    group: groupName,
    n: BENCH_ITERATIONS,
  }, async () => {
    const readable = new InMemoryReadableBuffer(avroTsBuffer);
    const tap = new ReadableTap(readable);
    await types.avroTs.read(tap);
  });
}

// =============================================================================
// DETERMINISTIC TEST DATA FIXTURES
// =============================================================================

/**
 * Fixed test data to ensure deterministic, reproducible benchmarks.
 * Using random data causes massive variance (65%+) between runs.
 *
 * Design principles:
 * - Arrays always have 10 elements (large, realistic workload)
 * - Ints use large values (stress varint encoding)
 * - Longs use true bigint values (> 2^31, cannot fit in int32)
 * - Strings are realistic length (not too short)
 */
const FIXTURES = {
  // Primitives
  null: null,
  boolean: true,
  int: 1234567890, // Large int value
  long: 9223372036854775807n, // Max int64 value (true bigint)
  float: 3.14159,
  double: 2.718281828459045,
  bytes: new Uint8Array([
    0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c,
  ]), // "Hello Worl" - 10 bytes
  string: "The quick brown fox jumps over the lazy dog",

  // Enums
  enum: "ACTIVE",

  // Fixed
  fixed: new Uint8Array([
    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
    0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
  ]), // 16 bytes

  // Arrays - always 10 elements with large values
  arrayOfInts: [
    1000000001,
    1000000002,
    1000000003,
    1000000004,
    1000000005,
    1000000006,
    1000000007,
    1000000008,
    1000000009,
    1000000010,
  ],
  arrayOfStrings: [
    "string_01",
    "string_02",
    "string_03",
    "string_04",
    "string_05",
    "string_06",
    "string_07",
    "string_08",
    "string_09",
    "string_10",
  ],

  // Maps (avro-typescript uses Map, avsc/avro-js use plain objects) - 10 entries
  mapOfInts: new Map([
    ["key_a", 1000001],
    ["key_b", 1000002],
    ["key_c", 1000003],
    ["key_d", 1000004],
    ["key_e", 1000005],
    ["key_f", 1000006],
    ["key_g", 1000007],
    ["key_h", 1000008],
    ["key_i", 1000009],
    ["key_j", 1000010],
  ]),
  mapOfStrings: new Map([
    ["key_01", "value_01"],
    ["key_02", "value_02"],
    ["key_03", "value_03"],
    ["key_04", "value_04"],
    ["key_05", "value_05"],
    ["key_06", "value_06"],
    ["key_07", "value_07"],
    ["key_08", "value_08"],
    ["key_09", "value_09"],
    ["key_10", "value_10"],
  ]),

  // Records
  simpleRecord: {
    id: 9876543,
    name: "Test Record Name",
    value: 99.99,
    active: true,
    data: new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe]),
  },

  deeplyNestedRecord: {
    id: 1000001,
    name: "Level 1 Record",
    level2: {
      id: 2000002,
      description: "Level 2 description with meaningful text",
      level3: {
        id: 3000003,
        value: 123.456,
        level4: {
          id: 4000004,
          data: new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a]),
          tags: [
            "tag_001",
            "tag_002",
            "tag_003",
            "tag_004",
            "tag_005",
            "tag_006",
            "tag_007",
            "tag_008",
            "tag_009",
            "tag_010",
          ],
        },
      },
    },
  },

  recordWithArrayOfRecords: {
    id: 5000100,
    title: "Parent Record Title",
    items: [
      { id: 5001001, name: "Item 001", value: 10.5 },
      { id: 5001002, name: "Item 002", value: 20.5 },
      { id: 5001003, name: "Item 003", value: 30.5 },
      { id: 5001004, name: "Item 004", value: 40.5 },
      { id: 5001005, name: "Item 005", value: 50.5 },
      { id: 5001006, name: "Item 006", value: 60.5 },
      { id: 5001007, name: "Item 007", value: 70.5 },
      { id: 5001008, name: "Item 008", value: 80.5 },
      { id: 5001009, name: "Item 009", value: 90.5 },
      { id: 5001010, name: "Item 010", value: 100.5 },
    ],
  },

  // Array of records - depth 1 (10 records)
  arrayOfRecordsDepth1: [
    { id: 6001001, name: "Record 001", value: 1.1 },
    { id: 6001002, name: "Record 002", value: 2.2 },
    { id: 6001003, name: "Record 003", value: 3.3 },
    { id: 6001004, name: "Record 004", value: 4.4 },
    { id: 6001005, name: "Record 005", value: 5.5 },
    { id: 6001006, name: "Record 006", value: 6.6 },
    { id: 6001007, name: "Record 007", value: 7.7 },
    { id: 6001008, name: "Record 008", value: 8.8 },
    { id: 6001009, name: "Record 009", value: 9.9 },
    { id: 6001010, name: "Record 010", value: 10.1 },
  ],

  // Array of records - depth 2 (10x10 structure)
  arrayOfRecordsDepth2: [
    [
      { id: 7001001, name: "R01-01", value: 1.01 },
      { id: 7001002, name: "R01-02", value: 1.02 },
      { id: 7001003, name: "R01-03", value: 1.03 },
      { id: 7001004, name: "R01-04", value: 1.04 },
      { id: 7001005, name: "R01-05", value: 1.05 },
      { id: 7001006, name: "R01-06", value: 1.06 },
      { id: 7001007, name: "R01-07", value: 1.07 },
      { id: 7001008, name: "R01-08", value: 1.08 },
      { id: 7001009, name: "R01-09", value: 1.09 },
      { id: 7001010, name: "R01-10", value: 1.10 },
    ],
    [
      { id: 7002001, name: "R02-01", value: 2.01 },
      { id: 7002002, name: "R02-02", value: 2.02 },
      { id: 7002003, name: "R02-03", value: 2.03 },
      { id: 7002004, name: "R02-04", value: 2.04 },
      { id: 7002005, name: "R02-05", value: 2.05 },
      { id: 7002006, name: "R02-06", value: 2.06 },
      { id: 7002007, name: "R02-07", value: 2.07 },
      { id: 7002008, name: "R02-08", value: 2.08 },
      { id: 7002009, name: "R02-09", value: 2.09 },
      { id: 7002010, name: "R02-10", value: 2.10 },
    ],
    [
      { id: 7003001, name: "R03-01", value: 3.01 },
      { id: 7003002, name: "R03-02", value: 3.02 },
      { id: 7003003, name: "R03-03", value: 3.03 },
      { id: 7003004, name: "R03-04", value: 3.04 },
      { id: 7003005, name: "R03-05", value: 3.05 },
      { id: 7003006, name: "R03-06", value: 3.06 },
      { id: 7003007, name: "R03-07", value: 3.07 },
      { id: 7003008, name: "R03-08", value: 3.08 },
      { id: 7003009, name: "R03-09", value: 3.09 },
      { id: 7003010, name: "R03-10", value: 3.10 },
    ],
    [
      { id: 7004001, name: "R04-01", value: 4.01 },
      { id: 7004002, name: "R04-02", value: 4.02 },
      { id: 7004003, name: "R04-03", value: 4.03 },
      { id: 7004004, name: "R04-04", value: 4.04 },
      { id: 7004005, name: "R04-05", value: 4.05 },
      { id: 7004006, name: "R04-06", value: 4.06 },
      { id: 7004007, name: "R04-07", value: 4.07 },
      { id: 7004008, name: "R04-08", value: 4.08 },
      { id: 7004009, name: "R04-09", value: 4.09 },
      { id: 7004010, name: "R04-10", value: 4.10 },
    ],
    [
      { id: 7005001, name: "R05-01", value: 5.01 },
      { id: 7005002, name: "R05-02", value: 5.02 },
      { id: 7005003, name: "R05-03", value: 5.03 },
      { id: 7005004, name: "R05-04", value: 5.04 },
      { id: 7005005, name: "R05-05", value: 5.05 },
      { id: 7005006, name: "R05-06", value: 5.06 },
      { id: 7005007, name: "R05-07", value: 5.07 },
      { id: 7005008, name: "R05-08", value: 5.08 },
      { id: 7005009, name: "R05-09", value: 5.09 },
      { id: 7005010, name: "R05-10", value: 5.10 },
    ],
    [
      { id: 7006001, name: "R06-01", value: 6.01 },
      { id: 7006002, name: "R06-02", value: 6.02 },
      { id: 7006003, name: "R06-03", value: 6.03 },
      { id: 7006004, name: "R06-04", value: 6.04 },
      { id: 7006005, name: "R06-05", value: 6.05 },
      { id: 7006006, name: "R06-06", value: 6.06 },
      { id: 7006007, name: "R06-07", value: 6.07 },
      { id: 7006008, name: "R06-08", value: 6.08 },
      { id: 7006009, name: "R06-09", value: 6.09 },
      { id: 7006010, name: "R06-10", value: 6.10 },
    ],
    [
      { id: 7007001, name: "R07-01", value: 7.01 },
      { id: 7007002, name: "R07-02", value: 7.02 },
      { id: 7007003, name: "R07-03", value: 7.03 },
      { id: 7007004, name: "R07-04", value: 7.04 },
      { id: 7007005, name: "R07-05", value: 7.05 },
      { id: 7007006, name: "R07-06", value: 7.06 },
      { id: 7007007, name: "R07-07", value: 7.07 },
      { id: 7007008, name: "R07-08", value: 7.08 },
      { id: 7007009, name: "R07-09", value: 7.09 },
      { id: 7007010, name: "R07-10", value: 7.10 },
    ],
    [
      { id: 7008001, name: "R08-01", value: 8.01 },
      { id: 7008002, name: "R08-02", value: 8.02 },
      { id: 7008003, name: "R08-03", value: 8.03 },
      { id: 7008004, name: "R08-04", value: 8.04 },
      { id: 7008005, name: "R08-05", value: 8.05 },
      { id: 7008006, name: "R08-06", value: 8.06 },
      { id: 7008007, name: "R08-07", value: 8.07 },
      { id: 7008008, name: "R08-08", value: 8.08 },
      { id: 7008009, name: "R08-09", value: 8.09 },
      { id: 7008010, name: "R08-10", value: 8.10 },
    ],
    [
      { id: 7009001, name: "R09-01", value: 9.01 },
      { id: 7009002, name: "R09-02", value: 9.02 },
      { id: 7009003, name: "R09-03", value: 9.03 },
      { id: 7009004, name: "R09-04", value: 9.04 },
      { id: 7009005, name: "R09-05", value: 9.05 },
      { id: 7009006, name: "R09-06", value: 9.06 },
      { id: 7009007, name: "R09-07", value: 9.07 },
      { id: 7009008, name: "R09-08", value: 9.08 },
      { id: 7009009, name: "R09-09", value: 9.09 },
      { id: 7009010, name: "R09-10", value: 9.10 },
    ],
    [
      { id: 7010001, name: "R10-01", value: 10.01 },
      { id: 7010002, name: "R10-02", value: 10.02 },
      { id: 7010003, name: "R10-03", value: 10.03 },
      { id: 7010004, name: "R10-04", value: 10.04 },
      { id: 7010005, name: "R10-05", value: 10.05 },
      { id: 7010006, name: "R10-06", value: 10.06 },
      { id: 7010007, name: "R10-07", value: 10.07 },
      { id: 7010008, name: "R10-08", value: 10.08 },
      { id: 7010009, name: "R10-09", value: 10.09 },
      { id: 7010010, name: "R10-10", value: 10.10 },
    ],
  ],

  // Array of records - depth 3 (10x10x10 = 1000 records total)
  // Using compact notation for readability, but maintaining 10 elements per level
  arrayOfRecordsDepth3: Array.from({ length: 10 }, (_, i) =>
    Array.from({ length: 10 }, (_, j) =>
      Array.from({ length: 10 }, (_, k) => ({
        id: 8000000 + i * 10000 + j * 100 + k,
        name: `R${String(i + 1).padStart(2, "0")}-${String(j + 1).padStart(2, "0")}-${String(k + 1).padStart(2, "0")}`,
        value: (i + 1) + (j + 1) / 10 + (k + 1) / 100,
      }))
    )
  ),

  // Array of records - depth 4 (10x10x10x10 = 10,000 records total)
  // This is a very large dataset to stress-test performance
  arrayOfRecordsDepth4: Array.from({ length: 10 }, (_, i) =>
    Array.from({ length: 10 }, (_, j) =>
      Array.from({ length: 10 }, (_, k) =>
        Array.from({ length: 10 }, (_, l) => ({
          id: 9000000 + i * 100000 + j * 10000 + k * 100 + l,
          name: `R${String(i + 1).padStart(2, "0")}-${String(j + 1).padStart(2, "0")}-${String(k + 1).padStart(2, "0")}-${String(l + 1).padStart(2, "0")}`,
          value: (i + 1) + (j + 1) / 10 + (k + 1) / 100 + (l + 1) / 1000,
        }))
      )
    )
  ),

  // Array of arrays - depth 1 (array<array<int>>) - 10x10 = 100 ints
  arrayOfArraysDepth1: [
    [1000001, 1000002, 1000003, 1000004, 1000005, 1000006, 1000007, 1000008, 1000009, 1000010],
    [1000011, 1000012, 1000013, 1000014, 1000015, 1000016, 1000017, 1000018, 1000019, 1000020],
    [1000021, 1000022, 1000023, 1000024, 1000025, 1000026, 1000027, 1000028, 1000029, 1000030],
    [1000031, 1000032, 1000033, 1000034, 1000035, 1000036, 1000037, 1000038, 1000039, 1000040],
    [1000041, 1000042, 1000043, 1000044, 1000045, 1000046, 1000047, 1000048, 1000049, 1000050],
    [1000051, 1000052, 1000053, 1000054, 1000055, 1000056, 1000057, 1000058, 1000059, 1000060],
    [1000061, 1000062, 1000063, 1000064, 1000065, 1000066, 1000067, 1000068, 1000069, 1000070],
    [1000071, 1000072, 1000073, 1000074, 1000075, 1000076, 1000077, 1000078, 1000079, 1000080],
    [1000081, 1000082, 1000083, 1000084, 1000085, 1000086, 1000087, 1000088, 1000089, 1000090],
    [1000091, 1000092, 1000093, 1000094, 1000095, 1000096, 1000097, 1000098, 1000099, 1000100],
  ],

  // Array of arrays - depth 2 (array<array<array<int>>>) - 10x10x10 = 1000 ints
  arrayOfArraysDepth2: Array.from({ length: 10 }, (_, i) =>
    Array.from({ length: 10 }, (_, j) =>
      Array.from({ length: 10 }, (_, k) => 2000000 + i * 10000 + j * 100 + k)
    )
  ),

  // Array of arrays - depth 3 (array<array<array<array<int>>>>) - 10x10x10x10 = 10,000 ints
  arrayOfArraysDepth3: Array.from({ length: 10 }, (_, i) =>
    Array.from({ length: 10 }, (_, j) =>
      Array.from({ length: 10 }, (_, k) =>
        Array.from({ length: 10 }, (_, l) => 3000000 + i * 100000 + j * 10000 + k * 100 + l)
      )
    )
  ),

  // Array of arrays - depth 4 (array<array<array<array<array<int>>>>>) - 10x10x10x10x10 = 100,000 ints
  arrayOfArraysDepth4: Array.from({ length: 10 }, (_, i) =>
    Array.from({ length: 10 }, (_, j) =>
      Array.from({ length: 10 }, (_, k) =>
        Array.from({ length: 10 }, (_, l) =>
          Array.from({ length: 10 }, (_, m) =>
            4000000 + i * 1000000 + j * 100000 + k * 10000 + l * 100 + m
          )
        )
      )
    )
  ),
};

// =============================================================================
// 1. PRIMITIVE TYPE BENCHMARKS
// =============================================================================

// --- Null ---
{
  const types = createLibraryTypes(primitiveSchemas.null);
  runDeserializeBenchmark({
    groupName: "primitive: null",
    types,
    avroTsData: FIXTURES.null,
  });
}

// --- Boolean ---
{
  const types = createLibraryTypes(primitiveSchemas.boolean);
  runDeserializeBenchmark({
    groupName: "primitive: boolean",
    types,
    avroTsData: FIXTURES.boolean,
  });
}

// --- Int ---
{
  const types = createLibraryTypes(primitiveSchemas.int);
  runDeserializeBenchmark({
    groupName: "primitive: int",
    types,
    avroTsData: FIXTURES.int,
  });
}

// --- Long ---
{
  const types = createLibraryTypes(primitiveSchemas.long);
  runDeserializeBenchmark({
    groupName: "primitive: long",
    types,
    avroTsData: FIXTURES.long,
  });
}

// --- Float ---
{
  const types = createLibraryTypes(primitiveSchemas.float);
  runDeserializeBenchmark({
    groupName: "primitive: float",
    types,
    avroTsData: FIXTURES.float,
  });
}

// --- Double ---
{
  const types = createLibraryTypes(primitiveSchemas.double);
  runDeserializeBenchmark({
    groupName: "primitive: double",
    types,
    avroTsData: FIXTURES.double,
  });
}

// --- Bytes ---
{
  const types = createLibraryTypes(primitiveSchemas.bytes);
  runDeserializeBenchmark({
    groupName: "primitive: bytes",
    types,
    avroTsData: FIXTURES.bytes,
  });
}

// --- String ---
{
  const types = createLibraryTypes(primitiveSchemas.string);
  runDeserializeBenchmark({
    groupName: "primitive: string",
    types,
    avroTsData: FIXTURES.string,
  });
}

// =============================================================================
// 2. COMPLEX TYPE BENCHMARKS
// =============================================================================

// --- Enum ---
{
  const types = createLibraryTypes(enumSchema);
  runDeserializeBenchmark({
    groupName: "complex: enum",
    types,
    avroTsData: FIXTURES.enum,
  });
}

// --- Fixed ---
{
  const types = createLibraryTypes(fixedSchema);
  runDeserializeBenchmark({
    groupName: "complex: fixed",
    types,
    avroTsData: FIXTURES.fixed,
  });
}

// --- Array<int> ---
{
  const types = createLibraryTypes(arrayOfIntsSchema);
  runDeserializeBenchmark({
    groupName: "complex: array<int>",
    types,
    avroTsData: FIXTURES.arrayOfInts,
  });
}

// --- Array<string> ---
{
  const types = createLibraryTypes(arrayOfStringsSchema);
  runDeserializeBenchmark({
    groupName: "complex: array<string>",
    types,
    avroTsData: FIXTURES.arrayOfStrings,
  });
}

// --- Map<int> ---
{
  const types = createLibraryTypes(mapOfIntsSchema);
  runDeserializeBenchmark({
    groupName: "complex: map<int>",
    types,
    avroTsData: FIXTURES.mapOfInts,
  });
}

// --- Map<string> ---
{
  const types = createLibraryTypes(mapOfStringsSchema);
  runDeserializeBenchmark({
    groupName: "complex: map<string>",
    types,
    avroTsData: FIXTURES.mapOfStrings,
  });
}

// =============================================================================
// 3. RECORD BENCHMARKS
// =============================================================================

// --- Simple Record ---
{
  const types = createLibraryTypes(simpleRecordSchema);
  runDeserializeBenchmark({
    groupName: "record: simple",
    types,
    avroTsData: FIXTURES.simpleRecord,
  });
}

// --- Deeply Nested Record (4 levels) ---
{
  const types = createLibraryTypes(deeplyNestedRecordSchema);
  runDeserializeBenchmark({
    groupName: "record: nested (4 levels)",
    types,
    avroTsData: FIXTURES.deeplyNestedRecord,
  });
}

// --- Record with Array of Records ---
{
  const types = createLibraryTypes(recordWithArrayOfRecordsSchema);
  runDeserializeBenchmark({
    groupName: "record: array of records",
    types,
    avroTsData: FIXTURES.recordWithArrayOfRecords,
  });
}

// =============================================================================
// 4. ARRAY OF RECORDS BENCHMARKS (by depth)
// =============================================================================

// --- Array of Records (depth 1) ---
{
  const types = createLibraryTypes(arrayOfRecordsDepth1Schema);
  runDeserializeBenchmark({
    groupName: "array-of-records: depth 1",
    types,
    avroTsData: FIXTURES.arrayOfRecordsDepth1,
  });
}

// --- Array of Records (depth 2) ---
{
  const types = createLibraryTypes(arrayOfRecordsDepth2Schema);
  runDeserializeBenchmark({
    groupName: "array-of-records: depth 2",
    types,
    avroTsData: FIXTURES.arrayOfRecordsDepth2,
  });
}

// --- Array of Records (depth 3) ---
{
  const types = createLibraryTypes(arrayOfRecordsDepth3Schema);
  runDeserializeBenchmark({
    groupName: "array-of-records: depth 3",
    types,
    avroTsData: FIXTURES.arrayOfRecordsDepth3,
  });
}

// --- Array of Records (depth 4) ---
{
  const types = createLibraryTypes(arrayOfRecordsDepth4Schema);
  runDeserializeBenchmark({
    groupName: "array-of-records: depth 4",
    types,
    avroTsData: FIXTURES.arrayOfRecordsDepth4,
  });
}

// =============================================================================
// 5. ARRAY OF ARRAYS BENCHMARKS (by depth)
// =============================================================================

// --- Array of Arrays (depth 1) ---
{
  const types = createLibraryTypes(arrayOfArraysDepth1Schema);
  runDeserializeBenchmark({
    groupName: "array-of-arrays: depth 1",
    types,
    avroTsData: FIXTURES.arrayOfArraysDepth1,
  });
}

// --- Array of Arrays (depth 2) ---
{
  const types = createLibraryTypes(arrayOfArraysDepth2Schema);
  runDeserializeBenchmark({
    groupName: "array-of-arrays: depth 2",
    types,
    avroTsData: FIXTURES.arrayOfArraysDepth2,
  });
}

// --- Array of Arrays (depth 3) ---
{
  const types = createLibraryTypes(arrayOfArraysDepth3Schema);
  runDeserializeBenchmark({
    groupName: "array-of-arrays: depth 3",
    types,
    avroTsData: FIXTURES.arrayOfArraysDepth3,
  });
}

// --- Array of Arrays (depth 4) ---
{
  const types = createLibraryTypes(arrayOfArraysDepth4Schema);
  runDeserializeBenchmark({
    groupName: "array-of-arrays: depth 4",
    types,
    avroTsData: FIXTURES.arrayOfArraysDepth4,
  });
}
