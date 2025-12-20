#!/usr/bin/env -S deno run --allow-read --allow-write

/**
 * Comprehensive benchmark comparing Compiled vs Interpreted writer strategies
 * across different record type complexities, including avsc and avro-js.
 */

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

const ITERATIONS = 100000;
const compiledStrategy = new CompiledWriterStrategy();
const interpretedStrategy = new InterpretedWriterStrategy();

// Helper to convert data for avsc/avro-js (they use Node Buffer for bytes, number for longs)
function toNodeFriendlyData(data: unknown): unknown {
  if (data === null || data === undefined) return data;
  if (typeof data === "bigint") return Number(data);
  if (data instanceof Uint8Array) return Buffer.from(data);
  if (data instanceof Map) {
    const obj: Record<string, unknown> = {};
    for (const [k, v] of data) {
      obj[k] = toNodeFriendlyData(v);
    }
    return obj;
  }
  if (Array.isArray(data)) return data.map(toNodeFriendlyData);
  if (typeof data === "object") {
    const keys = Object.keys(data);
    // Unwrap avro-typescript union format {type: value} -> value for avsc/avro-js
    // Only do this for single-key objects where the key is a primitive Avro type name
    if (
      keys.length === 1 &&
      ["null", "boolean", "int", "long", "float", "double", "bytes", "string"].includes(keys[0])
    ) {
      const value = (data as Record<string, unknown>)[keys[0]];
      return toNodeFriendlyData(value);
    }
    // Regular object - recurse on all fields
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(data)) {
      result[k] = toNodeFriendlyData(v);
    }
    return result;
  }
  return data;
}

// =============================================================================
// 1. SIMPLE TYPES - All primitive types
// =============================================================================

const simpleTypesSchema: SchemaLike = {
  type: "record",
  name: "SimpleTypesRecord",
  fields: [
    { name: "nullField", type: "null" },
    { name: "boolField", type: "boolean" },
    { name: "intField", type: "int" },
    { name: "longField", type: "long" },
    { name: "floatField", type: "float" },
    { name: "doubleField", type: "double" },
    { name: "bytesField", type: "bytes" },
    { name: "stringField", type: "string" },
  ],
};

const simpleTypesCompiled = createType(simpleTypesSchema, {
  writerStrategy: compiledStrategy,
});
const simpleTypesInterpreted = createType(simpleTypesSchema, {
  writerStrategy: interpretedStrategy,
});
const simpleTypesCompiledUnchecked = createType(simpleTypesSchema, {
  writerStrategy: compiledStrategy,
  validate: false,
});
const simpleTypesInterpretedUnchecked = createType(simpleTypesSchema, {
  writerStrategy: interpretedStrategy,
  validate: false,
});

const simpleTypesData = simpleTypesCompiled.random();

// =============================================================================
// 2. COMPLEX TYPES - Arrays, Maps, Unions, Enums, Fixed
// =============================================================================

const complexTypesSchema: SchemaLike = {
  type: "record",
  name: "ComplexTypesRecord",
  fields: [
    { name: "arrayField", type: { type: "array", items: "int" } },
    { name: "mapField", type: { type: "map", values: "string" } },
    { name: "unionField", type: ["null", "string", "int"] },
    {
      name: "enumField",
      type: {
        type: "enum",
        name: "Status",
        symbols: ["PENDING", "ACTIVE", "COMPLETED", "FAILED"],
      },
    },
    { name: "fixedField", type: { type: "fixed", name: "Hash", size: 16 } },
  ],
};

const complexTypesCompiled = createType(complexTypesSchema, {
  writerStrategy: compiledStrategy,
});
const complexTypesInterpreted = createType(complexTypesSchema, {
  writerStrategy: interpretedStrategy,
});
const complexTypesCompiledUnchecked = createType(complexTypesSchema, {
  writerStrategy: compiledStrategy,
  validate: false,
});
const complexTypesInterpretedUnchecked = createType(complexTypesSchema, {
  writerStrategy: interpretedStrategy,
  validate: false,
});

const complexTypesData = complexTypesCompiled.random();

// =============================================================================
// 3. COMBINATION TYPE - Uses all types together
// =============================================================================

const combinationSchema: SchemaLike = {
  type: "record",
  name: "CombinationRecord",
  fields: [
    // Primitives
    { name: "id", type: "int" },
    { name: "uuid", type: "string" },
    { name: "timestamp", type: "long" },
    { name: "value", type: "double" },
    { name: "isActive", type: "boolean" },
    { name: "data", type: "bytes" },
    // Complex
    { name: "tags", type: { type: "array", items: "string" } },
    { name: "metadata", type: { type: "map", values: "string" } },
    { name: "optionalValue", type: ["null", "double"] },
    {
      name: "status",
      type: {
        type: "enum",
        name: "RecordStatus",
        symbols: ["DRAFT", "PUBLISHED", "ARCHIVED"],
      },
    },
    { name: "checksum", type: { type: "fixed", name: "Checksum", size: 8 } },
  ],
};

const combinationCompiled = createType(combinationSchema, {
  writerStrategy: compiledStrategy,
});
const combinationInterpreted = createType(combinationSchema, {
  writerStrategy: interpretedStrategy,
});
const combinationCompiledUnchecked = createType(combinationSchema, {
  writerStrategy: compiledStrategy,
  validate: false,
});
const combinationInterpretedUnchecked = createType(combinationSchema, {
  writerStrategy: interpretedStrategy,
  validate: false,
});

const combinationData = combinationCompiled.random();

// =============================================================================
// 4. NESTED 1 LAYER
// =============================================================================

const nested1Schema: SchemaLike = {
  type: "record",
  name: "Nested1Record",
  fields: [
    { name: "id", type: "int" },
    { name: "name", type: "string" },
    {
      name: "child",
      type: {
        type: "record",
        name: "Child1",
        fields: [
          { name: "value", type: "int" },
          { name: "label", type: "string" },
        ],
      },
    },
  ],
};

const nested1Compiled = createType(nested1Schema, {
  writerStrategy: compiledStrategy,
});
const nested1Interpreted = createType(nested1Schema, {
  writerStrategy: interpretedStrategy,
});
const nested1CompiledUnchecked = createType(nested1Schema, {
  writerStrategy: compiledStrategy,
  validate: false,
});
const nested1InterpretedUnchecked = createType(nested1Schema, {
  writerStrategy: interpretedStrategy,
  validate: false,
});

const nested1Data = nested1Compiled.random();

// =============================================================================
// 5. NESTED 2 LAYERS
// =============================================================================

const nested2Schema: SchemaLike = {
  type: "record",
  name: "Nested2Record",
  fields: [
    { name: "id", type: "int" },
    { name: "name", type: "string" },
    {
      name: "level1",
      type: {
        type: "record",
        name: "Level1",
        fields: [
          { name: "value", type: "int" },
          {
            name: "level2",
            type: {
              type: "record",
              name: "Level2",
              fields: [
                { name: "deepValue", type: "double" },
                { name: "deepName", type: "string" },
              ],
            },
          },
        ],
      },
    },
  ],
};

const nested2Compiled = createType(nested2Schema, {
  writerStrategy: compiledStrategy,
});
const nested2Interpreted = createType(nested2Schema, {
  writerStrategy: interpretedStrategy,
});
const nested2CompiledUnchecked = createType(nested2Schema, {
  writerStrategy: compiledStrategy,
  validate: false,
});
const nested2InterpretedUnchecked = createType(nested2Schema, {
  writerStrategy: interpretedStrategy,
  validate: false,
});

const nested2Data = nested2Compiled.random();

// =============================================================================
// 6. NESTED 3 LAYERS
// =============================================================================

const nested3Schema: SchemaLike = {
  type: "record",
  name: "Nested3Record",
  fields: [
    { name: "id", type: "int" },
    {
      name: "layer1",
      type: {
        type: "record",
        name: "Layer1",
        fields: [
          { name: "name", type: "string" },
          {
            name: "layer2",
            type: {
              type: "record",
              name: "Layer2",
              fields: [
                { name: "value", type: "int" },
                {
                  name: "layer3",
                  type: {
                    type: "record",
                    name: "Layer3",
                    fields: [
                      { name: "deepId", type: "long" },
                      { name: "deepData", type: "bytes" },
                      { name: "deepFlag", type: "boolean" },
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

const nested3Compiled = createType(nested3Schema, {
  writerStrategy: compiledStrategy,
});
const nested3Interpreted = createType(nested3Schema, {
  writerStrategy: interpretedStrategy,
});
const nested3CompiledUnchecked = createType(nested3Schema, {
  writerStrategy: compiledStrategy,
  validate: false,
});
const nested3InterpretedUnchecked = createType(nested3Schema, {
  writerStrategy: interpretedStrategy,
  validate: false,
});

const nested3Data = nested3Compiled.random();

// =============================================================================
// BENCHMARK DEFINITIONS
// =============================================================================

// Helper to create benchmark for a category
function createBenchmarks(
  category: string,
  schema: SchemaLike,
  compiledType: ReturnType<typeof createType>,
  interpretedType: ReturnType<typeof createType>,
  compiledUncheckedType: ReturnType<typeof createType>,
  interpretedUncheckedType: ReturnType<typeof createType>,
  data: unknown,
) {
  const buffer = new ArrayBuffer(4096);

  // Create avsc and avro-js types
  const avscType = avsc.Type.forSchema(schema as Parameters<typeof avsc.Type.forSchema>[0]);
  const avroJsType = avrojs.parse(schema as Parameters<typeof avrojs.parse>[0]);
  const nodeData = toNodeFriendlyData(data);

  // Compiled + Validated
  Deno.bench({
    name: `[${category}] compiled, validate=true`,
    group: category,
    n: ITERATIONS,
  }, () => {
    const writable = new SyncInMemoryWritableBuffer(buffer);
    const tap = new SyncWritableTap(writable);
    compiledType.writeSync(tap, data);
  });

  // Interpreted + Validated
  Deno.bench({
    name: `[${category}] interpreted, validate=true`,
    group: category,
    n: ITERATIONS,
  }, () => {
    const writable = new SyncInMemoryWritableBuffer(buffer);
    const tap = new SyncWritableTap(writable);
    interpretedType.writeSync(tap, data);
  });

  // Compiled + Unchecked
  Deno.bench({
    name: `[${category}] compiled, validate=false`,
    group: category,
    n: ITERATIONS,
  }, () => {
    const writable = new SyncInMemoryWritableBuffer(buffer);
    const tap = new SyncWritableTap(writable);
    compiledUncheckedType.writeSync(tap, data);
  });

  // Interpreted + Unchecked
  Deno.bench({
    name: `[${category}] interpreted, validate=false`,
    group: category,
    n: ITERATIONS,
  }, () => {
    const writable = new SyncInMemoryWritableBuffer(buffer);
    const tap = new SyncWritableTap(writable);
    interpretedUncheckedType.writeSync(tap, data);
  });

  // avsc (no explicit validation - internal checks only)
  Deno.bench({
    name: `[${category}] avsc`,
    group: category,
    n: ITERATIONS,
  }, () => {
    try {
      avscType.toBuffer(nodeData);
    } catch {
      // Ignore errors from incompatible data formats (e.g., unions)
    }
  });

  // avro-js (no explicit validation - internal checks only)
  Deno.bench({
    name: `[${category}] avro-js`,
    group: category,
    n: ITERATIONS,
  }, () => {
    try {
      avroJsType.toBuffer(nodeData);
    } catch {
      // Ignore errors from incompatible data formats (e.g., unions)
    }
  });

  // toSyncBuffer benchmarks (includes allocation)
  Deno.bench({
    name: `[${category}] compiled, toSyncBuffer, validate=true`,
    group: `${category}-toSyncBuffer`,
    n: ITERATIONS,
  }, () => {
    compiledType.toSyncBuffer(data);
  });

  Deno.bench({
    name: `[${category}] interpreted, toSyncBuffer, validate=true`,
    group: `${category}-toSyncBuffer`,
    n: ITERATIONS,
  }, () => {
    interpretedType.toSyncBuffer(data);
  });

  Deno.bench({
    name: `[${category}] compiled, toSyncBuffer, validate=false`,
    group: `${category}-toSyncBuffer`,
    n: ITERATIONS,
  }, () => {
    compiledUncheckedType.toSyncBuffer(data);
  });

  Deno.bench({
    name: `[${category}] interpreted, toSyncBuffer, validate=false`,
    group: `${category}-toSyncBuffer`,
    n: ITERATIONS,
  }, () => {
    interpretedUncheckedType.toSyncBuffer(data);
  });

  // avsc toSyncBuffer equivalent
  Deno.bench({
    name: `[${category}] avsc, toBuffer`,
    group: `${category}-toSyncBuffer`,
    n: ITERATIONS,
  }, () => {
    try {
      avscType.toBuffer(nodeData);
    } catch {
      // Ignore errors from incompatible data formats (e.g., unions)
    }
  });

  // avro-js toSyncBuffer equivalent
  Deno.bench({
    name: `[${category}] avro-js, toBuffer`,
    group: `${category}-toSyncBuffer`,
    n: ITERATIONS,
  }, () => {
    try {
      avroJsType.toBuffer(nodeData);
    } catch {
      // Ignore errors from incompatible data formats (e.g., unions)
    }
  });
}

// Create all benchmarks
createBenchmarks(
  "1-simple-types",
  simpleTypesSchema,
  simpleTypesCompiled,
  simpleTypesInterpreted,
  simpleTypesCompiledUnchecked,
  simpleTypesInterpretedUnchecked,
  simpleTypesData,
);

createBenchmarks(
  "2-complex-types",
  complexTypesSchema,
  complexTypesCompiled,
  complexTypesInterpreted,
  complexTypesCompiledUnchecked,
  complexTypesInterpretedUnchecked,
  complexTypesData,
);

createBenchmarks(
  "3-combination",
  combinationSchema,
  combinationCompiled,
  combinationInterpreted,
  combinationCompiledUnchecked,
  combinationInterpretedUnchecked,
  combinationData,
);

createBenchmarks(
  "4-nested-1-layer",
  nested1Schema,
  nested1Compiled,
  nested1Interpreted,
  nested1CompiledUnchecked,
  nested1InterpretedUnchecked,
  nested1Data,
);

createBenchmarks(
  "5-nested-2-layers",
  nested2Schema,
  nested2Compiled,
  nested2Interpreted,
  nested2CompiledUnchecked,
  nested2InterpretedUnchecked,
  nested2Data,
);

createBenchmarks(
  "6-nested-3-layers",
  nested3Schema,
  nested3Compiled,
  nested3Interpreted,
  nested3CompiledUnchecked,
  nested3InterpretedUnchecked,
  nested3Data,
);
