#!/usr/bin/env -S deno run --allow-read --allow-env --allow-run

/**
 * Isolated cross-library read sweep.
 *
 * Motivation: measuring every library and configuration inside one process
 * distorts avro-typescript's numbers. Its compiled readers are closures, and
 * V8 shares feedback vectors between closures created from the same source
 * literal, so mixing tap classes or schemas in one process makes hot call
 * sites polymorphic and keeps the reader chain under-tiered. avsc/avro-js
 * generate per-schema readers with `new Function`, which are immune to this.
 * One (workload, variant) pair per process keeps every measurement clean.
 *
 * Usage:
 *   deno run --allow-read --allow-env --allow-run isolated_sweep_bench.ts
 *     Runs the full sweep (every workload x variant in its own subprocess)
 *     and prints one TSV line per combination:
 *     workload variant median_ns best_ns rep_list
 *
 *   deno run --allow-read --allow-env isolated_sweep_bench.ts <workload> <variant>
 *     Runs a single combination in this process.
 *
 * Workloads: depth1 | depth2 | depth3 | depth4 | nested-record | union-null |
 *   union-primitives | union-records
 * Variants:  avsc | avro-js | avrots-fsb | avrots-reused | avrots-direct
 */

import { Buffer } from "node:buffer";
import { createType, type SchemaLike } from "../../src/type/create_type.ts";
import { DirectSyncReadableTap } from "../../src/serialization/direct_tap_sync.ts";

const WORKLOADS = [
  "depth1",
  "depth2",
  "depth3",
  "depth4",
  "nested-record",
  "array-wide8",
  "array-wide16",
  "heterogeneous",
  "record-with-array",
  "int-arrays-shallow",
  "int-arrays-deep",
  "union-null",
  "union-primitives",
  "union-records",
] as const;
const VARIANTS = [
  "avsc",
  "avro-js",
  "avrots-fsb",
  "avrots-reused",
  "avrots-direct",
] as const;
const TARGET_WARMUP_MS = 150;
const TARGET_REPETITION_MS = 300;
const MAX_BATCH_ITERATIONS = 2_000_000;

let resultSink: unknown;

function runBatch(fn: () => unknown, iterations: number): void {
  let result: unknown;
  for (let i = 0; i < iterations; i++) {
    result = fn();
  }
  resultSink = result;
}

function makeDepthSchema(depth: number): SchemaLike {
  let items: SchemaLike = {
    type: "record",
    name: `RecordDepth${depth}`,
    fields: [
      { name: "id", type: "int" },
      { name: "name", type: "string" },
      { name: "value", type: "double" },
    ],
  };
  for (let i = 0; i < depth; i++) {
    items = { type: "array", items };
  }
  return items;
}

function makeDepthData(depth: number, prefix: string, base: number): unknown {
  if (depth === 0) {
    return { id: base, name: `R${prefix}`, value: base / 1000 + 1.5 };
  }
  return Array.from({ length: 10 }, (_, i) =>
    makeDepthData(
      depth - 1,
      `${prefix}-${String(i + 1).padStart(2, "0")}`,
      base * 10 + i,
    ));
}

const nestedRecordSchema: SchemaLike = {
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
                      {
                        name: "tags",
                        type: { type: "array", items: "string" },
                      },
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

const nestedRecordData = {
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
        data: new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        tags: Array.from(
          { length: 10 },
          (_, i) => `tag_${String(i + 1).padStart(3, "0")}`,
        ),
      },
    },
  },
};

/**
 * Array of records with many fields: guards against tuning record readers to
 * the narrow 3-field shape used by the depth workloads. Fields cycle through
 * int/string/double.
 */
function makeWideWorkload(
  fieldCount: number,
): { schema: SchemaLike; data: unknown } {
  const kinds = ["int", "string", "double"] as const;
  const fields = Array.from({ length: fieldCount }, (_, i) => ({
    name: `field${i}`,
    type: kinds[i % 3]!,
  }));
  const schema: SchemaLike = {
    type: "array",
    items: { type: "record", name: `WideRecord${fieldCount}`, fields },
  };
  const data = Array.from({ length: 10 }, (_, r) => {
    const record: Record<string, unknown> = {};
    for (let i = 0; i < fieldCount; i++) {
      if (i % 3 === 0) record[`field${i}`] = r * 1000 + i;
      else if (i % 3 === 1) record[`field${i}`] = `value ${r}-${i}`;
      else record[`field${i}`] = r + i / 100;
    }
    return record;
  });
  return { schema, data };
}

/**
 * Array of records mixing every major Avro shape (long, boolean, float, enum,
 * fixed, bytes, nullable union, array, map, nested record): guards against
 * tuning readers to int/string/double-only workloads.
 *
 * Data uses avro-typescript's value conventions (bigint longs, Uint8Array
 * bytes/fixed, Map maps, wrapped unions); the wire format is identical across
 * libraries, so decode timing remains comparable even though the decoded
 * representations differ.
 */
function makeHeterogeneousWorkload(): { schema: SchemaLike; data: unknown } {
  const schema: SchemaLike = {
    type: "array",
    items: {
      type: "record",
      name: "HeterogeneousRecord",
      fields: [
        { name: "id", type: "long" },
        { name: "name", type: "string" },
        { name: "active", type: "boolean" },
        { name: "ratio", type: "float" },
        {
          name: "status",
          type: {
            type: "enum",
            name: "HeterogeneousStatus",
            symbols: ["PENDING", "ACTIVE", "COMPLETED", "FAILED"],
          },
        },
        {
          name: "uuid",
          type: { type: "fixed", name: "HeterogeneousUuid", size: 16 },
        },
        { name: "payload", type: "bytes" },
        { name: "maybeNote", type: ["null", "string"] },
        { name: "tags", type: { type: "array", items: "string" } },
        { name: "attributes", type: { type: "map", values: "string" } },
        {
          name: "child",
          type: {
            type: "record",
            name: "HeterogeneousChild",
            fields: [
              { name: "id", type: "int" },
              { name: "label", type: "string" },
            ],
          },
        },
      ],
    },
  };
  const statuses = ["PENDING", "ACTIVE", "COMPLETED", "FAILED"];
  // Long values stay within 2^53 so avsc (which decodes longs as JS numbers
  // and rejects unsafe values) can decode the same wire bytes.
  const data = Array.from({ length: 10 }, (_, r) => ({
    id: 4503599627370496n + BigInt(r),
    name: `Heterogeneous record ${r} with 日本語`,
    active: r % 2 === 0,
    ratio: r / 7,
    status: statuses[r % 4]!,
    uuid: new Uint8Array(
      Array.from({ length: 16 }, (_, i) => (r * 16 + i) & 0xff),
    ),
    payload: new Uint8Array(
      Array.from({ length: 24 }, (_, i) => (r + i) & 0xff),
    ),
    maybeNote: r % 3 === 0 ? null : { string: `note-${r}` },
    tags: Array.from({ length: 5 }, (_, i) => `tag-${r}-${i}`),
    attributes: new Map([
      [`attr-a-${r}`, `alpha-${r}`],
      [`attr-b-${r}`, `beta-${r}`],
      [`attr-c-${r}`, `gamma-${r}`],
    ]),
    child: { id: r * 11, label: `child-${r}` },
  }));
  return { schema, data };
}

/** Mirrors the mixed suite's "record: array of records" group. */
function makeRecordWithArrayWorkload(): { schema: SchemaLike; data: unknown } {
  const schema: SchemaLike = {
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
  const data = {
    id: 5000100,
    title: "Parent Record Title",
    items: Array.from({ length: 10 }, (_, i) => ({
      id: 5001001 + i,
      name: `Item ${String(i + 1).padStart(3, "0")}`,
      value: (i + 1) * 10 + 0.5,
    })),
  };
  return { schema, data };
}

/** Nested int arrays mirroring the mixed suite's "array-of-arrays" groups. */
function makeIntArraysWorkload(
  depth: number,
): { schema: SchemaLike; data: unknown } {
  let schema: SchemaLike = { type: "array", items: "int" };
  for (let i = 0; i < depth; i++) {
    schema = { type: "array", items: schema };
  }
  const build = (d: number, base: number): unknown =>
    d === 0
      ? Array.from({ length: 10 }, (_, i) => base * 100 + i + 1000000)
      : Array.from({ length: 10 }, (_, i) => build(d - 1, base * 10 + i));
  return { schema, data: build(depth, 1) };
}

/** Nullable union indices without payload reads or wrapper allocations. */
function makeNullUnionWorkload(): { schema: SchemaLike; data: unknown } {
  return {
    schema: { type: "array", items: ["null", "int"] },
    data: new Array(1_024).fill(null),
  };
}

/** Mixed primitive branches exercise index dispatch and wrapped results. */
function makePrimitiveUnionWorkload(): {
  schema: SchemaLike;
  data: unknown;
} {
  const schema: SchemaLike = {
    type: "array",
    items: ["null", "int", "string", "double"],
  };
  const data = Array.from({ length: 768 }, (_, index) => {
    switch (index % 4) {
      case 0:
        return null;
      case 1:
        return { int: 1_000_000 + index };
      case 2:
        return { string: `union-${index}` };
      default:
        return { double: index + 0.125 };
    }
  });
  return { schema, data };
}

/** Named record branches exercise the common heterogeneous-event shape. */
function makeRecordUnionWorkload(): { schema: SchemaLike; data: unknown } {
  const schema: SchemaLike = {
    type: "array",
    items: [
      {
        type: "record",
        name: "UnionClick",
        fields: [
          { name: "id", type: "int" },
          { name: "target", type: "string" },
        ],
      },
      {
        type: "record",
        name: "UnionPurchase",
        fields: [
          { name: "id", type: "long" },
          { name: "amount", type: "double" },
        ],
      },
      {
        type: "record",
        name: "UnionHeartbeat",
        fields: [
          { name: "node", type: "string" },
          { name: "healthy", type: "boolean" },
        ],
      },
    ],
  };
  const data = Array.from({ length: 768 }, (_, index) => {
    switch (index % 3) {
      case 0:
        return {
          UnionClick: { id: index, target: `target-${index % 32}` },
        };
      case 1:
        return {
          UnionPurchase: {
            id: 4_000_000_000n + BigInt(index),
            amount: index + 0.99,
          },
        };
      default:
        return {
          UnionHeartbeat: {
            node: `node-${index % 64}`,
            healthy: index % 5 !== 0,
          },
        };
    }
  });
  return { schema, data };
}

function getWorkload(name: string): { schema: SchemaLike; data: unknown } {
  if (name === "nested-record") {
    return { schema: nestedRecordSchema, data: nestedRecordData };
  }
  if (name === "heterogeneous") {
    return makeHeterogeneousWorkload();
  }
  if (name === "record-with-array") {
    return makeRecordWithArrayWorkload();
  }
  if (name === "int-arrays-shallow") {
    return makeIntArraysWorkload(1);
  }
  if (name === "int-arrays-deep") {
    return makeIntArraysWorkload(4);
  }
  if (name === "union-null") {
    return makeNullUnionWorkload();
  }
  if (name === "union-primitives") {
    return makePrimitiveUnionWorkload();
  }
  if (name === "union-records") {
    return makeRecordUnionWorkload();
  }
  const wideMatch = name.match(/^array-wide(\d+)$/);
  if (wideMatch) {
    return makeWideWorkload(Number(wideMatch[1]));
  }
  const match = name.match(/^depth([1-4])$/);
  if (!match) {
    throw new Error(`Unknown workload: ${name}`);
  }
  const depth = Number(match[1]);
  return {
    schema: makeDepthSchema(depth),
    data: makeDepthData(depth, "01", 6001),
  };
}

async function measure(workload: string, variant: string): Promise<void> {
  const { schema, data } = getWorkload(workload);

  // Serialize with avro-ts; the wire format is identical across libraries.
  const serializer = createType(schema);
  const buf = serializer.toSyncBuffer(data);
  const u8 = new Uint8Array(buf);

  let fn: () => unknown;
  switch (variant) {
    case "avsc": {
      const avsc = (await import("npm:avsc@5.7.9")).default;
      const t = avsc.Type.forSchema(
        schema as Parameters<typeof avsc.Type.forSchema>[0],
      );
      const nodeBuf = Buffer.from(u8);
      fn = () => t.fromBuffer(nodeBuf);
      break;
    }
    case "avro-js": {
      const avrojs = (await import("npm:avro-js@1.12.1")).default;
      const t = avrojs.parse(schema as Parameters<typeof avrojs.parse>[0]);
      const nodeBuf = Buffer.from(u8);
      fn = () => t.fromBuffer(nodeBuf);
      break;
    }
    case "avrots-fsb": {
      const t = createType(schema);
      fn = () => t.fromSyncBuffer(buf);
      break;
    }
    case "avrots-reused": {
      const t = createType(schema);
      const tap = new DirectSyncReadableTap(u8);
      t.readSync(tap);
      fn = () => {
        tap.pos = 0;
        return t.readSync(tap);
      };
      break;
    }
    case "avrots-direct": {
      const t = createType(schema);
      fn = () => t.readSync(new DirectSyncReadableTap(u8));
      break;
    }
    default:
      throw new Error(`Unknown variant: ${variant}`);
  }

  // Warm before calibration. Estimating from cold calls drastically
  // under-counts iterations after V8 tiers up, turning nominal 300 ms reps
  // into a few milliseconds and producing bimodal results.
  let warmupIterations = 1;
  let warmupElapsed = 0;
  do {
    const start = performance.now();
    runBatch(fn, warmupIterations);
    warmupElapsed = performance.now() - start;
    if (warmupElapsed < TARGET_WARMUP_MS) {
      warmupIterations = Math.min(
        MAX_BATCH_ITERATIONS,
        warmupIterations * 2,
      );
    }
  } while (
    warmupElapsed < TARGET_WARMUP_MS &&
    warmupIterations < MAX_BATCH_ITERATIONS
  );

  const millisecondsPerIteration = warmupElapsed / warmupIterations;
  const iters = Math.max(
    1,
    Math.min(
      MAX_BATCH_ITERATIONS,
      Math.round(TARGET_REPETITION_MS / millisecondsPerIteration),
    ),
  );

  const reps: number[] = [];
  for (let r = 0; r < 5; r++) {
    const start = performance.now();
    runBatch(fn, iters);
    reps.push((performance.now() - start) * 1e6 / iters);
  }
  reps.sort((a, b) => a - b);
  console.log(
    `${workload}\t${variant}\t${reps[2]!.toFixed(1)}\t${reps[0]!.toFixed(1)}\t${
      reps.map((r) => r.toFixed(0)).join(",")
    }`,
  );
}

async function runFullSweep(): Promise<void> {
  console.log("workload\tvariant\tmedian_ns\tbest_ns\treps_ns");
  for (const workload of WORKLOADS) {
    for (const variant of VARIANTS) {
      const result = await new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--no-config",
          "--allow-read",
          "--allow-env",
          import.meta.url,
          workload,
          variant,
        ],
        stdout: "piped",
        stderr: "piped",
      }).output();
      if (!result.success) {
        console.error(
          `FAILED ${workload}/${variant}:`,
          new TextDecoder().decode(result.stderr).trim(),
        );
        continue;
      }
      const line = new TextDecoder().decode(result.stdout).trim();
      console.log(line);
    }
  }
}

if (Deno.args.length === 0) {
  await runFullSweep();
} else {
  await measure(Deno.args[0]!, Deno.args[1]!);
}

if (resultSink === runBatch) {
  console.log(resultSink);
}
