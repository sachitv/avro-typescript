import type { SchemaLike } from "../../src/type/create_type.ts";

export interface BuiltLargeRecordWorkload {
  schema: SchemaLike;
  records: unknown[];
}

export interface LargeRecordWorkloadDefinition {
  id: string;
  label: string;
  description: string;
  recordCount: number;
  build(): BuiltLargeRecordWorkload;
}

const FLAT_RECORD_COUNT = 4_096;
const NESTED_RECORD_COUNT = 2_048;
const UNION_RECORD_COUNT = 6_144;

function makeBytes(length: number, seed: number): Uint8Array {
  const value = new Uint8Array(length);
  for (let index = 0; index < length; index++) {
    value[index] = (seed * 31 + index * 17) & 0xff;
  }
  return value;
}

function buildNumericTelemetry(): BuiltLargeRecordWorkload {
  const schema: SchemaLike = {
    type: "array",
    items: {
      type: "record",
      name: "LargeNumericTelemetryRecord",
      fields: [
        { name: "deviceId", type: "int" },
        { name: "timestamp", type: "long" },
        { name: "temperature", type: "float" },
        { name: "reading", type: "double" },
        { name: "healthy", type: "boolean" },
      ],
    },
  };
  const records = Array.from({ length: FLAT_RECORD_COUNT }, (_, index) => ({
    deviceId: 1_000_000 + index,
    timestamp: 1_700_000_000_000n + BigInt(index * 250),
    temperature: 12.5 + (index % 80) * 0.25,
    reading: Math.sin(index / 20) * 1_000 + index / 10,
    healthy: index % 11 !== 0,
  }));
  return { schema, records };
}

function buildTextAndBinary(): BuiltLargeRecordWorkload {
  const schema: SchemaLike = {
    type: "array",
    items: {
      type: "record",
      name: "LargeTextAndBinaryRecord",
      fields: [
        { name: "id", type: "int" },
        {
          name: "level",
          type: {
            type: "enum",
            name: "LargeLogLevel",
            symbols: ["DEBUG", "INFO", "WARN", "ERROR"],
          },
        },
        { name: "message", type: "string" },
        { name: "payload", type: "bytes" },
      ],
    },
  };
  const messages = [
    "ok",
    "worker completed its assigned shard",
    "café déjà vu — UTF-8 text",
    "東京リージョンで処理が完了しました",
    "🚀 deployment completed with zero dropped messages",
    "x".repeat(96),
  ];
  const levels = ["DEBUG", "INFO", "WARN", "ERROR"];
  const records = Array.from({ length: FLAT_RECORD_COUNT }, (_, index) => ({
    id: 2_000_000 + index,
    level: levels[index % levels.length],
    message: `service-${index % 32}: ${
      messages[index % messages.length]
    } #${index}`,
    payload: makeBytes(8 + (index % 5) * 16, index),
  }));
  return { schema, records };
}

function buildNestedCollections(): BuiltLargeRecordWorkload {
  const schema: SchemaLike = {
    type: "array",
    items: {
      type: "record",
      name: "LargeNestedCollectionRecord",
      fields: [
        { name: "id", type: "int" },
        {
          name: "location",
          type: {
            type: "record",
            name: "LargeLocationRecord",
            fields: [
              { name: "latitude", type: "double" },
              { name: "longitude", type: "double" },
              { name: "region", type: "string" },
            ],
          },
        },
        { name: "samples", type: { type: "array", items: "float" } },
        { name: "attributes", type: { type: "map", values: "string" } },
      ],
    },
  };
  const records = Array.from({ length: NESTED_RECORD_COUNT }, (_, index) => ({
    id: 3_000_000 + index,
    location: {
      latitude: -80 + (index % 1_600) / 10,
      longitude: -170 + (index % 3_400) / 10,
      region: `region-${index % 12}`,
    },
    samples: Array.from(
      { length: 4 + (index % 9) },
      (_, sample) => index / 10 + sample * 0.125,
    ),
    attributes: new Map([
      ["host", `host-${index % 64}`],
      ["rack", `rack-${index % 16}`],
      ["mode", index % 2 === 0 ? "active" : "standby"],
    ]),
  }));
  return { schema, records };
}

function buildOptionalAndFixed(): BuiltLargeRecordWorkload {
  const schema: SchemaLike = {
    type: "array",
    items: {
      type: "record",
      name: "LargeOptionalAndFixedRecord",
      fields: [
        { name: "id", type: "long" },
        {
          name: "status",
          type: {
            type: "enum",
            name: "LargeOperationStatus",
            symbols: ["QUEUED", "RUNNING", "COMPLETE", "FAILED"],
          },
        },
        {
          name: "traceId",
          type: { type: "fixed", name: "LargeTraceId", size: 16 },
        },
        { name: "note", type: ["null", "string"] },
        { name: "measurement", type: ["null", "double"] },
        { name: "payload", type: "bytes" },
      ],
    },
  };
  const statuses = ["QUEUED", "RUNNING", "COMPLETE", "FAILED"];
  const records = Array.from({ length: FLAT_RECORD_COUNT }, (_, index) => ({
    id: 4_000_000_000n + BigInt(index),
    status: statuses[index % statuses.length],
    traceId: makeBytes(16, index + 100),
    note: index % 3 === 0 ? null : { string: `operation-note-${index}` },
    measurement: index % 5 === 0 ? null : { double: index * 0.0625 },
    payload: makeBytes(8 + (index % 4) * 8, index + 150),
  }));
  return { schema, records };
}

function buildWideBusinessRecords(): BuiltLargeRecordWorkload {
  const schema: SchemaLike = {
    type: "array",
    items: {
      type: "record",
      name: "LargeWideBusinessRecord",
      fields: [
        { name: "id", type: "int" },
        { name: "sequence", type: "long" },
        { name: "active", type: "boolean" },
        { name: "priority", type: "int" },
        { name: "ratio", type: "float" },
        { name: "amount", type: "double" },
        { name: "firstName", type: "string" },
        { name: "lastName", type: "string" },
        { name: "email", type: "string" },
        { name: "payload", type: "bytes" },
        {
          name: "region",
          type: {
            type: "enum",
            name: "LargeBusinessRegion",
            symbols: ["NA", "SA", "EU", "AF", "APAC"],
          },
        },
        {
          name: "signature",
          type: { type: "fixed", name: "LargeSignature", size: 16 },
        },
      ],
    },
  };
  const regions = ["NA", "SA", "EU", "AF", "APAC"];
  const records = Array.from({ length: FLAT_RECORD_COUNT }, (_, index) => ({
    id: 5_000_000 + index,
    sequence: 8_000_000_000n + BigInt(index),
    active: index % 7 !== 0,
    priority: index % 10,
    ratio: (index % 100) / 100,
    amount: 1_000 + index * 1.25,
    firstName: `First${index % 128}`,
    lastName: `Last${index % 256}`,
    email: `user-${index}@example.test`,
    payload: makeBytes(12 + (index % 4) * 8, index + 200),
    region: regions[index % regions.length],
    signature: makeBytes(16, index + 300),
  }));
  return { schema, records };
}

function buildHeterogeneousEvents(): BuiltLargeRecordWorkload {
  const schema: SchemaLike = {
    type: "array",
    items: [
      {
        type: "record",
        name: "LargeClickEvent",
        fields: [
          { name: "id", type: "int" },
          { name: "target", type: "string" },
          { name: "x", type: "int" },
          { name: "y", type: "int" },
        ],
      },
      {
        type: "record",
        name: "LargePurchaseEvent",
        fields: [
          { name: "id", type: "long" },
          { name: "sku", type: "string" },
          { name: "quantity", type: "int" },
          { name: "price", type: "double" },
          {
            name: "currency",
            type: {
              type: "enum",
              name: "LargeCurrency",
              symbols: ["USD", "EUR", "JPY", "GBP"],
            },
          },
        ],
      },
      {
        type: "record",
        name: "LargeHeartbeatEvent",
        fields: [
          { name: "node", type: "string" },
          { name: "timestamp", type: "long" },
          { name: "metrics", type: { type: "map", values: "double" } },
        ],
      },
    ],
  };
  const currencies = ["USD", "EUR", "JPY", "GBP"];
  const records = Array.from({ length: UNION_RECORD_COUNT }, (_, index) => {
    switch (index % 3) {
      case 0:
        return {
          LargeClickEvent: {
            id: 6_000_000 + index,
            target: `button-${index % 48}`,
            x: index % 1_920,
            y: index % 1_080,
          },
        };
      case 1:
        return {
          LargePurchaseEvent: {
            id: 9_000_000_000n + BigInt(index),
            sku: `SKU-${index % 512}`,
            quantity: 1 + (index % 8),
            price: 5 + (index % 100) * 1.99,
            currency: currencies[index % currencies.length],
          },
        };
      default:
        return {
          LargeHeartbeatEvent: {
            node: `node-${index % 128}`,
            timestamp: 1_700_000_000_000n + BigInt(index * 1_000),
            metrics: new Map([
              ["cpu", (index % 100) / 100],
              ["memory", (index % 80) / 80],
            ]),
          },
        };
    }
  });
  return { schema, records };
}

export const largeRecordWorkloadDefinitions:
  readonly LargeRecordWorkloadDefinition[] = [
    {
      id: "numeric-telemetry",
      label: "numeric telemetry",
      description:
        "Five primitive numeric/boolean fields, including long and float.",
      recordCount: FLAT_RECORD_COUNT,
      build: buildNumericTelemetry,
    },
    {
      id: "text-and-binary",
      label: "text and binary",
      description:
        "Four fields covering short, long, Unicode, enum, and variable-length bytes.",
      recordCount: FLAT_RECORD_COUNT,
      build: buildTextAndBinary,
    },
    {
      id: "nested-collections",
      label: "nested collections",
      description:
        "Four outer fields covering nested records, arrays, maps, strings, floats, and doubles.",
      recordCount: NESTED_RECORD_COUNT,
      build: buildNestedCollections,
    },
    {
      id: "optional-and-fixed",
      label: "optional and fixed",
      description:
        "Six fields covering nullable unions, enum, fixed/variable bytes, long, and double.",
      recordCount: FLAT_RECORD_COUNT,
      build: buildOptionalAndFixed,
    },
    {
      id: "wide-business-records",
      label: "wide business records",
      description: "Twelve fields to exercise the wide-record scalar fallback.",
      recordCount: FLAT_RECORD_COUNT,
      build: buildWideBusinessRecords,
    },
    {
      id: "heterogeneous-events",
      label: "heterogeneous event union",
      description:
        "A mixed array cycling through three differently shaped record branches.",
      recordCount: UNION_RECORD_COUNT,
      build: buildHeterogeneousEvents,
    },
  ];
