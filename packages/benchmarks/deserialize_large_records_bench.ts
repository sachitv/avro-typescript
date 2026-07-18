#!/usr/bin/env -S deno bench --no-config --allow-read --allow-env=AVRO_LARGE_WORKLOAD

import { Buffer } from "node:buffer";
import avsc from "npm:avsc@5.7.9";
import avrojs from "npm:avro-js@1.12.1";

import { createType } from "../../src/type/create_type.ts";
import {
  DirectSyncReadableTap,
} from "../../src/serialization/direct_tap_sync.ts";
import { largeRecordWorkloadDefinitions } from "./large_record_workloads.ts";

const TARGET_MEASURED_RECORDS = 500_000;
const TARGET_WARMUP_RECORDS = 100_000;
const MIN_WARMUP_BATCHES = 12;

interface Decoder {
  key: "avsc" | "avro-js" | "fromSyncBuffer" | "direct-reused";
  label: string;
  decode(): unknown;
}

let decodedLengthSink = 0;

function consumeDecoded(value: unknown, expectedLength: number): void {
  if (!Array.isArray(value) || value.length !== expectedLength) {
    throw new Error(
      `Expected ${expectedLength} decoded records, received ${
        Array.isArray(value) ? value.length : typeof value
      }`,
    );
  }
  decodedLengthSink = (decodedLengthSink + value.length) | 0;
}

const selectedWorkload = Deno.env.get("AVRO_LARGE_WORKLOAD");
const definitions = selectedWorkload
  ? largeRecordWorkloadDefinitions.filter(({ id }) => id === selectedWorkload)
  : largeRecordWorkloadDefinitions;

if (definitions.length === 0) {
  throw new Error(
    `Unknown AVRO_LARGE_WORKLOAD: ${selectedWorkload}. Expected one of: ${
      largeRecordWorkloadDefinitions.map(({ id }) => id).join(", ")
    }`,
  );
}

for (const definition of definitions) {
  const { schema, records } = definition.build();
  if (records.length !== definition.recordCount) {
    throw new Error(
      `${definition.id} declared ${definition.recordCount} records but built ${records.length}`,
    );
  }

  const avroTsType = createType(schema);
  const avscType = avsc.Type.forSchema(
    schema as Parameters<typeof avsc.Type.forSchema>[0],
  );
  const avroJsType = avrojs.parse(
    schema as Parameters<typeof avrojs.parse>[0],
  );
  const avroTsBuffer = avroTsType.toSyncBuffer(records);
  const bytes = new Uint8Array(avroTsBuffer);
  const nodeBuffer = Buffer.from(bytes);
  const directTap = new DirectSyncReadableTap(bytes);

  const decoders: Decoder[] = [
    {
      key: "avsc",
      label: "avsc",
      decode: () => avscType.fromBuffer(nodeBuffer),
    },
    {
      key: "avro-js",
      label: "avro-js",
      decode: () => avroJsType.fromBuffer(nodeBuffer),
    },
    {
      key: "fromSyncBuffer",
      label: "avro-ts fromSyncBuffer",
      decode: () => avroTsType.fromSyncBuffer(avroTsBuffer),
    },
    {
      key: "direct-reused",
      label: "avro-ts direct-reused",
      decode: () => {
        directTap.pos = 0;
        return avroTsType.readSync(directTap);
      },
    },
  ];

  // Validate every decoder once before timing, then warm the exact closures
  // passed to Deno.bench. This makes the benchmark measure steady-state batch
  // throughput rather than schema setup or first-tier compilation.
  for (const decoder of decoders) {
    consumeDecoded(decoder.decode(), definition.recordCount);
  }
  const warmupBatches = Math.max(
    MIN_WARMUP_BATCHES,
    Math.ceil(TARGET_WARMUP_RECORDS / definition.recordCount),
  );
  for (let batch = 0; batch < warmupBatches; batch++) {
    for (const decoder of decoders) {
      consumeDecoded(decoder.decode(), definition.recordCount);
    }
  }

  const measuredBatches = Math.max(
    20,
    Math.ceil(TARGET_MEASURED_RECORDS / definition.recordCount),
  );
  const group =
    `large records: ${definition.label} [records=${definition.recordCount}, bytes=${avroTsBuffer.byteLength}]`;

  for (const decoder of decoders) {
    Deno.bench({
      name: `${group} (${decoder.label})`,
      group,
      baseline: decoder.key === "avsc",
      n: measuredBatches,
    }, () => {
      consumeDecoded(decoder.decode(), definition.recordCount);
    });
  }
}

if (decodedLengthSink === -1) {
  console.log(decodedLengthSink);
}
