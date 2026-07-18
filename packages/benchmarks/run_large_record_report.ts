#!/usr/bin/env -S deno run --allow-run=deno

import { largeRecordWorkloadDefinitions } from "./large_record_workloads.ts";

interface BenchStats {
  p75: number;
}

interface BenchResult {
  group: string;
  name: string;
  results: Array<{ ok?: BenchStats }>;
}

interface BenchOutput {
  runtime: string;
  benches: BenchResult[];
}

interface WorkloadResult {
  id: string;
  label: string;
  description: string;
  recordCount: number;
  byteLength: number;
  runtime: string;
  p75: Record<string, number>;
}

const REQUIRED_CONFIGURATIONS = [
  "avsc",
  "avro-js",
  "fromSyncBuffer",
  "direct-reused",
] as const;
const DEFAULT_PROCESS_REPEATS = 3;

const benchmarkFile = new URL(
  "./deserialize_large_records_bench.ts",
  import.meta.url,
).pathname;

function parseBenchOutput(output: string): BenchOutput {
  const jsonStart = output.indexOf("{");
  if (jsonStart < 0) {
    throw new Error(`Benchmark did not return JSON:\n${output}`);
  }
  return JSON.parse(output.slice(jsonStart)) as BenchOutput;
}

function configurationKey(name: string): string | undefined {
  if (name.endsWith("(avsc)")) return "avsc";
  if (name.endsWith("(avro-js)")) return "avro-js";
  if (name.endsWith("(avro-ts fromSyncBuffer)")) return "fromSyncBuffer";
  if (name.endsWith("(avro-ts direct-reused)")) return "direct-reused";
  return undefined;
}

function formatTime(nanoseconds: number): string {
  if (nanoseconds < 1_000) return `${nanoseconds.toFixed(1)} ns`;
  if (nanoseconds < 1_000_000) {
    return `${(nanoseconds / 1_000).toFixed(2)} us`;
  }
  return `${(nanoseconds / 1_000_000).toFixed(2)} ms`;
}

function formatBytes(bytes: number): string {
  if (bytes < 1_024) return `${bytes} B`;
  if (bytes < 1_048_576) return `${(bytes / 1_024).toFixed(1)} KiB`;
  return `${(bytes / 1_048_576).toFixed(2)} MiB`;
}

function formatThroughput(nanoseconds: number, records: number): string {
  const recordsPerSecond = records * 1_000_000_000 / nanoseconds;
  if (recordsPerSecond >= 1_000_000) {
    return `${(recordsPerSecond / 1_000_000).toFixed(2)} M/s`;
  }
  return `${(recordsPerSecond / 1_000).toFixed(1)} K/s`;
}

function comparison(candidate: number, reference: number): string {
  if (candidate <= reference) {
    return `${(reference / candidate).toFixed(2)}x faster`;
  }
  return `${(candidate / reference).toFixed(2)}x slower`;
}

function median(values: number[]): number {
  const sorted = values.toSorted((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1]! + sorted[middle]!) / 2
    : sorted[middle]!;
}

const repeatArguments = Deno.args.filter((arg) => arg.startsWith("--repeats="));
if (repeatArguments.length > 1) {
  throw new Error("Specify --repeats only once.");
}
const processRepeats = repeatArguments.length === 0
  ? DEFAULT_PROCESS_REPEATS
  : Number(repeatArguments[0]!.slice("--repeats=".length));
if (!Number.isSafeInteger(processRepeats) || processRepeats < 1) {
  throw new Error("--repeats must be a positive integer.");
}

const requestedIds = new Set(
  Deno.args.filter((arg) => arg !== "--" && !arg.startsWith("--repeats=")),
);
const definitions = requestedIds.size === 0
  ? largeRecordWorkloadDefinitions
  : largeRecordWorkloadDefinitions.filter(({ id }) => requestedIds.has(id));

if (definitions.length !== requestedIds.size && requestedIds.size > 0) {
  const knownIds = new Set(largeRecordWorkloadDefinitions.map(({ id }) => id));
  const unknown = [...requestedIds].filter((id) => !knownIds.has(id));
  throw new Error(
    `Unknown workload(s): ${unknown.join(", ")}. Expected: ${
      [...knownIds].join(", ")
    }`,
  );
}

const workloadResults: WorkloadResult[] = [];

for (const definition of definitions) {
  const samples = Object.fromEntries(
    REQUIRED_CONFIGURATIONS.map((key) => [key, [] as number[]]),
  ) as Record<(typeof REQUIRED_CONFIGURATIONS)[number], number[]>;
  const p75: Record<string, number> = {};
  let byteLength = 0;
  let runtime = "unknown";

  for (let repeat = 0; repeat < processRepeats; repeat++) {
    const command = new Deno.Command("deno", {
      args: [
        "bench",
        "--no-config",
        "--allow-read",
        "--allow-env=AVRO_LARGE_WORKLOAD",
        "--json",
        benchmarkFile,
      ],
      env: { AVRO_LARGE_WORKLOAD: definition.id },
      stdout: "piped",
      stderr: "piped",
    });
    const result = await command.output();
    const stdout = new TextDecoder().decode(result.stdout);
    if (!result.success) {
      const stderr = new TextDecoder().decode(result.stderr);
      throw new Error(
        `Benchmark ${definition.id} failed:\n${stdout}\n${stderr}`,
      );
    }

    const output = parseBenchOutput(stdout);
    runtime = output.runtime;
    for (const bench of output.benches) {
      const key = configurationKey(bench.name) as
        | (typeof REQUIRED_CONFIGURATIONS)[number]
        | undefined;
      const value = bench.results[0]?.ok?.p75;
      if (key && value !== undefined) samples[key].push(value);
      const metadata = bench.group.match(/\[records=\d+, bytes=(\d+)\]/);
      if (metadata) byteLength = Number(metadata[1]);
    }
  }

  for (const required of REQUIRED_CONFIGURATIONS) {
    if (samples[required].length !== processRepeats) {
      throw new Error(`${definition.id} is missing benchmark: ${required}`);
    }
    p75[required] = median(samples[required]);
  }

  workloadResults.push({
    id: definition.id,
    label: definition.label,
    description: definition.description,
    recordCount: definition.recordCount,
    byteLength,
    runtime,
    p75,
  });
}

console.log("# Large Heterogeneous Record Deserialization\n");
console.log(`Runtime: ${workloadResults[0]?.runtime ?? "unknown"}\n`);
const processRunLabel = processRepeats === 1 ? "run" : "runs";
console.log(
  `Each timed operation decodes one complete batch. Every exact decoder closure is warmed with at least 100,000 records before measurement; serialization and schema construction are outside the timed region. Values below are the median p75 from ${processRepeats} isolated process ${processRunLabel}.\n`,
);

console.log("## Batch latency\n");
console.log(
  "| Workload | Records | Wire size | avsc | avro-js | avro-ts fromSyncBuffer | avro-ts direct reused | Best avro-ts vs avsc | Best avro-ts vs avro-js |",
);
console.log(
  "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
);
for (const workload of workloadResults) {
  const bestAvroTs = Math.min(
    workload.p75.fromSyncBuffer,
    workload.p75["direct-reused"],
  );
  console.log(
    `| ${workload.label} | ${workload.recordCount.toLocaleString("en-US")} | ${
      formatBytes(workload.byteLength)
    } | ${formatTime(workload.p75.avsc)} | ${
      formatTime(workload.p75["avro-js"])
    } | ${formatTime(workload.p75.fromSyncBuffer)} | ${
      formatTime(workload.p75["direct-reused"])
    } | ${comparison(bestAvroTs, workload.p75.avsc)} | ${
      comparison(bestAvroTs, workload.p75["avro-js"])
    } |`,
  );
}

console.log("\n## Throughput\n");
console.log(
  "| Workload | avsc | avro-js | avro-ts fromSyncBuffer | avro-ts direct reused |",
);
console.log("| --- | ---: | ---: | ---: | ---: |");
for (const workload of workloadResults) {
  console.log(
    `| ${workload.label} | ${
      formatThroughput(workload.p75.avsc, workload.recordCount)
    } | ${formatThroughput(workload.p75["avro-js"], workload.recordCount)} | ${
      formatThroughput(workload.p75.fromSyncBuffer, workload.recordCount)
    } | ${
      formatThroughput(workload.p75["direct-reused"], workload.recordCount)
    } |`,
  );
}

console.log("\n## Workloads\n");
for (const workload of workloadResults) {
  console.log(`- **${workload.label}**: ${workload.description}`);
}
