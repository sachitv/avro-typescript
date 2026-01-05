#!/usr/bin/env -S deno run --allow-read --allow-write

/**
 * Script to run deserialize single record benchmark and generate a focused report
 * with comprehensive comparison table across all libraries and configurations.
 */

// Run the deserialize single record benchmark
const benchCommand = new Deno.Command("deno", {
  args: [
    "bench",
    "--no-config",
    "--allow-read",
    "--allow-write",
    "--json",
    "deserialize_single_bench.ts",
  ],
  stdout: "piped",
  stderr: "piped",
});

console.log("Running deserialize (read) benchmark...");

const result = await benchCommand.output();
const decoder = new TextDecoder();

const output = decoder.decode(result.stdout);

// Deno bench --json may report "Bench failed" even when benchmarks run successfully
// Check if we got valid JSON output before considering it a failure
if (!result.success && !output.includes('"benches"')) {
  const stderr = decoder.decode(result.stderr);
  console.error("Benchmark failed: Check deserialize_single_bench.ts");
  console.error(stderr);
  Deno.exit(1);
}
const jsonStart = output.indexOf("{");
const jsonText = output.slice(jsonStart);

interface BenchResult {
  name: string;
  results: Array<{
    ok?: {
      n: number;
      min: number;
      max: number;
      avg: number;
      p75: number;
      p99: number;
      p995: number;
      p999: number;
    };
  }>;
}

interface BenchData {
  benches: BenchResult[];
}

// Configuration keys for avro-ts variants (deserialization-specific)
const AVRO_TS_CONFIGS = [
  { key: "fromSyncBuffer", label: "fromSyncBuffer", pattern: "avro-ts, fromSyncBuffer)" },
  { key: "readSync", label: "readSync", pattern: "avro-ts, readSync)" },
  { key: "read-async", label: "read async", pattern: "avro-ts, read async)" },
];

function formatTime(avgNs: number): string {
  if (avgNs < 1000) return `${avgNs.toFixed(1)} ns`;
  if (avgNs < 1000000) return `${(avgNs / 1000).toFixed(2)} µs`;
  return `${(avgNs / 1000000).toFixed(2)} ms`;
}

function getAvgNs(bench: BenchResult | undefined): number | null {
  return bench?.results[0]?.ok?.avg ?? null;
}

try {
  const benchData = JSON.parse(jsonText) as BenchData;

  // Group benchmarks by type (e.g., "primitive: null", "record: simple")
  const groups = new Map<string, Map<string, BenchResult>>();

  for (const bench of benchData.benches) {
    // Extract the group name (everything before the library/config part)
    const match = bench.name.match(/^(.+?)\s*\(/);
    if (!match) continue;

    const groupName = match[1].trim();
    if (!groups.has(groupName)) {
      groups.set(groupName, new Map());
    }

    // Determine the config key
    let configKey = "other";
    if (bench.name.includes("(avsc)")) {
      configKey = "avsc";
    } else if (bench.name.includes("(avro-js)")) {
      configKey = "avro-js";
    } else {
      for (const config of AVRO_TS_CONFIGS) {
        if (bench.name.includes(config.pattern)) {
          configKey = config.key;
          break;
        }
      }
    }

    groups.get(groupName)!.set(configKey, bench);
  }

  console.log("\n# Deserialize (Read) Benchmark Results - Cross-Library Comparison\n");
  console.log(`Generated on ${new Date().toISOString()}\n`);

  // Build header
  const headers = [
    "Type",
    "avsc",
    "avro-js",
    ...AVRO_TS_CONFIGS.map((c) => c.label),
    "Best avro-ts",
    "Best Config",
    "vs avsc",
  ];
  console.log(`| ${headers.join(" | ")} |`);
  console.log(`| ${headers.map(() => "---").join(" | ")} |`);

  // Sort groups by category then name
  const sortedGroups = Array.from(groups.entries()).sort((a, b) => {
    const order = ["primitive:", "complex:", "record:", "array-of-records:", "array-of-arrays:"];
    const aOrder = order.findIndex((p) => a[0].startsWith(p));
    const bOrder = order.findIndex((p) => b[0].startsWith(p));
    if (aOrder !== bOrder) return aOrder - bOrder;
    return a[0].localeCompare(b[0]);
  });

  for (const [groupName, configMap] of sortedGroups) {
    const avscTime = getAvgNs(configMap.get("avsc"));
    const avroJsTime = getAvgNs(configMap.get("avro-js"));

    // Find best avro-ts config
    let bestConfig: { key: string; label: string; time: number } | null = null;
    const avroTsTimes: (string | null)[] = [];

    for (const config of AVRO_TS_CONFIGS) {
      const bench = configMap.get(config.key);
      const time = getAvgNs(bench);
      avroTsTimes.push(time !== null ? formatTime(time) : "-");

      if (time !== null && (bestConfig === null || time < bestConfig.time)) {
        bestConfig = { key: config.key, label: config.label, time };
      }
    }

    // Calculate speedup vs avsc
    let vsAvsc = "-";
    if (bestConfig && avscTime) {
      const ratio = avscTime / bestConfig.time;
      if (ratio >= 1) {
        vsAvsc = `**${ratio.toFixed(2)}x faster**`;
      } else {
        vsAvsc = `${(1 / ratio).toFixed(2)}x slower`;
      }
    }

    const row = [
      `**${groupName}**`,
      avscTime !== null ? formatTime(avscTime) : "-",
      avroJsTime !== null ? formatTime(avroJsTime) : "-",
      ...avroTsTimes,
      bestConfig ? formatTime(bestConfig.time) : "-",
      bestConfig ? bestConfig.label : "-",
      vsAvsc,
    ];

    console.log(`| ${row.join(" | ")} |`);
  }

  // Summary statistics
  console.log("\n## Summary\n");

  let fasterCount = 0;
  let slowerCount = 0;
  let totalSpeedup = 0;

  for (const [, configMap] of groups) {
    const avscTime = getAvgNs(configMap.get("avsc"));
    if (!avscTime) continue;

    let bestTime: number | null = null;
    for (const config of AVRO_TS_CONFIGS) {
      const time = getAvgNs(configMap.get(config.key));
      if (time !== null && (bestTime === null || time < bestTime)) {
        bestTime = time;
      }
    }

    if (bestTime) {
      const ratio = avscTime / bestTime;
      totalSpeedup += ratio;
      if (ratio >= 1) fasterCount++;
      else slowerCount++;
    }
  }

  const benchmarksWithAvsc = Array.from(groups.values()).filter((m) =>
    m.has("avsc")
  ).length;

  console.log(`- **Benchmarks compared**: ${benchmarksWithAvsc}`);
  console.log(
    `- **avro-ts faster than avsc**: ${fasterCount}/${benchmarksWithAvsc}`
  );
  console.log(
    `- **avro-ts slower than avsc**: ${slowerCount}/${benchmarksWithAvsc}`
  );
  if (benchmarksWithAvsc > 0) {
    console.log(
      `- **Average speedup (best config)**: ${(totalSpeedup / benchmarksWithAvsc).toFixed(2)}x`
    );
  }

  // =============================================================================
  // Array Depth Comparison Section
  // =============================================================================
  const arrayDepthCategories = ["array-of-records:", "array-of-arrays:"];
  const depthGroups = ["depth 1", "depth 2", "depth 3", "depth 4"];

  for (const category of arrayDepthCategories) {
    const categoryGroups = sortedGroups.filter(([name]) => name.startsWith(category));

    if (categoryGroups.length > 0) {
      const categoryTitle = category.replace(":", "").replace(/-/g, " ").trim();
      console.log(`\n## ${categoryTitle.charAt(0).toUpperCase() + categoryTitle.slice(1)} - Depth Comparison\n`);

      const depthHeaders = ["Config", ...depthGroups];
      console.log(`| ${depthHeaders.join(" | ")} |`);
      console.log(`| ${depthHeaders.map(() => "---").join(" | ")} |`);

      // Build a map from depth index to configMap for easy lookup
      const depthToConfigMap = new Map<number, Map<string, BenchResult>>();
      for (const [groupName, configMap] of categoryGroups) {
        const depth = groupName.replace(category, "").trim();
        const depthIndex = depthGroups.indexOf(depth);
        if (depthIndex !== -1) {
          depthToConfigMap.set(depthIndex, configMap);
        }
      }

      // Build rows for each config (avsc, avro-js, and all avro-ts configs)
      const allConfigs = [
        { key: "avsc", label: "avsc" },
        { key: "avro-js", label: "avro-js" },
        ...AVRO_TS_CONFIGS.map((c) => ({ key: c.key, label: c.label })),
      ];

      for (const config of allConfigs) {
        const row = [config.label];
        for (let i = 0; i < depthGroups.length; i++) {
          const configMap = depthToConfigMap.get(i);
          const time = configMap ? getAvgNs(configMap.get(config.key)) : null;
          row.push(time !== null ? formatTime(time) : "-");
        }
        console.log(`| ${row.join(" | ")} |`);
      }

      // Add best avro-ts row
      const bestRow = ["**Best avro-ts**"];
      const bestTimes: (number | null)[] = [];
      for (let i = 0; i < depthGroups.length; i++) {
        const configMap = depthToConfigMap.get(i);
        let bestTime: number | null = null;
        if (configMap) {
          for (const config of AVRO_TS_CONFIGS) {
            const time = getAvgNs(configMap.get(config.key));
            if (time !== null && (bestTime === null || time < bestTime)) {
              bestTime = time;
            }
          }
        }
        bestTimes.push(bestTime);
        bestRow.push(bestTime !== null ? `${formatTime(bestTime)}` : "-");
      }
      console.log(`| ${bestRow.join(" | ")} |`);

      // Add speedup row vs avsc
      const speedupRow = ["**vs avsc**"];
      for (let i = 0; i < depthGroups.length; i++) {
        const configMap = depthToConfigMap.get(i);
        const avscTime = configMap ? getAvgNs(configMap.get("avsc")) : null;
        const bestTime = bestTimes[i];
        if (avscTime && bestTime) {
          const ratio = avscTime / bestTime;
          if (ratio >= 1) {
            speedupRow.push(`**${ratio.toFixed(2)}x faster**`);
          } else {
            speedupRow.push(`${(1 / ratio).toFixed(2)}x slower`);
          }
        } else {
          speedupRow.push("-");
        }
      }
      console.log(`| ${speedupRow.join(" | ")} |`);
    }
  }

  console.log("\n### Configuration Legend\n");
  console.log("| Abbreviation | Full Configuration |");
  console.log("| --- | --- |");
  console.log("| fromSyncBuffer | `type.fromSyncBuffer(buffer)` - convenience method that creates tap internally |");
  console.log("| readSync | `type.readSync(tap)` - manual tap setup with `SyncInMemoryReadableBuffer` + `SyncReadableTap` |");
  console.log("| read async | `await type.read(tap)` - async read with `InMemoryReadableBuffer` + `ReadableTap` |");

  console.log("\n### Notes\n");
  console.log("- avsc and avro-js use `type.fromBuffer(buffer)` for deserialization");
  console.log("- avro-typescript read performance compared to write performance tends to be slower due to buffer abstraction overhead");
  console.log("- The `fromSyncBuffer` method is generally the fastest avro-ts option as it has the least overhead");

  console.log("\n Benchmark completed successfully!");
} catch (error) {
  console.error("Failed to parse benchmark results:", error);
  console.log("Raw output:", output);
  Deno.exit(1);
}
