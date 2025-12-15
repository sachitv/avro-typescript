#!/usr/bin/env -S deno run --allow-read --allow-write

/**
 * Script to run serialize single record benchmark and generate a focused report
 */

// Using Deno's built-in TextDecoder

// Run the serialize single record benchmark
const benchCommand = new Deno.Command("deno", {
  args: [
    "bench",
    "--no-config",
    "--allow-read",
    "--allow-write",
    "--json",
    "serialize_single_bench.ts"
  ],
  stdout: "piped",
  stderr: "piped",
});

console.log("Running serialize single record benchmark...");

const result = await benchCommand.output();
const decoder = new TextDecoder();

if (!result.success) {
  const stderr = decoder.decode(result.stderr);
  console.error(`Benchmark failed: ${stderr}`);
  Deno.exit(1);
}

const output = decoder.decode(result.stdout);
const jsonStart = output.indexOf('{');
const jsonText = output.slice(jsonStart);

try {
  const benchData = JSON.parse(jsonText) as {
    benches: Array<{
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
    }>;
  };

  console.log("\n# Serialize Single Record Benchmark Results\n");
  console.log(`Generated on ${new Date().toISOString()}\n`);

  console.log("| Benchmark | Avg Time | Iter/s | Min | Max | P75 | P99 |");
  console.log("| --- | --- | --- | --- | --- | --- | --- |");

  for (const bench of benchData.benches) {
    if (bench.results[0]?.ok) {
      const ok = bench.results[0].ok;
      const avgNs = ok.avg;
      const avgTime = avgNs < 1000 ? `${(avgNs).toFixed(1)} ns` :
                     avgNs < 1000000 ? `${(avgNs / 1000).toFixed(1)} µs` :
                     `${(avgNs / 1000000).toFixed(1)} ms`;
      const iterPerSec = (1000000000 / avgNs).toLocaleString();

      console.log(`| ${bench.name} | ${avgTime} | ${iterPerSec}/s | ${(ok.min / 1000).toFixed(1)} µs | ${(ok.max / 1000).toFixed(1)} µs | ${(ok.p75 / 1000).toFixed(1)} µs | ${(ok.p99 / 1000).toFixed(1)} µs |`);
    }
  }

  console.log("\nBenchmark completed successfully!");

} catch (error) {
  console.error("Failed to parse benchmark results:", error);
  console.log("Raw output:", output);
  Deno.exit(1);
}