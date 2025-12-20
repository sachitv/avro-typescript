import { fromFileUrl, join } from "jsr:@std/path@^1.0.6";
import type { BenchmarkLibrary } from "./library_targets.ts";

const BENCH_DIR = fromFileUrl(new URL(".", import.meta.url));
const OUTPUT_FILE = join(BENCH_DIR, "benchmark_report.md");
const BENCH_ARGS = ["bench", "--no-config", "--allow-read", "--allow-write", "--json"];
const LIBRARIES: BenchmarkLibrary[] = ["avro-typescript", "avsc", "avro-js"];
const LIBRARY_LABELS: Record<BenchmarkLibrary, string> = {
  "avro-typescript": "Avro TypeScript",
  "avsc": "avsc",
  "avro-js": "avro-js",
};

type BenchStats = {
  avgNs: number;
  iterPerSec: number;
};

const decoder = new TextDecoder();
const encoder = new TextEncoder();

const benchCommand = new Deno.Command(Deno.execPath(), {
  args: BENCH_ARGS,
  cwd: BENCH_DIR,
});

const result = await benchCommand.output();
if (!result.success) {
  const stderr = decoder.decode(result.stderr);
  throw new Error(
    `Benchmark run failed with exit code ${result.code}.\n${stderr}`,
  );
}

const jsonText = decoder.decode(result.stdout);
const benchData = JSON.parse(jsonText) as {
  benches: Array<{
    name: string;
    results: Array<{ ok?: { avg: number; n: number } }>;
  }>;
};

const scenarioOrder: string[] = [];
const scenarioMatrix = new Map<string, Map<BenchmarkLibrary, BenchStats>>();

for (const bench of benchData.benches ?? []) {
  const stats = bench.results?.[0]?.ok;
  if (!stats) {
    continue;
  }

  const { scenario, library } = parseBenchName(bench.name);
  const resolvedLibrary = library ?? "avro-typescript";
  if (!scenarioMatrix.has(scenario)) {
    scenarioMatrix.set(scenario, new Map());
    scenarioOrder.push(scenario);
  }
  scenarioMatrix.get(scenario)!.set(resolvedLibrary, {
    avgNs: stats.avg,
    iterPerSec: stats.avg > 0 ? 1_000_000_000 / stats.avg : 0,
  });
}

const markdown = [
  "# Benchmark Comparison",
  "",
  `Generated on ${new Date().toISOString()}`,
  "",
  `Command: \`deno ${BENCH_ARGS.join(" ")}\``,
  "",
  ...scenarioOrder.flatMap((scenario) =>
    formatScenarioSection(
      scenario,
      scenarioMatrix.get(scenario)!,
    )
  ),
  "_Times use average per-iteration wall-clock duration. Rates are derived from the recorded averages._",
  "",
].join("\n");

await Deno.writeFile(OUTPUT_FILE, encoder.encode(markdown));
console.log(`Benchmark report written to ${OUTPUT_FILE}`);

function parseBenchName(name: string): {
  scenario: string;
  library?: BenchmarkLibrary;
} {
  const bracketMatch = name.match(/\[([^\]]+)\]\s*$/);
  if (bracketMatch) {
    const library = normalizeLibrary(bracketMatch[1]);
    if (library) {
      return {
        scenario: name.slice(0, bracketMatch.index).trim(),
        library,
      };
    }
  }

  const parenMatch = name.match(/\(([^()]+)\)\s*$/);
  if (parenMatch) {
    const library = normalizeLibrary(parenMatch[1]);
    if (library) {
      return {
        scenario: name.slice(0, parenMatch.index).trim(),
        library,
      };
    }
  }

  return { scenario: name.trim() };
}

function normalizeLibrary(label: string): BenchmarkLibrary | undefined {
  const normalized = label.trim().toLowerCase().replace(/\s+/g, "-");
  if (normalized === "avro-typescript") {
    return "avro-typescript";
  }
  if (normalized === "avsc") {
    return "avsc";
  }
  if (normalized === "avro-js") {
    return "avro-js";
  }
  return undefined;
}

function formatScenarioSection(
  scenario: string,
  data: Map<BenchmarkLibrary, BenchStats>,
): string[] {
  const lines = [
    `## ${scenario}`,
    "",
    "| Library | Avg Time | Iter/s |",
    "| --- | --- | --- |",
    ...LIBRARIES.map((lib) => {
      const stats = data.get(lib);
      if (!stats) {
        return `| ${LIBRARY_LABELS[lib]} | — | — |`;
      }
      return `| ${LIBRARY_LABELS[lib]} | ${formatDuration(stats.avgNs)} | ${formatRate(stats.iterPerSec)} |`;
    }),
    "",
  ];
  return lines;
}

function formatDuration(ns: number): string {
  if (ns >= 1_000_000_000) {
    return `${(ns / 1_000_000_000).toFixed(1)} s`;
  }
  if (ns >= 1_000_000) {
    return `${(ns / 1_000_000).toFixed(1)} ms`;
  }
  if (ns >= 1_000) {
    return `${(ns / 1_000).toFixed(1)} µs`;
  }
  return `${ns.toFixed(1)} ns`;
}

function formatRate(rate: number): string {
  if (!Number.isFinite(rate) || rate <= 0) {
    return "n/a";
  }
  if (rate >= 1_000_000) {
    return `${(rate / 1_000_000).toFixed(1)}M/s`;
  }
  if (rate >= 1_000) {
    return `${(rate / 1_000).toFixed(1)}k/s`;
  }
  return `${rate.toFixed(1)}/s`;
}
