#!/usr/bin/env -S deno run --allow-read

// Run benchmarks - this file imports all benchmark modules
// Run with: deno bench --allow-read run_benchmarks.ts

import "./serialization_bench.ts";
import "./schema_bench.ts";
import "./file_sync_bench.ts";

// Note: This file is meant to be run with `deno bench`, not `deno run`
