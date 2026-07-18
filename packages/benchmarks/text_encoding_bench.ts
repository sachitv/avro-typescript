#!/usr/bin/env -S deno bench --allow-read --no-config

/**
 * Benchmark: Uint8Array to String UTF-8 Conversion
 *
 * Compares exactly 3 methods:
 * 1. Buffer.toString('utf8') - Node.js native
 * 2. String.fromCharCode manual loop - Pure JS ASCII fast-path
 * 3. TextDecoder - Web standard API
 *
 * Goal: Find the crossover points where each method wins.
 */

import { Buffer } from "node:buffer";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function fromCharCodeDecode(arr: Uint8Array, pos: number, len: number): string {
  const end = pos + len;
  let output = "";

  while (pos + 3 < end) {
    const a = arr[pos], b = arr[pos + 1], c = arr[pos + 2], d = arr[pos + 3];
    if ((a | b | c | d) & 0x80) {
      return output + textDecoder.decode(arr.subarray(pos, end));
    }
    output += String.fromCharCode(a, b, c, d);
    pos += 4;
  }

  while (pos < end) {
    const char = arr[pos];
    if (char & 0x80) {
      return output + textDecoder.decode(arr.subarray(pos, end));
    }
    output += String.fromCharCode(char);
    pos++;
  }

  return output;
}

function generateAsciiString(length: number): string {
  let result = "";
  for (let i = 0; i < length; i++) {
    result += String.fromCharCode(65 + (i % 26));
  }
  return result;
}

const BENCH_ITERATIONS = 10000;

// =============================================================================
// FINE-GRAINED SIZE COMPARISON (ASCII only - to find crossover points)
// =============================================================================

const sizes = [
  4,
  8,
  12,
  16,
  20,
  24,
  28,
  32,
  40,
  48,
  56,
  64,
  80,
  96,
  112,
  128,
  160,
  192,
  224,
  256,
  320,
  384,
  448,
  512,
  640,
  768,
  896,
  1024,
  1536,
  2048,
  3072,
  4096,
  6144,
  8192,
  12288,
  16384,
];

for (const size of sizes) {
  const str = generateAsciiString(size);
  const bytes = textEncoder.encode(str);
  const buffer = Buffer.from(bytes);

  Deno.bench({
    name: `Buffer.toString - ${size}`,
    group: `size-${size}`,
    baseline: true,
    n: BENCH_ITERATIONS,
  }, () => {
    buffer.toString("utf8");
  });

  Deno.bench({
    name: `fromCharCode - ${size}`,
    group: `size-${size}`,
    n: BENCH_ITERATIONS,
  }, () => {
    fromCharCodeDecode(bytes, 0, size);
  });

  Deno.bench({
    name: `TextDecoder - ${size}`,
    group: `size-${size}`,
    n: BENCH_ITERATIONS,
  }, () => {
    textDecoder.decode(bytes);
  });
}
