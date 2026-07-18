#!/usr/bin/env -S deno bench --allow-read --no-config

import { Buffer } from "node:buffer";

const DOUBLE_VALUE = 3.141592653589793;

const arrayBuffer = new ArrayBuffer(8);
const dataView = new DataView(arrayBuffer);
dataView.setFloat64(0, DOUBLE_VALUE, true);

const uint8Array = new Uint8Array(arrayBuffer);
const nodeBuffer = Buffer.from(uint8Array);

const BENCH_ITERATIONS = 100000;

Deno.bench({
  name: "Buffer.readDoubleLE (baseline - avsc/avro-js approach)",
  group: "double-read-methods",
  baseline: true,
  n: BENCH_ITERATIONS,
}, () => {
  nodeBuffer.readDoubleLE(0);
});

Deno.bench({
  name: "DataView.getFloat64 (avro-typescript approach)",
  group: "double-read-methods",
  n: BENCH_ITERATIONS,
}, () => {
  dataView.getFloat64(0, true);
});

Deno.bench({
  name: "DataView.getFloat64 (with new DataView creation)",
  group: "double-read-methods",
  n: BENCH_ITERATIONS,
}, () => {
  const view = new DataView(
    uint8Array.buffer,
    uint8Array.byteOffset,
    uint8Array.byteLength,
  );
  view.getFloat64(0, true);
});

Deno.bench({
  name: "Manual bytes (theoretical minimum)",
  group: "double-read-methods",
  n: BENCH_ITERATIONS,
}, () => {
  const b0 = uint8Array[0];
  const b1 = uint8Array[1];
  const b2 = uint8Array[2];
  const b3 = uint8Array[3];
  const b4 = uint8Array[4];
  const b5 = uint8Array[5];
  const b6 = uint8Array[6];
  const b7 = uint8Array[7];
  const sum = b0! + b1! + b2! + b3! + b4! + b5! + b6! + b7!;
  if (sum === -1) throw new Error();
});

const ARRAY_SIZE = 100;
const sequentialBuffer = new ArrayBuffer(ARRAY_SIZE * 8);
const sequentialView = new DataView(sequentialBuffer);
const sequentialUint8 = new Uint8Array(sequentialBuffer);
const sequentialNodeBuffer = Buffer.from(sequentialUint8);

for (let i = 0; i < ARRAY_SIZE; i++) {
  sequentialView.setFloat64(i * 8, i * Math.PI, true);
}

Deno.bench({
  name: "Buffer.readDoubleLE (sequential 100 doubles)",
  group: "double-sequential-reads",
  baseline: true,
  n: BENCH_ITERATIONS / 100,
}, () => {
  let sum = 0;
  for (let i = 0; i < ARRAY_SIZE; i++) {
    sum += sequentialNodeBuffer.readDoubleLE(i * 8);
  }
  if (sum === Infinity) throw new Error();
});

Deno.bench({
  name: "DataView.getFloat64 (sequential 100 doubles)",
  group: "double-sequential-reads",
  n: BENCH_ITERATIONS / 100,
}, () => {
  let sum = 0;
  for (let i = 0; i < ARRAY_SIZE; i++) {
    sum += sequentialView.getFloat64(i * 8, true);
  }
  if (sum === Infinity) throw new Error();
});

Deno.bench({
  name: "DataView.getFloat64 (advancing position, DirectTap style)",
  group: "double-sequential-reads",
  n: BENCH_ITERATIONS / 100,
}, () => {
  let sum = 0;
  let pos = 0;
  for (let i = 0; i < ARRAY_SIZE; i++) {
    sum += sequentialView.getFloat64(pos, true);
    pos += 8;
  }
  if (sum === Infinity) throw new Error();
});

const floatBuffer = new ArrayBuffer(4);
const floatDataView = new DataView(floatBuffer);
floatDataView.setFloat32(0, 3.14159, true);

const floatUint8 = new Uint8Array(floatBuffer);
const floatNodeBuffer = Buffer.from(floatUint8);

Deno.bench({
  name: "Buffer.readFloatLE (baseline)",
  group: "float-read-methods",
  baseline: true,
  n: BENCH_ITERATIONS,
}, () => {
  floatNodeBuffer.readFloatLE(0);
});

Deno.bench({
  name: "DataView.getFloat32 (avro-typescript approach)",
  group: "float-read-methods",
  n: BENCH_ITERATIONS,
}, () => {
  floatDataView.getFloat32(0, true);
});
