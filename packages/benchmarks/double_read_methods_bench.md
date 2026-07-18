# Double/Float Read Methods Benchmark

This benchmark compares different methods of reading IEEE 754 floating-point values from binary data.

## Purpose

Compare the performance of:
- `Buffer.readDoubleLE()` / `Buffer.readFloatLE()` (Node.js API used by avsc and avro-js)
- `DataView.getFloat64()` / `DataView.getFloat32()` (Web API used by avro-typescript)

## Running the Benchmark

```bash
deno bench --no-config --allow-read double_read_methods_bench.ts
```

## Results Summary

### Single Value Reads

**Double (64-bit):**
- DataView.getFloat64: **1.62x faster** than Buffer.readDoubleLE
- Time: ~3.7ns vs ~6.0ns

**Float (32-bit):**
- DataView.getFloat32: **1.38x faster** than Buffer.readFloatLE
- Time: ~3.6ns vs ~5.0ns

### Sequential Reads (100 values)

**Double (64-bit):**
- DataView.getFloat64: **21x faster** than Buffer.readDoubleLE
- Time: ~59ns vs ~1.3µs per iteration

### DataView Creation Overhead

Creating a new DataView on each read adds significant overhead:
- With reused DataView: ~3.7ns
- With new DataView: ~49.7ns (**13x slower**)

This shows why DirectSyncReadableTap creates the DataView once in the constructor.

## Key Findings

1. **DataView is faster than Buffer** for reading floats/doubles in Deno
2. **Reusing DataView instances is critical** for performance
3. **Sequential reads amplify the difference** - the gap grows from 1.6x to 21x
4. The pattern holds for both float (32-bit) and double (64-bit) operations

## Implications for avro-typescript

The DirectSyncReadableTap approach of creating a DataView once in the constructor and reusing it for all reads is optimal. This benchmark validates that design decision.

The performance advantage over Buffer-based approaches (avsc/avro-js) in Deno comes from:
- DataView's lower per-read overhead
- One-time DataView allocation in the constructor
- No Buffer compatibility layer overhead
