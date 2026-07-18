# Large Heterogeneous Record Deserialization

Date: 2026-07-17\
Runtime: Deno 2.9.2, V8 14.9, Apple arm64

This suite complements the small single-value benchmarks with deterministic
batches containing 2,048-6,144 top-level records. It deliberately varies record
width, field types, nesting, and whether `ArrayType` can use the compiled-record
block path.

## Method

- Schema construction and serialization happen before the timed region.
- Every timed operation decodes one complete Avro array.
- The exact closure later passed to `Deno.bench` decodes at least 100,000
  records first, giving V8 a meaningful steady-state warmup.
- Each configuration measures at least 500,000 records after warmup.
- The report runs each workload in an isolated process three times and reports
  the median p75. The raw benchmark task runs all workloads in one process to
  expose cross-schema tiering and GC effects.
- All libraries decode the same bytes, serialized once by avro-typescript.

## Workloads

| Workload                  | Batch | Shape                                                                         |
| ------------------------- | ----: | ----------------------------------------------------------------------------- |
| Numeric telemetry         | 4,096 | Five fields: int, bigint long, float, double, and boolean                     |
| Text and binary           | 4,096 | Four fields: enum, short/long/Unicode strings, and variable bytes             |
| Nested collections        | 2,048 | Four outer fields containing a nested record, float array, and string map     |
| Optional and fixed        | 4,096 | Six fields with nullable unions, enum, fixed/variable bytes, long, and double |
| Wide business records     | 4,096 | Twelve mixed fields, intentionally using the wide-record scalar fallback      |
| Heterogeneous event union | 6,144 | Mixed array cycling through three record branches with 3-5 fields             |

## Representative results

These are median p75 values from three isolated process runs.

| Workload                  |      avsc |   avro-js | avro-ts fromSyncBuffer | avro-ts direct reused | Best avro-ts vs avsc |
| ------------------------- | --------: | --------: | ---------------------: | --------------------: | -------------------: |
| Numeric telemetry         | 157.42 us | 188.71 us |              431.38 us |             420.13 us |         2.67x slower |
| Text and binary           |   1.36 ms |   1.93 ms |                1.09 ms |               1.10 ms |         1.25x faster |
| Nested collections        |   1.50 ms |   1.45 ms |              871.92 us |             870.92 us |         1.72x faster |
| Optional and fixed        |   1.59 ms |   2.93 ms |                1.11 ms |               1.13 ms |         1.43x faster |
| Wide business records     |   2.43 ms |   3.93 ms |                1.61 ms |               1.84 ms |         1.51x faster |
| Heterogeneous event union |   1.15 ms |   1.13 ms |                1.21 ms |               1.19 ms |         1.04x slower |

| Workload                  |              avsc |           avro-js | avro-ts fromSyncBuffer | avro-ts direct reused |
| ------------------------- | ----------------: | ----------------: | ---------------------: | --------------------: |
| Numeric telemetry         | 26.02 M records/s | 21.71 M records/s |       9.50 M records/s |      9.75 M records/s |
| Text and binary           |  3.02 M records/s |  2.12 M records/s |       3.76 M records/s |      3.74 M records/s |
| Nested collections        |  1.37 M records/s |  1.41 M records/s |       2.35 M records/s |      2.35 M records/s |
| Optional and fixed        |  2.58 M records/s |  1.40 M records/s |       3.70 M records/s |      3.61 M records/s |
| Wide business records     |  1.69 M records/s |  1.04 M records/s |       2.54 M records/s |      2.22 M records/s |
| Heterogeneous event union |  5.36 M records/s |  5.45 M records/s |       5.07 M records/s |      5.15 M records/s |

## Focused union results

The isolated sweep also measures three batched union shapes. These numbers use
the same post-warmup, 300 ms repetition calibration for every variant.

| Workload              | Values |     avsc |  avro-js | avro-ts fromSyncBuffer | avro-ts direct reused | Best avro-ts vs avsc |
| --------------------- | -----: | -------: | -------: | ---------------------: | --------------------: | -------------------: |
| Null branch only      |  1,024 |  9.68 us |  5.50 us |                4.32 us |               4.43 us |         2.24x faster |
| Mixed primitives      |    768 | 31.65 us | 30.00 us |               29.86 us |              30.28 us |         1.06x faster |
| Mixed record branches |    768 | 69.91 us | 73.05 us |               99.31 us |              98.17 us |         1.40x slower |

The small record-union case magnifies output-representation differences:
avro-typescript allocates the required branch wrapper and returns exact `bigint`
long values, while both competitors return unwrapped values and numbers. The
larger heterogeneous workload amortizes that work and has varied between 1.02x
faster and 1.04x slower than avsc across isolated reports, so it should be
treated as parity rather than a stable win or loss.

## Findings

- The record-block optimization is not a universal proxy for performance. It
  helps flat records, but field semantics and child types can dominate large
  batches.
- Numeric telemetry highlights the cost of avro-typescript's exact `bigint` long
  semantics; avsc and avro-js decode these safe-range values as numbers.
- Text and binary is a clear avro-typescript strength, consistent with the
  dedicated string and byte-path benchmarks.
- The nullable workload moved from 1.41x slower to 1.43x faster than avsc after
  union indices stopped taking the `BigInt` path.
- Nested collections and wide records now lead both competitors. The nested
  workload returns `Map` values where the competitors return plain objects, so
  this is not identical output work.
- Heterogeneous unions moved from 2.19x slower to near parity by combining fast
  discriminant reads, cheaper wrapper construction, and direct dispatch to
  compiled branch-record readers.
- Running all workloads in one process is slower and noisier for some nested and
  union cases than isolated reporting. That mode is useful for testing realistic
  multi-schema optimization feedback rather than publishing a single headline
  ratio.

## Commands

```sh
# Stable comparison table: three isolated runs per workload.
deno task bench:deserialize:large:report

# All heterogeneous schemas in one process.
deno task bench:deserialize:large

# One raw workload.
AVRO_LARGE_WORKLOAD=heterogeneous-events deno task bench:deserialize:large

# Selected isolated workloads with a custom repeat count.
deno task bench:deserialize:large:report -- --repeats=5 numeric-telemetry wide-business-records
```

Available workload IDs are `numeric-telemetry`, `text-and-binary`,
`nested-collections`, `optional-and-fixed`, `wide-business-records`, and
`heterogeneous-events`.
