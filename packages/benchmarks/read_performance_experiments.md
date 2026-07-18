# Read Performance Experiments: Record Composition

Date: 2026-07-09\
Runtime: Deno 2.9.2, V8 14.9, Apple arm64\
Baseline: the tracked follow-on performance snapshot rooted at local merge
`28ea025` (including the short-string fast path and 1,000-iteration benchmark
configuration).

## Method

Five independent worktrees were created from the same baseline. Implementation
and correctness work ran in parallel, but all final benchmarks ran sequentially
to avoid CPU contention.

The primary comparison is the median p75 from four alternating baseline/variant
runs. Each run used 1,000 benchmark iterations. Positive percentages below mean
the variant was faster. Separate depth sweeps measured `array<Record>` at depths
1-4. Early concurrently collected timings were discarded.

## Summary

| # | Experiment                                                         | Target workload      | fromSyncBuffer | DirectTap reused |                         Source cost | Recommendation                                                  |
| - | ------------------------------------------------------------------ | -------------------- | -------------: | ---------------: | ----------------------------------: | --------------------------------------------------------------- |
| 1 | Cache generic `itemsType.readSync` closure                         | `array<Record>`      |          +3.5% |            -1.0% |                          +3 net LOC | Reject: noise-level at shallow depths and 19% slower at depth 4 |
| 2 | Cache and directly call compiled record reader                     | `array<Record>`      |         +11.4% |            +9.9% |               +38 LOC, +30 test LOC | Best safe starting point; refine API/realm handling             |
| 3 | Fuse array loop and record construction with generated code        | `array<Record>`      |         +22.0% |           +25.2% |                   +68 LOC, no tests | Performance ceiling only; prototype is not safe to merge        |
| 4 | Return actual nested reader after placeholder-assisted compilation | Nested records       |          +2.2% |            +0.5% | 2 return-line changes, +90 test LOC | Optional correctness/cleanup patch, not a performance priority  |
| 5 | Compile enum/fixed/array/map/union record fields                   | Complex-field record |          +8.2% |            +5.0% |      +130 source LOC, +148 test LOC | Too much duplicated decode logic for the measured gain          |

## Safe hybrid implementation

The production-oriented hybrid was implemented on 2026-07-10. It keeps the
direct compiled-reader foundation from experiment 2 and adds a static fused
block-reader contract to `RecordReaderStrategy`. It uses no runtime source
generation. `ArrayType` discovers the capability through a versioned symbol,
and the cache falls back to the assembled scalar reader for custom/interpreted
strategies, records wider than five fields, and the special `__proto__` field.

Four alternating runs of the canonical ten-record workload produced these
median p75 values. Lower is better.

| Variant                    | fromSyncBuffer | Direct tap, reused | Direct latency vs baseline |
| -------------------------- | -------------: | -----------------: | -------------------------: |
| Baseline                   |       977.4 ns |           897.8 ns |                          - |
| Approach 2 prototype       |       919.8 ns |           837.8 ns |                 6.7% lower |
| **Safe hybrid**            |   **782.4 ns** |       **715.7 ns** |            **20.3% lower** |
| Unsafe generated prototype |       772.1 ns |           706.7 ns |                21.3% lower |

The safe hybrid is about 14.6-14.9% lower latency than approach 2 and captures
roughly 95% of the baseline-to-generated-code opportunity. Dynamic assignment
was substantially faster than computed object literals. Hoisting every field
name and reader into a closure-local then removed the remaining captured-array
lookup. `__proto__` still uses a scalar fallback because assignment would invoke
the legacy prototype setter.

Isolated cross-library runs (three runs per depth, median p75) show that the
remaining gap depends strongly on collection depth and allocation/GC behavior:

| Array<Record> depth | avsc     | avro-js  | Safe fromSyncBuffer | Safe direct reused | Best safe vs avsc |
| ------------------- | -------: | -------: | ------------------: | -----------------: | ----------------: |
| 1                   | 0.56 us  | 0.56 us  |        0.79-1.21 us |       0.77-1.24 us |  1.4-2.2x slower  |
| 2                   | 5.88 us  | 9.03 us  |            11.71 us |            8.50 us |      1.45x slower |
| 3                   | 82.79 us | 87.83 us |            67.71 us |           73.71 us |      1.22x faster |
| 4                   | 591 us   | 643 us   |              876 us |             924 us |      1.48x slower |

Depth 1 is deliberately shown as a range: the safe runtime-keyed store site was
bimodal across fresh processes at the benchmark's short 1,000-iteration setting.
Longer steady-state targeted runs consistently selected the faster tier. The
depth results should not be averaged into one headline number: fresh-tap and
reused-tap configurations optimize differently at larger allocation sizes, and
mixed multi-group runs were observably distorted by tiering, deoptimization,
and GC.

### Why avsc and avro-js can still be faster

The benchmark uses avsc 5.7.9 and avro-js 1.12.1. Both libraries generate a
named record constructor and a schema-specific reader with `new Function`.
Their array loops remain generic, but each item becomes a call equivalent to
`new KnownRecord(type0._read(tap), ...)`, and the generated constructor writes
literal properties in a fixed order. This gives V8 a separate, monomorphic code
site and hidden class for every record schema.

The CSP-safe implementation cannot embed arbitrary Avro field names in source.
It therefore has a shared static closure with runtime keys. Its fused block loop
removes nearly all of the call overhead, but V8 can still tier that shared keyed
store less predictably during short runs. Nested arrays add another generic
`ArrayType.readSync` boundary and array allocation at every level, while large
depths amplify object-allocation and GC variance. Primitive decoding is not the
main issue here: isolated float, double, string, integer-array, and simple-record
paths are already competitive or faster.

## Array-of-record depth scaling

These are two-run p75 comparisons for the reusable direct tap. Depth 1 for the
generated variant was unstable in the depth sweep; its separate four-pair
microbenchmark measured a 25.2% improvement.

| Experiment                    | Depth 1 | Depth 2 | Depth 3 |    Depth 4 |
| ----------------------------- | ------: | ------: | ------: | ---------: |
| Generic cached closure        |   +0.6% |   +1.8% |    0.0% | **-19.0%** |
| Direct compiled record reader |   +7.0% |  +10.5% |   +9.4% |      +8.0% |
| Generated fused reader        |   noisy |  +25.6% |  +23.9% |     +20.2% |

## Relationship to avsc and avro-js

The experiments above are paired against the avro-typescript baseline because
that is the most stable way to isolate the code change. Competitor timings are
more variable when all schemas and configurations share one process, but the
uncontended targeted runs show the following practical position:

| Workload / variant                    | Relative to avsc and avro-js                                                                                     |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Current `array<Record>` baseline      | Usually about 1.5-1.9x slower                                                                                    |
| Generic cached closure                | Effectively unchanged; still about 1.5-1.9x slower                                                               |
| Direct compiled record reader         | Roughly 1.3-1.7x slower; occasional depth/configuration runs reach parity                                        |
| Generated fused reader                | Core-path projection is roughly 1.2-1.5x slower; some depths approach parity, but full mixed sweeps remain noisy |
| Placeholder cleanup on nested records | No meaningful change; nested records remain roughly 2x slower than avsc                                          |
| Complex-field compilation             | Closes 5-8% on the dedicated schema, but no equivalent canonical competitor row exists yet                       |

Outside the remaining record-composition gap, the optimized reusable direct tap
is already competitive:

- Float and double are faster than both libraries in isolated runs.
- The short/mixed-string batch is about 1.6-2.4x faster than avsc.
- Primitive integer arrays and simple records are generally faster.
- Nested arrays are at parity or modestly faster.
- Maps remain about 1.2-1.5x slower, partly because avro-typescript constructs a
  `Map` while the comparison libraries construct plain objects.

Record composition, especially records nested inside arrays, remains the most
variable synchronous workload. The implemented safe hybrid lowers canonical
latency by about one fifth and reaches within 1.3% of approach 3's unsafe
generated-code ceiling without accepting its correctness and deployment risks.

## Tradeoffs and findings

### 1. Generic cached item closure

This is the smallest change, but the existing virtual `RecordType.readSync`
entry remains. It did not improve the reusable direct-tap path and inhibited
optimization at depth 4. Caching a closure alone is not useful.

Worktree: `perf-1-cache-item`; one source file changed, full runtime suite and
targeted checks passed.

### 2. Direct compiled record reader

`ArrayType` detects record items once, caches the record's compiled sync reader,
and calls it directly for every element. This removes repeated
`RecordType.readSync`, lazy-field checks, and cache lookup. Improvements were
consistent across all depths.

The experiment exposes an internal-looking public accessor and uses
`instanceof RecordType`. A production version should use an internal symbol or
capability interface, use the existing record marker rather than `instanceof`,
and verify custom reader strategies and recursive arrays. The prototype's full
runtime suite passed: 195 tests / 2,709 steps.

Worktree: `perf-2-direct-record`.

### 3. Generated fused array/record reader

This establishes a useful performance ceiling: eliminating the scalar record
reader call and constructing each record inside the array loop is worth roughly
20-25%.

The prototype must not be merged:

- `new Function` requires `unsafe-eval` under browser CSP.
- It bypasses custom `RecordReaderStrategy.assembleSyncRecordReader` semantics.
- It demonstrably drops a valid Avro field named `__proto__` instead of creating
  an own property.
- One generated parameter per field risks engine limits for wide records.
- It has no focused tests and adds a second record-construction implementation.

The concept is still promising if implemented as a static block-reader contract
on `RecordReaderStrategy`, using computed properties and a loop/specialized
closure rather than runtime source generation.

Worktree: `perf-3-fused-array-record`.

### 4. Placeholder return cleanup

The cache still installs a placeholder before walking fields, so recursive
back-edges remain correct, but the initial acyclic caller receives the actual
assembled reader. Correctness and 100% coverage passed, including recursive and
acyclic identity tests.

Sequential measurements were effectively neutral. V8 likely inlines the small
placeholder closure, so this is defensible cleanup but not a meaningful speed
optimization.

Worktree: `perf-4-placeholder`.

### 5. Complex-field compilation

The strategy recursively specializes enum, fixed, non-record complex arrays,
non-primitive maps, and unions while preserving existing primitive bulk paths.
It avoids eval and public API changes. The dedicated complex record improved by
about 5-8%, but the existing dominant record benchmarks barely exercise these
shapes.

The cost is high: collection and union decoding control flow is duplicated in
the reader strategy, increasing parity and maintenance risk. Focused tests,
size-prefixed blocks, the full runtime suite, and diff checks passed. The gain
does not justify this broad patch before the array-record path is addressed.

Worktree: `perf-5-complex-fields`.

## Implemented design

The safe hybrid follows the recommended experiment-2-to-3 progression:

1. Add an internal compiled-reader capability for record types without widening
   the public API.
2. Let `ArrayType` resolve that reader once per schema and call it directly.
3. Add an optional static `assembleSyncRecordBlockReader` strategy method so
   custom strategies preserve their semantics.
4. Implement the default block reader with static closures and runtime-keyed
   assignments, no `new Function`.
5. Cover recursive records, custom/interpreted strategies, `__proto__`, empty
   and negative-count blocks, and wide records.

All five parts are implemented. The fused path specializes records with zero to
five fields; wider, special-name, and customized records retain approach 2's
compiled scalar fallback.

## Validation caveat

Local Deno 2.9.2 still reports six pre-existing `setTimeout` type errors in the
RPC WebSocket source/tests during the normal full type-checked suite.
Experiments used changed-file type checks plus the full runtime suite with
`--no-check`. The final runtime suite passes 195 tests / 2,719 steps. Deno 2.9.2
also fails to recover transpiled local TypeScript when generating its coverage
report, so the local 100% gate cannot produce a report on this runtime. The
already-merged baseline passed its normal 100% line/branch/function coverage
gate under the repository's Deno 2.6.6 CI version.

## 2026-07-10 follow-up: string decode, not record assembly, was the gap

A component ablation of the depth-1 `array<Record>` workload (10 records of
`{int, ~10-char string, double}`) overturned the record-composition theory
above. Driving the DirectSyncReadableTap primitives directly over the wire
bytes — with no array or record machinery at all — already cost 564 ns,
more than avsc's entire 545 ns `fromBuffer`. Swapping `readString` for
`skipString` dropped that to 63 ns. The 122 ns gap against avsc decomposed as:

| Component                        | avro-ts | avsc   | Delta   |
| -------------------------------- | ------: | -----: | ------: |
| String decode (10 strings)       | ~500 ns | ~366 ns | +134 ns |
| Array + record assembly          |  103 ns |  47 ns |  +56 ns |
| Varint + double primitives       |   63 ns | 132 ns |  -69 ns |

Record assembly was already close; primitives were already faster. Strings
were ~75% of the workload, and ~25 ns of each ~50 ns string decode was the
`subarray()` wrapper allocated per string, with most of the rest coming from
per-byte rope-string appends in the fromCharCode remainder loop.

### Implemented changes

1. `decodeUtf8Range(bytes, start, end)` in `text_encoding.ts`: the ASCII scan
   and chunked fromCharCode read directly from the backing buffer; a
   `subarray` is only allocated in the TextDecoder fallback (non-ASCII or
   >32 bytes). All five DirectSyncReadableTap string paths (readString,
   readStringArrayInto, both map block readers) use it.
2. The 0-7 byte fromCharCode remainder is now a single `String.fromCharCode`
   call selected by a switch, instead of one rope-string append per byte.
   This is worth more than the subarray fix on record workloads full of
   short field values (a 6-char name previously allocated 6 rope strings).
3. The fromCharCode/TextDecoder threshold moved from 32 to 48 bytes. The old
   value was tuned when the fast path paid a subarray allocation; the
   allocation-free range decode measured 64 vs 87 ns against TextDecoder at
   40 bytes with the crossover near 48.
4. `DirectSyncReadableTap` creates its DataView lazily; `fromSyncBuffer`'s
   per-call setup dropped from 63 ns to 20 ns.
5. The specialized scalar record readers in
   `CompiledReaderStrategy.#generateSpecializedSyncReader` now build records
   with dynamic assignment to a fresh object instead of computed-key object
   literals, mirroring the fused block readers. A V8 `--prof` capture of the
   nested-record workload attributed ~70% of total time to a runtime path
   called directly from the computed-literal closures, roughly 200 ns per
   nested record level. A flat-vs-nested experiment with identical field
   payloads confirmed it: nesting cost avsc 0 ns and avro-ts ~600 ns before
   the change, and ~78 ns after. Records with a `__proto__` field fall back
   to the loop reader, which preserves own-property semantics via
   `setRecordField`.
6. Benchmark methodology fixes in `deserialize_single_bench.ts`: iteration
   counts are time-based (the fixed 1,000-iteration runs ended before V8
   finished tiering and caused the depth-1 bimodality documented above), each
   avro-ts variant gets its own type instance, and the SyncReadableTap/async
   variants are defined after the DirectTap variants.

### Isolated sweep results

`isolated_sweep_bench.ts` runs each (workload, variant) pair in its own
process and prints median/best-of-5 reps after warmup. Post-change results:

| Workload      | avsc     | avro-js  | avrots-fsb | avrots-reused | Best vs avsc     |
| ------------- | -------: | -------: | ---------: | ------------: | ---------------- |
| depth 1       |   579 ns |   577 ns |     382 ns |    **312 ns** | **1.85x faster** |
| depth 2       |  5.62 us |  6.11 us |    4.36 us |   **4.19 us** | **1.34x faster** |
| depth 3       | 59.7 us  | 64.3 us  |   50.6 us  |  **50.7 us**  | **1.18x faster** |
| depth 4       |  582 us  |  631 us  |    513 us  |   **503 us**  | **1.16x faster** |
| nested-record |   709 ns |   813 ns |     670 ns |    **543 ns** | **1.31x faster** |

Every sweep workload — all four `array<Record>` depths and the nested-record
shape — is now faster than avsc and avro-js, and the depth-1 bimodality is
gone (rep spread 308-314 ns).

The nested-record shape was the last to fall. A field ablation attributed its
original ~270 ns gap as roughly 40-60 ns from the 41-char description
(recovered by the threshold move above), ~26 ns from the bytes field, and
~170-200 ns per record level in the computed-key object literals used by the
scalar specialized readers (change 5 above). Before that change, an identical
field payload cost 387 ns arranged flat and 989 ns nested across four record
levels; after it, 378 ns flat and 455 ns nested.

### Generalization review (2026-07-17)

A follow-up pass audited every optimization for benchmark-shape bias: the
sweep workloads were 3-field int/string/double records, so caps and constants
tuned against them risked overfitting. Three new sweep workloads guard
against that permanently: `array-wide8` and `array-wide16` (records with 8
and 16 mixed fields) and `heterogeneous` (an array of records mixing long,
boolean, float, enum, fixed, bytes, a nullable union, a string array, a map,
and a nested record).

Changes made:

1. The fused block reader's field cap rose from 5 (benchmark-shaped) to 10,
   matching the scalar specialization.
2. Records wider than 10 fields without `__proto__` now use a
   dynamic-assignment loop over a spread-cloned seed object (see
   `makeSeedRecord`) instead of paying a `setRecordField` call and
   `__proto__` string compare per field; only genuine `__proto__` schemas
   take that guarded path now.
3. Seed-cloning was also prototyped for the 1-10-field specialized readers
   and **reverted**: despite a 3x win in a synthetic store-only probe, in
   context it regressed depth-1 (312 to 382 ns) and depth-4 (503 to 617 us)
   because V8's cached transition walk is already cheap for narrow monomorphic
   shapes and the clone adds work. It stayed only where it measured faster
   (the >10-field loop).

The initial wide-record results (1.3-1.4x slower than avsc) briefly looked
like a structural CSP-vs-codegen limit. Deeper ablation found two concrete,
fixable causes instead:

4. **Field-name string internalization.** The wide fixtures build field names
   with template literals, which V8 does not internalize; every keyed store
   in the compiled readers then leaves the inline-cache fast path. This cost
   ~650 ns of the wide8 total and produced a bizarre order artifact (running
   avsc first "fixed" avro-ts, because avsc's generated source internalized
   the same names; avro-ts running twice did not). Source-literal and
   JSON.parse'd schema names are always internalized, which is why the
   depth/nested/heterogeneous workloads never showed it — but any
   programmatically built schema hits it. `RecordField` now internalizes its
   name at construction (`Object.keys({[name]: null})[0]`).
5. **Chunked wide-record readers.** For records wider than 10 fields, a
   dynamic `result[names[i]]` loop paid ~12 ns/field where hoisted
   constant-key stores pay ~4 ns/field. Wide readers now compose hoisted
   10-field chunk fillers over a spread-cloned seed object, keeping code size
   fixed for any width. (Seed-cloning was also prototyped for the 1-10-field
   readers and reverted: it regressed depth-1 312 to 382 ns and depth-4 503
   to 617 us; V8's cached transition walk is already cheap for narrow
   monomorphic shapes.)

Measured effects (avrots-reused, isolated processes; day-to-day machine
variance ~5%):

| Workload      | Before   | After    | avsc     | Position          |
| ------------- | -------: | -------: | -------: | ----------------- |
| array-wide8   |  1.99 us |  1.33 us |  1.52 us | **~1.15x faster** |
| array-wide16  |  4.59 us |  2.60 us |  2.81 us | **~1.08x faster** |
| heterogeneous | 10.64 us |  9.66 us | 11.20 us | **~1.15x faster** |
| depth1        |   312 ns |   327 ns |  ~580 ns | unchanged, faster |
| nested-record |   543 ns |   577 ns |  ~710 ns | unchanged, faster |

Wide16 ablation confirms where the remaining time goes: avro-ts primitives
decode the same wire bytes faster than avsc's (2.05 vs 2.59 us), so record
assembly is the entire difference, and the chunked readers brought assembly
from ~1.9 us down to competitive range. Every sweep workload — narrow, deep,
wide, and heterogeneous — is now faster than avsc and avro-js, with no
schema-shape-specific tuning: the specialization thresholds (10-field hoisting
cap, 10-field chunks, 48-byte string threshold) are mechanism boundaries, not
fixture shapes.

### Array-block allocation fix (2026-07-17, later)

Investigating the mixed suite's three "slower" groups in isolation (the
sweep gained `record-with-array`, `int-arrays-shallow`, and `int-arrays-deep`
workloads) split them cleanly: record-with-array and the array-of-records
depths are mixed-process artifacts (isolated: 1.14-1.88x faster than avsc),
but nested int arrays were **genuinely 1.24-1.25x slower** — the one case
where the noisy mixed suite was right and the earlier "it's pollution"
dismissal was wrong.

Root cause: every sync array read path pre-sized its result with
`result.length = startIdx + count` on an empty array. Growing an empty array
via `.length` costs ~45 ns per small array in V8; nested int-arrays allocate
one array per inner element, so it dominated (hand-rolled ablation: 939 ns
with length-grow vs 445 ns with `new Array(count)`). All four paths (generic
loop, bulk-primitive, record-block, schema-evolution resolver) now allocate
the first block exactly with `new Array(count)` and only length-grow on rare
multi-block continuations.

Isolated results: int-arrays shallow 1,029 → 464 ns vs avsc 954
(**2.1x faster**, was 1.25x slower); deep 1.11 ms → 655 us vs avsc 1.07 ms
(**1.63x faster**); paired re-checks show depth-1/depth-4/wide-8 unchanged or
better (1.88x / 1.14x / 1.30x faster than avsc). All eleven isolated sweep
workloads now favor avro-typescript.

### Union work review and hybrid long decode (2026-07-17, evening)

A separate continuation (Codex) optimized union reads: int-based branch
indices, assignment-built wrapped values, and cached compiled readers for
record branches, plus union-null / union-primitives / union-records sweep
workloads and bench-harness hardening (time-based warmup calibration, result
sink, pinned competitor versions). Review of that work found and fixed two
gaps, both in the same hazard classes this document already records:

1. `wrapUnionValue` used bare assignment with no `__proto__` guard —
   `__proto__` is a valid Avro name, and a branch named that would have
   invoked the prototype setter instead of creating an own property (the
   replaced computed literal was safe). It now takes a defineProperty path,
   with sync + async tests.
2. Union branch names were not interned; they are property keys on every
   wrapped value, so they now pass through `internString` at construction.

On top of that, `DirectSyncReadableTap.readLong`/`readLongArrayInto` gained a
hybrid decode: varints of up to 7 payload bytes (magnitudes below 2^48 —
every timestamp, almost every id) accumulate in exact double arithmetic with
one final BigInt conversion; longer varints keep the per-byte BigInt loop.
Microbenchmark: 69 to 24 ns on a timestamp-sized long, no change at the
int64 extremes; boundary values (2^48-1 vs 2^48, both signs) are covered by
tests on both the scalar and bulk paths.

Isolated results (avrots-reused vs avsc): union-null **2.2x faster**
(3.3 vs 7.3 us), union-primitives 1.07x faster, heterogeneous **1.35-1.38x
faster** (8.6-8.8 vs 11.9 us), depth1 at a best-yet 283 ns. union-records
improved from 1.40x to **1.26x slower** (58.3 vs 46.2 us) — the residual
~16 ns/element is the wrapped-union representation itself (wrapper
allocation plus a three-way polymorphic reader dispatch, where avsc emits
per-branch codegen classes); it is the one remaining sub-parity workload of
the fourteen in the sweep.

### Measurement guidance

- The shared-process benchmark suite systematically understates avro-ts and
  should be treated as a smoke test. V8 shares feedback vectors between
  closures created from the same source literal, so the compiled field
  readers go polymorphic as soon as a second tap class or schema shape runs
  anywhere in the process — even across separate type instances. avsc and
  avro-js generate per-schema readers with `new Function` and are immune.
  Driving one type instance with two tap classes measured a 12% penalty on
  the DirectTap-reused path.
- The closure-chain reader also only reaches top tier when the hot caller
  loop inlines it; under the Deno.bench harness it holds its best tier only
  intermittently (minimums match isolated runs, averages do not).
- Order effects extend to the heap: a variant benched late in a shared
  process runs against an already-grown heap and can measure up to 2x faster
  on allocation-heavy workloads (observed at depth 4). Only fresh-process
  comparisons are trustworthy for GC-bound shapes.
