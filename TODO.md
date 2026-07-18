# Read Performance Optimization TODO

This document outlines the performance optimization opportunities for the
avro-typescript library's read/deserialization path. The optimizations are
ordered by expected impact, with the highest-impact items first.

## Current State

Based on benchmarks comparing avro-typescript against avsc and avro-js:

- **0/25** benchmarks where avro-ts is faster than avsc
- **Average**: ~11x slower than avsc
- **Worst case**: 80x+ slower for deeply nested arrays
- **Root causes**: Memory allocations, abstraction layers, lack of compiled
  readers

---

## Phase 1: Memory Allocation Elimination (Highest Impact)

Memory allocation is the single largest performance bottleneck. Every allocation
triggers potential GC pressure and cache misses.

### 1.1 Optimized Varint Reading by Reading Larger Chunks

**Files**: `src/serialization/tap_sync.ts`, `src/serialization/tap.ts`

**Problem**: Varint reading reads byte by byte, causing multiple allocations:

```typescript
// Current implementation
readLong(): bigint {
  let pos = this.pos;
  let byte: number;
  do {
    byte = this.getByteAt(pos++);  // Allocates Uint8Array for each byte!
    // ...
  } while ((byte & 0x80) !== 0);
}
```

**Solution**: Read the maximum possible bytes for varints at once:

```typescript
// For readLong (up to 11 bytes)
readLong(): bigint {
  const maxBytes = 11;
  const bytes = this.buffer.read(this.pos, maxBytes);
  let result = 0n;
  let i = 0;
  let byte: number;
  do {
    byte = bytes[i++]!;
    result |= BigInt(byte & 0x7f) << shift;
    shift += 7n;
  } while ((byte & 0x80) !== 0 && shift < 70n);
  while ((byte & 0x80) !== 0) {
    byte = bytes[i++]!;
    result |= BigInt(byte & 0x7f) << shift;
    shift += 7n;
  }
  this.pos += i;
  return (result >> 1n) ^ -(result & 1n);
}

// For readInt (up to 5 bytes)
readInt(): number {
  const maxBytes = 5;
  const bytes = this.buffer.read(this.pos, maxBytes);
  let result = 0;
  let i = 0;
  let byte: number;
  do {
    byte = bytes[i++]!;
    result |= (byte & 0x7f) << shift;
    shift += 7;
  } while ((byte & 0x80) !== 0 && shift < 35);
  while ((byte & 0x80) !== 0) {
    byte = bytes[i++]!;
    result |= (byte & 0x7f) << shift;
    shift += 7;
  }
  this.pos += i;
  return (result >>> 1) ^ -(result & 1);
}
```

**Impact**: Reduces allocations from N (where N is varint length) to 1 per
varint. For ints/longs used in array lengths, string lengths, etc., this
eliminates most allocations in primitive-heavy schemas. Could provide 2-5x
improvement.

**Files to modify**:

- `src/serialization/tap_sync.ts` - Implement chunked varint reading
- `src/serialization/tap.ts` - Same for async version

---

### 1.3 Pre-allocated Result Objects for Records

**File**: `src/schemas/complex/record_type.ts`

**Problem**: Every record read creates a new empty object:

```typescript
// Current implementation (line 273-280)
public override readSync(tap: SyncReadableTapLike): Record<string, unknown> {
  const result: Record<string, unknown> = {};  // New object every time
  for (const field of this.#fields) {
    result[field.getName()] = field.getType().readSync(tap);
  }
  return result;
}
```

**Solution**: Use `Object.create(null)` with pre-defined shape, or consider
object pooling for hot paths:

```typescript
// Option A: Better object shape (minor improvement)
const result = Object.create(null);

// Option B: Pre-define property descriptors for consistent shape
private #propertyDescriptors: PropertyDescriptorMap;  // Cached at schema parse time

public override readSync(tap: SyncReadableTapLike): Record<string, unknown> {
  const result = Object.create(null, this.#propertyDescriptors);
  // ... fill values
}
```

**Impact**: Reduces object creation overhead and helps V8 optimize property
access patterns.

---

### 1.4 Pre-sized Arrays

**File**: `src/schemas/complex/array_type.ts`

**Problem**: Arrays grow dynamically via `push()`:

```typescript
// Current implementation
const result: T[] = [];
// ... in loop:
result.push(value); // May trigger reallocation
```

**Solution**: Pre-size arrays when block count is known:

```typescript
const result: T[] = [];
while (true) {
  let count = tap.readInt();
  if (count === 0) break;
  if (count < 0) {
    count = -count;
    tap.skipLong(); // Skip block size
  }

  // Pre-size the array
  const startIdx = result.length;
  result.length = startIdx + count;

  for (let i = 0; i < count; i++) {
    result[startIdx + i] = itemReader(tap); // Direct index assignment
  }
}
```

**Impact**: Avoids multiple array reallocations for large arrays. Combined with
removing closures (see 2.1), this could provide 2-3x improvement for array-heavy
schemas.

---

## Phase 2: Compiled Reader Strategies (High Impact)

Following the pattern established by `RecordWriterStrategy`, implement compiled
readers that eliminate per-field method call overhead.

### 2.1 Remove Closure Allocations in Array/Map Reading

**Files**: `src/schemas/complex/array_type.ts`,
`src/schemas/complex/map_type.ts`

**Problem**: Current implementation creates closures on every read:

```typescript
// Current implementation (line 274-284)
public override readSync(tap: SyncReadableTapLike): T[] {
  const result: T[] = [];
  readArrayIntoSync(
    tap,
    (innerTap) => this.#itemsType.readSync(innerTap),  // Closure allocation!
    (value) => { result.push(value); }                  // Closure allocation!
  );
  return result;
}
```

**Solution**: Pre-compile item readers at schema parse time:

```typescript
// Cache compiled reader
#compiledItemReader: ((tap: SyncReadableTapLike) => T) | null = null;

private getCompiledItemReader(): (tap: SyncReadableTapLike) => T {
  if (!this.#compiledItemReader) {
    this.#compiledItemReader = compileReader(this.#itemsType);
  }
  return this.#compiledItemReader;
}

public override readSync(tap: SyncReadableTapLike): T[] {
  const result: T[] = [];
  const itemReader = this.getCompiledItemReader();
  // Inline the loop - no closures needed
  while (true) {
    let count = tap.readInt();
    if (count === 0) break;
    // ... pre-size and read directly
    for (let i = 0; i < count; i++) {
      result[startIdx + i] = itemReader(tap);
    }
  }
  return result;
}
```

**Impact**: Arrays showed the worst benchmark results (30-80x slower). Removing
2N function calls per array could provide 3-5x improvement.

---

### 2.2 Create RecordReaderStrategy Interface

**New file**: `src/schemas/complex/record_reader_strategy.ts`

Mirror the existing `RecordWriterStrategy` pattern:

```typescript
export type CompiledSyncReader<T = unknown> = (tap: SyncReadableTapLike) => T;
export type CompiledAsyncReader<T = unknown> = (
  tap: ReadableTapLike,
) => Promise<T>;

export interface RecordReaderContext {
  fieldNames: readonly string[];
  fieldTypes: readonly Type[];
  recordType: RecordType;
}

export interface RecordReaderStrategy {
  /**
   * Compile a reader for a single field's type.
   */
  compileFieldReader<T>(
    fieldType: Type<T>,
    getRecordReader: (recordType: RecordType) => CompiledSyncReader,
  ): CompiledSyncReader<T>;

  /**
   * Assemble compiled field readers into a complete record reader.
   */
  assembleSyncRecordReader(
    context: RecordReaderContext,
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncReader<Record<string, unknown>>;
}
```

---

### 2.3 Implement InterpretedReaderStrategy

**New file**: `src/schemas/complex/record_reader_strategy.ts` (continued)

Start with an interpreted strategy as the baseline:

```typescript
export class InterpretedReaderStrategy implements RecordReaderStrategy {
  compileFieldReader<T>(
    fieldType: Type<T>,
    _getRecordReader: (recordType: RecordType) => CompiledSyncReader,
  ): CompiledSyncReader<T> {
    // Simply delegate to the type's readSync method
    return (tap) => fieldType.readSync(tap);
  }

  assembleSyncRecordReader(
    context: RecordReaderContext,
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncReader<Record<string, unknown>> {
    const { fieldNames } = context;
    const fieldCount = fieldNames.length;

    return (tap) => {
      const result: Record<string, unknown> = Object.create(null);
      for (let i = 0; i < fieldCount; i++) {
        result[fieldNames[i]!] = fieldReaders[i]!(tap);
      }
      return result;
    };
  }
}
```

---

### 2.4 Implement CompiledReaderStrategy

**New file**: `src/schemas/complex/record_reader_strategy.ts` (continued)

The compiled strategy inlines primitive reads for maximum performance:

```typescript
export class CompiledReaderStrategy implements RecordReaderStrategy {
  compileFieldReader<T>(
    fieldType: Type<T>,
    getRecordReader: (recordType: RecordType) => CompiledSyncReader,
  ): CompiledSyncReader<T> {
    // Inline readers for primitive types
    if (fieldType instanceof NullType) {
      return (_tap) => null as T;
    }
    if (fieldType instanceof BooleanType) {
      return (tap) => tap.readBoolean() as T;
    }
    if (fieldType instanceof IntType) {
      return (tap) => tap.readInt() as T;
    }
    if (fieldType instanceof LongType) {
      return (tap) => tap.readLong() as T;
    }
    if (fieldType instanceof FloatType) {
      return (tap) => tap.readFloat() as T;
    }
    if (fieldType instanceof DoubleType) {
      return (tap) => tap.readDouble() as T;
    }
    if (fieldType instanceof StringType) {
      return (tap) => tap.readString() as T;
    }
    if (fieldType instanceof BytesType) {
      const bytesType = fieldType as BytesType;
      return (tap) => {
        const len = tap.readInt(); // Use optimized int read
        return tap.readFixed(len) as T;
      };
    }
    if (fieldType instanceof RecordType) {
      // Use cached record reader to handle recursion
      return getRecordReader(fieldType) as CompiledSyncReader<T>;
    }
    if (fieldType instanceof ArrayType) {
      return this.#compileArrayReader(
        fieldType,
        getRecordReader,
      ) as CompiledSyncReader<T>;
    }
    // ... handle other types

    // Fallback for complex/unknown types
    return (tap) => fieldType.readSync(tap);
  }

  #compileArrayReader<T>(
    arrayType: ArrayType<T>,
    getRecordReader: (recordType: RecordType) => CompiledSyncReader,
  ): CompiledSyncReader<T[]> {
    const itemReader = this.compileFieldReader(
      arrayType.getItemsType(),
      getRecordReader,
    );

    return (tap) => {
      const result: T[] = [];
      while (true) {
        let count = tap.readInt();
        if (count === 0) break;
        if (count < 0) {
          count = -count;
          tap.skipLong();
        }
        const startIdx = result.length;
        result.length = startIdx + count;
        for (let i = 0; i < count; i++) {
          result[startIdx + i] = itemReader(tap);
        }
      }
      return result;
    };
  }

  assembleSyncRecordReader(
    context: RecordReaderContext,
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncReader<Record<string, unknown>> {
    const { fieldNames } = context;
    const fieldCount = fieldNames.length;

    // For small records, unroll the loop
    if (fieldCount <= 8) {
      return this.#assembleUnrolledReader(fieldNames, fieldReaders);
    }

    return (tap) => {
      const result: Record<string, unknown> = Object.create(null);
      for (let i = 0; i < fieldCount; i++) {
        result[fieldNames[i]!] = fieldReaders[i]!(tap);
      }
      return result;
    };
  }

  #assembleUnrolledReader(
    fieldNames: readonly string[],
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncReader<Record<string, unknown>> {
    // Generate unrolled reader for better JIT optimization
    switch (fieldNames.length) {
      case 1:
        return (tap) => ({
          [fieldNames[0]!]: fieldReaders[0]!(tap),
        });
      case 2:
        return (tap) => ({
          [fieldNames[0]!]: fieldReaders[0]!(tap),
          [fieldNames[1]!]: fieldReaders[1]!(tap),
        });
      case 3:
        return (tap) => ({
          [fieldNames[0]!]: fieldReaders[0]!(tap),
          [fieldNames[1]!]: fieldReaders[1]!(tap),
          [fieldNames[2]!]: fieldReaders[2]!(tap),
        });
      // ... up to 8 fields
      default:
        // Fallback to loop
        return (tap) => {
          const result: Record<string, unknown> = Object.create(null);
          for (let i = 0; i < fieldNames.length; i++) {
            result[fieldNames[i]!] = fieldReaders[i]!(tap);
          }
          return result;
        };
    }
  }
}
```

---

### 2.5 Create RecordReaderCache

**New file**: `src/schemas/complex/record_reader_cache.ts`

Handle recursive record types (similar to `RecordWriterCache`):

```typescript
export class RecordReaderCache {
  #syncReaders = new Map<
    RecordType,
    CompiledSyncReader<Record<string, unknown>>
  >();
  #strategy: RecordReaderStrategy;

  constructor(strategy: RecordReaderStrategy) {
    this.#strategy = strategy;
  }

  getOrCompileSyncReader(
    recordType: RecordType,
  ): CompiledSyncReader<Record<string, unknown>> {
    const existing = this.#syncReaders.get(recordType);
    if (existing) return existing;

    // Install placeholder for recursive references
    let actualReader: CompiledSyncReader<Record<string, unknown>> | null = null;
    const placeholder: CompiledSyncReader<Record<string, unknown>> = (tap) => {
      if (!actualReader) {
        throw new Error("Recursive reader not yet compiled");
      }
      return actualReader(tap);
    };
    this.#syncReaders.set(recordType, placeholder);

    // Compile field readers
    const fields = recordType.getFields();
    const fieldNames = fields.map((f) => f.getName());
    const fieldTypes = fields.map((f) => f.getType());
    const fieldReaders = fieldTypes.map((type) =>
      this.#strategy.compileFieldReader(
        type,
        (rt) => this.getOrCompileSyncReader(rt),
      )
    );

    // Assemble complete reader
    actualReader = this.#strategy.assembleSyncRecordReader(
      { fieldNames, fieldTypes, recordType },
      fieldReaders,
    );

    // Replace placeholder
    this.#syncReaders.set(recordType, actualReader);
    return actualReader;
  }
}
```

---

### 2.6 Integrate Reader Strategy into RecordType

**File**: `src/schemas/complex/record_type.ts`

```typescript
// Add to RecordType class
#readerCache: RecordReaderCache | null = null;
#compiledSyncReader: CompiledSyncReader<Record<string, unknown>> | null = null;

private getCompiledSyncReader(): CompiledSyncReader<Record<string, unknown>> {
  if (!this.#compiledSyncReader) {
    const strategy = this.#options?.readerStrategy ?? new CompiledReaderStrategy();
    this.#readerCache = new RecordReaderCache(strategy);
    this.#compiledSyncReader = this.#readerCache.getOrCompileSyncReader(this);
  }
  return this.#compiledSyncReader;
}

public override readSync(tap: SyncReadableTapLike): Record<string, unknown> {
  return this.getCompiledSyncReader()(tap);
}
```

---

## Phase 3: Tap-Level Optimizations (Medium Impact)

### 3.1 Optimized Integer Reading (32-bit Fast Path)

**File**: `src/serialization/tap_sync.ts`

**Problem**: All integer reads go through BigInt operations:

```typescript
// Current implementation
readInt(): number {
  return bigIntToSafeNumber(this.readLong(), "readInt value");
}

readLong(): bigint {
  // Uses BigInt throughout, even for small values
}
```

**Solution**: Keep the chunked reading from Phase 1, but since we can't change
APIs, the optimization is already in place by reading larger chunks. For further
optimization, we could implement readInt separately with chunked reading as done
in Phase 1.

**Impact**: Integer fields are extremely common (array lengths, map sizes, enum
values, int fields). The chunked reading provides most of the benefit.

---

### 3.2 Optimized String Reading

**File**: `src/serialization/tap_sync.ts`

**Problem**: String reading allocates a new Uint8Array via `readFixed()`:

```typescript
// Current implementation
readString(): string {
  const len = bigIntToSafeNumber(this.readLong(), "readString length");
  const bytes = this.readFixed(len);  // Allocates!
  return decode(bytes);
}
```

**Solution**: Already optimized by reading larger chunks for the length varint.
For the string data, it still allocates via readFixed, but since readFixed is
used for fixed-size data, it's acceptable.

**Impact**: Strings are common in records and map keys. The length reading
optimization helps, but full zero-copy would require API changes.

---

### 3.3 Add Skip Methods for Efficient Skipping

**File**: `src/serialization/tap_sync.ts`

When skipping over unused fields (e.g., schema evolution), avoid full
deserialization:

```typescript
skipInt(): void {
  // Use chunked reading similar to readInt but without decoding
  const maxBytes = 5;
  const bytes = this.buffer.read(this.pos, maxBytes);
  let i = 0;
  while ((bytes[i++]! & 0x80) !== 0) { }
  this.pos += i;
}

skipLong(): void {
  // Same for long
  const maxBytes = 11;
  const bytes = this.buffer.read(this.pos, maxBytes);
  let i = 0;
  while ((bytes[i++]! & 0x80) !== 0) { }
  this.pos += i;
}

skipString(): void {
  const len = this.readInt();  // Use optimized int read
  this.pos += len;
}

skipBytes(): void {
  this.skipString();  // Same encoding
}

skipFixed(size: number): void {
  this.pos += size;
}
```

---

## Phase 4: Advanced Optimizations (Lower Impact, Higher Complexity)

### 4.1 Object Shape Hints

Help V8 optimize object creation by ensuring consistent property order:

```typescript
// In compiled reader, create objects with consistent shape
assembleSyncRecordReader(context, fieldReaders) {
  const { fieldNames } = context;
  
  // Create a prototype with all properties pre-defined
  const proto = Object.create(null);
  for (const name of fieldNames) {
    proto[name] = undefined;
  }
  
  return (tap) => {
    const result = Object.create(proto);
    for (let i = 0; i < fieldNames.length; i++) {
      result[fieldNames[i]!] = fieldReaders[i]!(tap);
    }
    return result;
  };
}
```

### 4.2 TypedArray Fast Paths

For arrays of fixed-size primitives (int, long, float, double), read directly
into TypedArrays:

```typescript
// Special case for array<int>
if (itemType instanceof IntType) {
  return (tap) => {
    const result: number[] = [];
    while (true) {
      let count = tap.readInt();
      if (count === 0) break;
      if (count < 0) {
        count = -count;
        tap.skipLong();
      }

      const startIdx = result.length;
      result.length = startIdx + count;

      // Bulk read if possible
      for (let i = 0; i < count; i++) {
        result[startIdx + i] = tap.readInt();
      }
    }
    return result;
  };
}
```

### 4.3 Streaming/Iterator-Based Reading

For very large arrays, provide an iterator interface that doesn't materialize
the entire array:

```typescript
public *iterReadSync(tap: SyncReadableTapLike): Generator<T> {
  const itemReader = this.getCompiledItemReader();
  while (true) {
    let count = tap.readInt();
    if (count === 0) break;
    if (count < 0) { count = -count; tap.skipLong(); }
    for (let i = 0; i < count; i++) {
      yield itemReader(tap);
    }
  }
}
```

### 4.4 WebAssembly for Varint Decoding

For extreme performance, implement varint decoding in WebAssembly:

```typescript
// Load WASM module once
const wasmModule = await WebAssembly.instantiate(warintDecoderWasm);
const decodeVarint = wasmModule.instance.exports.decode_varint as (
  bufferPtr: number,
  offset: number,
) => bigint;
```

---

## Implementation Checklist

### Phase 1 (Do First - Highest ROI)

- [ ] Modify `SyncReadableTap.readInt()` to read up to 5 bytes at once
- [ ] Modify `SyncReadableTap.readLong()` to read up to 11 bytes at once
- [ ] Modify `SyncReadableTap.skipLong()` to read up to 11 bytes and count
      length
- [ ] Modify `ReadableTap.readInt()` to read up to 5 bytes at once (async)
- [ ] Modify `ReadableTap.readLong()` to read up to 11 bytes at once (async)
- [ ] Modify `ReadableTap.skipLong()` to read up to 11 bytes and count length
      (async)
- [ ] Pre-size arrays in `ArrayType.readSync()`
- [ ] Use `Object.create(null)` in `RecordType.readSync()`

### Phase 2 (Do Second - Major Architectural Improvement)

- [ ] Create `RecordReaderStrategy` interface
- [ ] Implement `InterpretedReaderStrategy`
- [ ] Implement `CompiledReaderStrategy`
- [ ] Create `RecordReaderCache` for recursive types
- [ ] Integrate reader strategy into `RecordType`
- [ ] Add `readerStrategy` option to `createType()`
- [ ] Compile item readers in `ArrayType`
- [ ] Compile value readers in `MapType`

### Phase 3 (Do Third - Tap Optimizations)

- [ ] Implement chunked reading for `readInt()` and `readLong()` (completed in
      Phase 1)
- [ ] Optimize `readString()` length reading (completed in Phase 1)
- [ ] Add skip methods (`skipInt`, `skipString`, etc.)
- [ ] Inline zigzag decoding

### Phase 4 (Optional - Advanced)

- [ ] Object shape hints for consistent V8 optimization
- [ ] TypedArray fast paths for primitive arrays
- [ ] Iterator-based reading for large arrays
- [ ] Consider WASM for varint decoding (if still needed after other opts)

---

## Expected Performance Improvements

| Phase   | Expected Improvement | Complexity |
| ------- | -------------------- | ---------- |
| Phase 1 | 2-5x                 | Low        |
| Phase 2 | 2-3x additional      | Medium     |
| Phase 3 | 1.5-2x additional    | Low-Medium |
| Phase 4 | 1.2-1.5x additional  | High       |

**Cumulative**: With all phases implemented, read performance should be
competitive with avsc (within 1-2x), and potentially faster for certain schemas.

---

## Files to Create/Modify

### New Files

- `src/schemas/complex/record_reader_strategy.ts`
- `src/schemas/complex/record_reader_cache.ts`

### Files to Modify

- `src/serialization/buffers/buffer_sync.ts` - Add interface methods
- `src/serialization/buffers/in_memory_buffer_sync.ts` - Implement new methods
- `src/serialization/tap_sync.ts` - Optimize read methods
- `src/schemas/complex/record_type.ts` - Use compiled readers
- `src/schemas/complex/array_type.ts` - Pre-compile, remove closures
- `src/schemas/complex/map_type.ts` - Pre-compile, remove closures
- `src/type/create_type.ts` - Add `readerStrategy` option

---

## Benchmarking Strategy

After each phase, run benchmarks to measure improvement:

```bash
# Run deserialize benchmarks
deno task bench:deserialize

# Compare specific schemas
deno bench --filter "primitive:" packages/benchmarks/deserialize_single_bench.ts
deno bench --filter "record:" packages/benchmarks/deserialize_single_bench.ts
deno bench --filter "array-of-records:" packages/benchmarks/deserialize_single_bench.ts
```

Track metrics:

1. **vs avsc ratio** - Primary metric (goal: < 2x slower)
2. **Absolute time** - For regression detection
3. **Memory allocations** - Use `--v8-flags=--trace-gc` to monitor GC

---

## References

- Existing writer strategy: `src/schemas/complex/record_writer_strategy.ts`
- Existing writer cache: `src/schemas/complex/record_writer_cache.ts`
- avsc implementation: Uses direct buffer access and compiled readers
- V8 optimization tips: https://v8.dev/docs/hidden-classes
