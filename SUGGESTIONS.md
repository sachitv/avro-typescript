# Performance Optimization Suggestions for Array and Record Deserialization

## Current Performance Gap

Based on benchmark results (`deno task bench:deserialize`):

| Benchmark                         | Performance vs avsc                    |
| --------------------------------- | -------------------------------------- |
| array-of-records: depth 1         | **4.73x slower**                       |
| array-of-records: depth 4         | **4.49x slower**                       |
| record: array of records          | **4.97x slower**                       |
| record: nested                    | **2.89x slower**                       |
| record: simple                    | 1.02x slower (competitive!)            |
| array-of-arrays                   | 1.05-1.12x slower (nearly competitive) |
| primitive: int (DirectTap-reused) | **3.30x faster**                       |

## Root Cause Analysis

### Problem 1: Array Block Reading Overhead

In `src/schemas/complex/array_type.ts:290-315`, the `readSync` method has
per-element method call overhead:

```typescript
while (true) {
  let count = tap.readInt();
  if (count === 0) break;
  // ...
  for (let i = 0; i < count; i++) {
    result[startIdx + i] = itemsType.readSync(tap); // ← Per-element method call
  }
}
```

For an array with 10 records, that's 10 function calls with indirection
overhead.

### Problem 2: Record Reading Indirection

In `record_type.ts` lines 304-308:

```typescript
public override readSync(tap: SyncReadableTapLike): Record<string, unknown> {
  this.#ensureFields();
  const reader = this.#getOrCreateCompiledSyncReader();  // ← Cache lookup every call
  return reader(tap) as Record<string, unknown>;
}
```

Issues:

- Cache lookup overhead on every call
- Type assertion overhead
- Function boundary crossing

### Problem 3: Nested Array/Record Amplification

When you have `array<array<record>>`, the overhead compounds:

- Outer array loop → inner array loop → record reading
- Each level adds indirection

At depth 4 with 10,000 records:

- **avro-typescript**: 10,000 × (array overhead + record cache lookup + reader
  call) = 2.56ms
- **avsc**: Likely a single tight loop with inlined field reads = 570µs

---

## Optimization Recommendations

### Phase 1: Quick Wins (Target: 2x improvement)

#### 1. Cache the compiled reader directly in RecordType

**Estimated improvement: 10-15%**

File: `src/schemas/complex/record_type.ts`

Add field after line 114:

```typescript
#cachedCompiledSyncReader?: CompiledSyncReader;
```

Modify `readSync` (line 304-308):

```typescript
public override readSync(tap: SyncReadableTapLike): Record<string, unknown> {
  this.#ensureFields();
  if (!this.#cachedCompiledSyncReader) {
    this.#cachedCompiledSyncReader = this.#getOrCreateCompiledSyncReader();
  }
  return this.#cachedCompiledSyncReader(tap);
}
```

#### 2. Optimize array block reading for records

**Estimated improvement: 20-30%**

In `ArrayType.readSync`, detect record types and use a specialized fast path
that avoids the virtual method call overhead.

---

### Phase 2: Major Optimizations (Target: 4-5x total improvement)

#### 3. Implement composite reader generation for array<record>

Generate a single function that inlines both array iteration AND record field
reading:

**Current code** (2 layers of indirection):

```typescript
// ArrayType.readSync calls:
for (let i = 0; i < count; i++) {
  result[startIdx + i] = itemsType.readSync(tap); // Calls RecordType.readSync
}

// RecordType.readSync then calls:
const reader = this.#getOrCreateCompiledSyncReader(); // Cache lookup
return reader(tap);
```

**Optimized** (single generated function):

```typescript
// Generated once and cached:
function readArrayOfRecords(tap) {
  const result = [];
  while (true) {
    let count = tap.readInt();
    if (count === 0) break;
    if (count < 0) {
      count = -count;
      tap.skipLong();
    }
    const startIdx = result.length;
    result.length = startIdx + count;
    // INLINED RECORD READS - no function calls!
    for (let i = 0; i < count; i++) {
      const record = {
        id: tap.readInt(),
        name: tap.readString(),
        value: tap.readDouble(),
      };
      result[startIdx + i] = record;
    }
  }
  return result;
}
```

This eliminates:

- 10,000 `RecordType.readSync()` calls
- 10,000 cache lookups
- 10,000 function returns

#### 4. Use Function constructor for truly dynamic code generation

avsc likely uses `new Function(...)` to generate optimized readers. Consider
implementing similar code generation in `CompiledReaderStrategy`.

Example:

```typescript
function generateRecordReader(
  fieldNames: string[],
  fieldReaders: string[],
): CompiledSyncReader {
  const body = `
    return {
      ${
    fieldNames.map((name, i) => `"${name}": tap.${fieldReaders[i]}()`).join(
      ",\n      ",
    )
  }
    };
  `;
  return new Function("tap", body) as CompiledSyncReader;
}
```

---

### Phase 3: Advanced Optimizations

#### 5. Buffer reading optimizations

- Batch reads where possible
- Reduce bounds checking
- Use DataView more efficiently
- Consider reading multiple varints in a single pass

#### 6. Object creation optimization

- Use `Object.create(null)` for records (faster property access, no prototype
  chain)
- Consider object pooling for high-frequency scenarios
- Pre-define object shapes for V8 hidden class optimization

#### 7. Inline primitive reads in nested structures

For deeply nested structures, generate a single function that reads the entire
structure without any intermediate function calls.

---

## Implementation Priority

1. **High Priority / Low Effort**: Cache compiled reader (#1)
2. **High Priority / Medium Effort**: Composite array<record> reader (#3)
3. **Medium Priority / High Effort**: Dynamic code generation (#4)
4. **Low Priority / Research**: Buffer and object optimizations (#5, #6, #7)

## Benchmark Commands

```bash
# Run deserialization benchmarks
deno task bench:deserialize

# Run with specific iterations for more accuracy
# Edit BENCH_ITERATIONS in deserialize_single_bench.ts
```

---

## Appendix: How avsc Achieves Fast Performance

Based on analysis of the [avsc repository](https://github.com/mtth/avsc):

### 1. Runtime Code Generation (JIT-Style Compilation)

avsc uses `new Function()` to create specialized functions for each schema:

```javascript
// From avsc lib/types.js - Record reader generation
_createReader () {
  let names = [];
  let values = [this.recordConstructor];
  for (let i = 0, l = this.fields.length; i < l; i++) {
    names.push('t' + i);
    values.push(this.fields[i].type);
  }
  let name = this._getConstructorName();
  let body = 'return function read' + name + '(t) {\n';
  body += '  return new ' + name + '(\n    ';
  body += names.map((s) => { return s + '._read(t)'; }).join(',\n    ');
  body += '\n  );\n};';
  names.unshift(name);
  return new Function(names.join(), body).apply(undefined, values);
}
```

This generates tight, schema-specific functions that eliminate runtime schema
interpretation.

### 2. Pre-serialized Default Values

Default values are serialized once and stored in closures:

```javascript
// From avsc - Writer generation with pre-serialized defaults
if (field.defaultValue() !== undefined) {
  let value = field.type.toBuffer(field.defaultValue());
  args.push("d" + i);
  values.push(value);
  body += "if (v" + i + " === undefined) {\n";
  body += "    t.writeFixed(d" + i + ", " + value.length + ");\n";
  body += "  } else {\n    t" + i + "._write(t, v" + i + ");\n  }\n";
}
```

### 3. Array Pre-allocation

avsc pre-allocates arrays with the first block size (~10% speedup):

```javascript
// From avsc lib/types.js
_read (tap) {
  let items = this.itemsType;
  let i = 0;
  let val, n;
  while ((n = tap.readLong())) {
    if (n < 0) { n = -n; tap.skipLong(); }
    val = val || new Array(n);  // Pre-allocate on first block
    while (n--) {
      val[i++] = items._read(tap);
    }
  }
  return val || [];
}
```

### 4. Optimized String Decoding

Size-based strategy for strings:

```javascript
// From avsc - String reading
if (len > 24) {
  return decodeSlice(arr, pos, end); // TextDecoder for long strings
}
// Manual 4-byte chunk decoding for short strings
while (pos + 3 < end) {
  let a = arr[pos], b = arr[pos + 1], c = arr[pos + 2], d = arr[pos + 3];
  if ((a | b | c | d) & 0x80) {
    output += decodeSlice(arr, pos, end);
    return output;
  }
  output += String.fromCharCode(a, b, c, d);
  pos += 4;
}
```

### 5. Shared Encoding Buffer

Reuses a single 1KB buffer for most encoding:

```javascript
let TAP = Tap.withCapacity(1024);

toBuffer (val) {
  TAP.pos = 0;
  this._write(TAP, val);
  if (TAP.isValid()) {
    return TAP.toBuffer();
  }
  // Overflow: allocate larger
  let buf = new Uint8Array(TAP.pos);
  this._write(Tap.fromBuffer(buf), val);
  return buf;
}
```

### Key Takeaways

| Technique             | avsc                     | avro-typescript        |
| --------------------- | ------------------------ | ---------------------- |
| Code generation       | `new Function()`         | Closure composition    |
| Schema interpretation | Once at init             | Per-call cache lookup  |
| Array allocation      | Pre-allocate first block | Pre-size on each block |
| String decoding       | Size-based strategy      | Standard TextDecoder   |
| Default values        | Pre-serialized           | Computed per-write     |

The **biggest performance gap** comes from avsc's use of `new Function()` to
generate tight, specialized code vs avro-typescript's closure-based compiled
readers that still have function call overhead per field.
