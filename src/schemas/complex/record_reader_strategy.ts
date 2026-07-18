import type { SyncReadableTapLike } from "../../serialization/tap_sync.ts";
import type { ReadableTapLike } from "../../serialization/tap.ts";
import type { Type } from "../type.ts";
import { BooleanType } from "../primitive/boolean_type.ts";
import { BytesType } from "../primitive/bytes_type.ts";
import { DoubleType } from "../primitive/double_type.ts";
import { FloatType } from "../primitive/float_type.ts";
import { IntType } from "../primitive/int_type.ts";
import { LongType } from "../primitive/long_type.ts";
import { NullType } from "../primitive/null_type.ts";
import { StringType } from "../primitive/string_type.ts";

/**
 * Compiled async reader function signature.
 */
export type CompiledReader = (tap: ReadableTapLike) => Promise<unknown>;

/**
 * Compiled sync reader function signature.
 */
export type CompiledSyncReader = (tap: SyncReadableTapLike) => unknown;

/**
 * Fills up to ten fields of an in-progress record from the tap.
 * Used to compose readers for records wider than the specialization limit.
 */
type SyncFieldChunkFiller = (
  tap: SyncReadableTapLike,
  result: Record<string, unknown>,
) => void;

/**
 * Compiled reader that fills a contiguous block of record values.
 */
export type CompiledSyncRecordBlockReader = (
  tap: SyncReadableTapLike,
  result: Record<string, unknown>[],
  startIndex: number,
  count: number,
) => void;

/** Internal capability for obtaining an assembled asynchronous record reader. */
export const compiledRecordReader = Symbol.for(
  "@sachitv/avro-typescript/compiled-record-reader/v1",
);

/** A type exposing its assembled asynchronous record reader. */
export interface CompiledRecordReaderProvider {
  [compiledRecordReader](): CompiledReader;
}

/** Internal capability for obtaining an assembled synchronous record reader. */
export const compiledSyncRecordReader = Symbol.for(
  "@sachitv/avro-typescript/compiled-sync-record-reader/v1",
);

/** A type exposing its assembled synchronous record reader. */
export interface CompiledSyncRecordReaderProvider {
  [compiledSyncRecordReader](): CompiledSyncReader;
}

/**
 * Internal capability used by composite types to obtain a record block reader.
 * A versioned global symbol keeps the optimization off the package's public
 * string-named API while still working across duplicate package instances.
 */
export const compiledSyncRecordBlockReader = Symbol.for(
  "@sachitv/avro-typescript/compiled-sync-record-block-reader/v1",
);

/** A type exposing the internal compiled record-block capability. */
export interface CompiledSyncRecordBlockReaderProvider {
  [compiledSyncRecordBlockReader](): CompiledSyncRecordBlockReader;
}

/**
 * Context passed to strategy methods for assembling record readers.
 */
export interface RecordReaderContext {
  /** Field names in order. */
  fieldNames: string[];
  /** Field types in order. */
  fieldTypes: Type[];
}

/**
 * Strategy interface for compiling record field readers.
 *
 * Implementations can choose different approaches:
 * - CompiledReaderStrategy: Inlines primitive tap methods for performance
 * - InterpretedReaderStrategy: Delegates to type.read() for simplicity
 */
export interface RecordReaderStrategy {
  /**
   * Compiles an async reader for a single field type.
   * @param fieldType The type of the field.
   * @param getRecordReader Callback to get a reader for nested RecordTypes (handles recursion).
   */
  compileFieldReader(
    fieldType: Type,
    getRecordReader: (type: Type) => CompiledReader,
  ): CompiledReader;

  /**
   * Compiles a sync reader for a single field type.
   * @param fieldType The type of the field.
   * @param getRecordReader Callback to get a reader for nested RecordTypes (handles recursion).
   */
  compileSyncFieldReader(
    fieldType: Type,
    getRecordReader: (type: Type) => CompiledSyncReader,
  ): CompiledSyncReader;

  /**
   * Assembles field readers into a complete async record reader.
   * @param context The record reader context.
   * @param fieldReaders Pre-compiled readers for each field.
   */
  assembleRecordReader(
    context: RecordReaderContext,
    fieldReaders: CompiledReader[],
  ): CompiledReader;

  /**
   * Assembles field readers into a complete sync record reader.
   * @param context The record reader context.
   * @param fieldReaders Pre-compiled readers for each field.
   */
  assembleSyncRecordReader(
    context: RecordReaderContext,
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncReader;

  /**
   * Optionally assembles a fused synchronous block reader. Strategies which do
   * not implement this hook retain their exact scalar assembly semantics via a
   * cache-provided fallback.
   */
  assembleSyncRecordBlockReader?(
    context: RecordReaderContext,
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncRecordBlockReader | undefined;
}

/**
 * Checks if a type is a RecordType without importing it (avoids circular dependency).
 * Uses a static marker property instead of constructor name to avoid minification issues.
 */
function isRecordType(type: Type): boolean {
  return "__AVRO_RECORD_TYPE__" in type.constructor;
}

/**
 * Builds a template object holding every field name with an undefined value.
 *
 * Spread-cloning this seed gives each decoded record its final hidden class
 * and property backing store in a single allocation; the per-field stores
 * then overwrite existing properties instead of walking one map transition
 * (and possibly growing the out-of-object backing store) per field. This is
 * what keeps construction cost linear in field count for wide records.
 * Callers must exclude `__proto__` before building a seed.
 */
function makeSeedRecord(fieldNames: string[]): Record<string, unknown> {
  const seed: Record<string, unknown> = {};
  for (const name of fieldNames) {
    seed[name] = undefined;
  }
  return seed;
}

function setRecordField(
  record: Record<string, unknown>,
  name: string,
  value: unknown,
): void {
  if (name === "__proto__") {
    Object.defineProperty(record, name, {
      configurable: true,
      enumerable: true,
      value,
      writable: true,
    });
  } else {
    record[name] = value;
  }
}

/**
 * Compiled reader strategy that inlines primitive tap methods for performance.
 *
 * This is the default strategy that provides optimized deserialization by:
 * - Inlining primitive type reads directly from tap methods
 * - Supporting recursive record types via the getRecordReader callback
 */
export class CompiledReaderStrategy implements RecordReaderStrategy {
  /**
   * Compiles an async reader for a single field type with primitive inlining.
   */
  public compileFieldReader(
    fieldType: Type,
    getRecordReader: (type: Type) => CompiledReader,
  ): CompiledReader {
    if (isRecordType(fieldType)) {
      return getRecordReader(fieldType);
    }

    return CompiledReaderStrategy.#compileReader(fieldType);
  }

  /**
   * Compiles a sync reader for a single field type with primitive inlining.
   */
  public compileSyncFieldReader(
    fieldType: Type,
    getRecordReader: (type: Type) => CompiledSyncReader,
  ): CompiledSyncReader {
    if (isRecordType(fieldType)) {
      return getRecordReader(fieldType);
    }

    return CompiledReaderStrategy.#compileSyncReader(fieldType);
  }

  /**
   * Assembles an async record reader from field readers.
   */
  public assembleRecordReader(
    context: RecordReaderContext,
    fieldReaders: CompiledReader[],
  ): CompiledReader {
    const { fieldNames } = context;
    const fieldCount = fieldNames.length;

    return async (tap) => {
      const result: Record<string, unknown> = {};
      for (let i = 0; i < fieldCount; i++) {
        setRecordField(
          result,
          fieldNames[i]!,
          await fieldReaders[i]!(tap),
        );
      }
      return result;
    };
  }

  /**
   * Assembles a sync record reader from field readers.
   *
   * Optimization: For small records (<=10 fields), generates specialized
   * readers with direct property assignment to avoid loop overhead. Wider
   * records use a dynamic-assignment loop. Only records containing the
   * special `__proto__` field name (whose assignment would invoke the legacy
   * prototype setter) pay the per-field setRecordField dispatch.
   */
  public assembleSyncRecordReader(
    context: RecordReaderContext,
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncReader {
    const { fieldNames } = context;
    const fieldCount = fieldNames.length;

    if (!fieldNames.includes("__proto__")) {
      if (fieldCount <= 10) {
        return this.#generateSpecializedSyncReader(fieldNames, fieldReaders);
      }

      // Wide records compose hoisted 10-field chunk fillers over a cloned
      // seed. Hoisted constant-key stores stay on V8's monomorphic fast path
      // (~4 ns/field) where a dynamic `result[names[i]]` loop pays ~12
      // ns/field, and the composition keeps code size fixed for any width.
      const seed = makeSeedRecord(fieldNames);
      const fillers: SyncFieldChunkFiller[] = [];
      for (let start = 0; start < fieldCount; start += 10) {
        fillers.push(CompiledReaderStrategy.#generateSyncFieldChunkFiller(
          fieldNames.slice(start, start + 10),
          fieldReaders.slice(start, start + 10),
        ));
      }
      const fillerCount = fillers.length;
      return (tap) => {
        const result: Record<string, unknown> = { ...seed };
        for (let i = 0; i < fillerCount; i++) {
          fillers[i]!(tap, result);
        }
        return result;
      };
    }

    return (tap) => {
      const result: Record<string, unknown> = {};
      for (let i = 0; i < fieldCount; i++) {
        setRecordField(result, fieldNames[i]!, fieldReaders[i]!(tap));
      }
      return result;
    };
  }

  /**
   * Assembles a CSP-safe fused block reader for the default record semantics.
   * Subclasses overriding scalar record assembly fall back to that scalar
   * reader unless they explicitly override this hook as well.
   */
  public assembleSyncRecordBlockReader(
    context: RecordReaderContext,
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncRecordBlockReader | undefined {
    if (
      this.assembleSyncRecordReader !==
        CompiledReaderStrategy.prototype.assembleSyncRecordReader
    ) {
      return undefined;
    }

    return this.#generateSpecializedSyncBlockReader(
      context.fieldNames,
      fieldReaders,
    );
  }

  /**
   * Builds a filler writing 1-10 fields into an existing record with hoisted
   * constant-key stores. The record already has its final shape from the
   * cloned seed, so every store overwrites an existing property.
   */
  static #generateSyncFieldChunkFiller(
    fieldNames: string[],
    fieldReaders: CompiledSyncReader[],
  ): SyncFieldChunkFiller {
    const n = fieldNames.length;
    const r0 = fieldReaders[0]!;
    const r1 = fieldReaders[1]!;
    const r2 = fieldReaders[2]!;
    const r3 = fieldReaders[3]!;
    const r4 = fieldReaders[4]!;
    const r5 = fieldReaders[5]!;
    const r6 = fieldReaders[6]!;
    const r7 = fieldReaders[7]!;
    const r8 = fieldReaders[8]!;
    const r9 = fieldReaders[9]!;
    const f0 = fieldNames[0]!;
    const f1 = fieldNames[1]!;
    const f2 = fieldNames[2]!;
    const f3 = fieldNames[3]!;
    const f4 = fieldNames[4]!;
    const f5 = fieldNames[5]!;
    const f6 = fieldNames[6]!;
    const f7 = fieldNames[7]!;
    const f8 = fieldNames[8]!;
    const f9 = fieldNames[9]!;

    switch (n) {
      case 1:
        return (tap, v) => {
          v[f0] = r0(tap);
        };
      case 2:
        return (tap, v) => {
          v[f0] = r0(tap);
          v[f1] = r1(tap);
        };
      case 3:
        return (tap, v) => {
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
        };
      case 4:
        return (tap, v) => {
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
        };
      case 5:
        return (tap, v) => {
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
        };
      case 6:
        return (tap, v) => {
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          v[f5] = r5(tap);
        };
      case 7:
        return (tap, v) => {
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          v[f5] = r5(tap);
          v[f6] = r6(tap);
        };
      case 8:
        return (tap, v) => {
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          v[f5] = r5(tap);
          v[f6] = r6(tap);
          v[f7] = r7(tap);
        };
      case 9:
        return (tap, v) => {
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          v[f5] = r5(tap);
          v[f6] = r6(tap);
          v[f7] = r7(tap);
          v[f8] = r8(tap);
        };
      default:
        return (tap, v) => {
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          v[f5] = r5(tap);
          v[f6] = r6(tap);
          v[f7] = r7(tap);
          v[f8] = r8(tap);
          v[f9] = r9(tap);
        };
    }
  }

  #generateSpecializedSyncReader(
    fieldNames: string[],
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncReader {
    const n = fieldNames.length;
    // Dynamic assignment to a fresh object is substantially faster than a
    // computed-key object literal: V8 turns each assignment into a cached
    // hidden-class transition, while computed literal keys take a generic
    // property-definition runtime path. Profiling the nested-record workload
    // attributed roughly 200 ns per record level to that runtime path.
    // Hoisting names and readers into locals avoids captured-array loads.
    // Seed-cloning (see makeSeedRecord) was measured slower than plain
    // assignment at these widths and is reserved for records >10 fields.
    const r0 = fieldReaders[0]!;
    const r1 = fieldReaders[1]!;
    const r2 = fieldReaders[2]!;
    const r3 = fieldReaders[3]!;
    const r4 = fieldReaders[4]!;
    const r5 = fieldReaders[5]!;
    const r6 = fieldReaders[6]!;
    const r7 = fieldReaders[7]!;
    const r8 = fieldReaders[8]!;
    const r9 = fieldReaders[9]!;
    const f0 = fieldNames[0]!;
    const f1 = fieldNames[1]!;
    const f2 = fieldNames[2]!;
    const f3 = fieldNames[3]!;
    const f4 = fieldNames[4]!;
    const f5 = fieldNames[5]!;
    const f6 = fieldNames[6]!;
    const f7 = fieldNames[7]!;
    const f8 = fieldNames[8]!;
    const f9 = fieldNames[9]!;

    switch (n) {
      case 0:
        return () => ({});
      case 1:
        return (tap) => {
          const v: Record<string, unknown> = {};
          v[f0] = r0(tap);
          return v;
        };
      case 2:
        return (tap) => {
          const v: Record<string, unknown> = {};
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          return v;
        };
      case 3:
        return (tap) => {
          const v: Record<string, unknown> = {};
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          return v;
        };
      case 4:
        return (tap) => {
          const v: Record<string, unknown> = {};
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          return v;
        };
      case 5:
        return (tap) => {
          const v: Record<string, unknown> = {};
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          return v;
        };
      case 6:
        return (tap) => {
          const v: Record<string, unknown> = {};
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          v[f5] = r5(tap);
          return v;
        };
      case 7:
        return (tap) => {
          const v: Record<string, unknown> = {};
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          v[f5] = r5(tap);
          v[f6] = r6(tap);
          return v;
        };
      case 8:
        return (tap) => {
          const v: Record<string, unknown> = {};
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          v[f5] = r5(tap);
          v[f6] = r6(tap);
          v[f7] = r7(tap);
          return v;
        };
      case 9:
        return (tap) => {
          const v: Record<string, unknown> = {};
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          v[f5] = r5(tap);
          v[f6] = r6(tap);
          v[f7] = r7(tap);
          v[f8] = r8(tap);
          return v;
        };
      default:
        return (tap) => {
          const v: Record<string, unknown> = {};
          v[f0] = r0(tap);
          v[f1] = r1(tap);
          v[f2] = r2(tap);
          v[f3] = r3(tap);
          v[f4] = r4(tap);
          v[f5] = r5(tap);
          v[f6] = r6(tap);
          v[f7] = r7(tap);
          v[f8] = r8(tap);
          v[f9] = r9(tap);
          return v;
        };
    }
  }

  #generateSpecializedSyncBlockReader(
    fieldNames: string[],
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncRecordBlockReader | undefined {
    const fieldCount = fieldNames.length;
    // Assignment gives V8 a stable hidden-class transition and is materially
    // faster than computed object-literal keys. `__proto__` is the one key for
    // which assignment has setter semantics, so retain the scalar reader's
    // safe construction for that uncommon schema shape. The 10-field cap
    // matches the scalar specialization; wider records fall back to calling
    // the assembled scalar reader per element.
    if (fieldCount > 10 || fieldNames.includes("__proto__")) {
      return undefined;
    }

    const r0 = fieldReaders[0]!;
    const r1 = fieldReaders[1]!;
    const r2 = fieldReaders[2]!;
    const r3 = fieldReaders[3]!;
    const r4 = fieldReaders[4]!;
    const r5 = fieldReaders[5]!;
    const r6 = fieldReaders[6]!;
    const r7 = fieldReaders[7]!;
    const r8 = fieldReaders[8]!;
    const r9 = fieldReaders[9]!;
    const f0 = fieldNames[0]!;
    const f1 = fieldNames[1]!;
    const f2 = fieldNames[2]!;
    const f3 = fieldNames[3]!;
    const f4 = fieldNames[4]!;
    const f5 = fieldNames[5]!;
    const f6 = fieldNames[6]!;
    const f7 = fieldNames[7]!;
    const f8 = fieldNames[8]!;
    const f9 = fieldNames[9]!;

    switch (fieldCount) {
      case 0:
        return (_tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            result[i] = {};
          }
        };
      case 1:
        return (tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            const value: Record<string, unknown> = {};
            value[f0] = r0(tap);
            result[i] = value;
          }
        };
      case 2:
        return (tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            const value: Record<string, unknown> = {};
            value[f0] = r0(tap);
            value[f1] = r1(tap);
            result[i] = value;
          }
        };
      case 3:
        return (tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            const value: Record<string, unknown> = {};
            value[f0] = r0(tap);
            value[f1] = r1(tap);
            value[f2] = r2(tap);
            result[i] = value;
          }
        };
      case 4:
        return (tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            const value: Record<string, unknown> = {};
            value[f0] = r0(tap);
            value[f1] = r1(tap);
            value[f2] = r2(tap);
            value[f3] = r3(tap);
            result[i] = value;
          }
        };
      case 5:
        return (tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            const value: Record<string, unknown> = {};
            value[f0] = r0(tap);
            value[f1] = r1(tap);
            value[f2] = r2(tap);
            value[f3] = r3(tap);
            value[f4] = r4(tap);
            result[i] = value;
          }
        };
      case 6:
        return (tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            const value: Record<string, unknown> = {};
            value[f0] = r0(tap);
            value[f1] = r1(tap);
            value[f2] = r2(tap);
            value[f3] = r3(tap);
            value[f4] = r4(tap);
            value[f5] = r5(tap);
            result[i] = value;
          }
        };
      case 7:
        return (tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            const value: Record<string, unknown> = {};
            value[f0] = r0(tap);
            value[f1] = r1(tap);
            value[f2] = r2(tap);
            value[f3] = r3(tap);
            value[f4] = r4(tap);
            value[f5] = r5(tap);
            value[f6] = r6(tap);
            result[i] = value;
          }
        };
      case 8:
        return (tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            const value: Record<string, unknown> = {};
            value[f0] = r0(tap);
            value[f1] = r1(tap);
            value[f2] = r2(tap);
            value[f3] = r3(tap);
            value[f4] = r4(tap);
            value[f5] = r5(tap);
            value[f6] = r6(tap);
            value[f7] = r7(tap);
            result[i] = value;
          }
        };
      case 9:
        return (tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            const value: Record<string, unknown> = {};
            value[f0] = r0(tap);
            value[f1] = r1(tap);
            value[f2] = r2(tap);
            value[f3] = r3(tap);
            value[f4] = r4(tap);
            value[f5] = r5(tap);
            value[f6] = r6(tap);
            value[f7] = r7(tap);
            value[f8] = r8(tap);
            result[i] = value;
          }
        };
      default:
        return (tap, result, startIndex, count) => {
          const end = startIndex + count;
          for (let i = startIndex; i < end; i++) {
            const value: Record<string, unknown> = {};
            value[f0] = r0(tap);
            value[f1] = r1(tap);
            value[f2] = r2(tap);
            value[f3] = r3(tap);
            value[f4] = r4(tap);
            value[f5] = r5(tap);
            value[f6] = r6(tap);
            value[f7] = r7(tap);
            value[f8] = r8(tap);
            value[f9] = r9(tap);
            result[i] = value;
          }
        };
    }
  }

  static #compileReader(type: Type): CompiledReader {
    if (type instanceof NullType) {
      return () => Promise.resolve(null);
    }
    if (type instanceof BooleanType) {
      return (tap) => tap.readBoolean();
    }
    if (type instanceof IntType) {
      return (tap) => tap.readInt();
    }
    if (type instanceof LongType) {
      return (tap) => tap.readLong();
    }
    if (type instanceof FloatType) {
      return (tap) => tap.readFloat();
    }
    if (type instanceof DoubleType) {
      return (tap) => tap.readDouble();
    }
    if (type instanceof BytesType) {
      return (tap) => tap.readBytes();
    }
    if (type instanceof StringType) {
      return (tap) => tap.readString();
    }
    // Fallback for complex types (arrays, maps, unions, enums, fixed, etc.)
    return (tap) => type.read(tap);
  }

  static #compileSyncReader(type: Type): CompiledSyncReader {
    if (type instanceof NullType) {
      return () => null;
    }
    if (type instanceof BooleanType) {
      return (tap) => tap.readBoolean();
    }
    if (type instanceof IntType) {
      return (tap) => tap.readInt();
    }
    if (type instanceof LongType) {
      return (tap) => tap.readLong();
    }
    if (type instanceof FloatType) {
      return (tap) => tap.readFloat();
    }
    if (type instanceof DoubleType) {
      return (tap) => tap.readDouble();
    }
    if (type instanceof BytesType) {
      return (tap) => tap.readBytes();
    }
    if (type instanceof StringType) {
      return (tap) => tap.readString();
    }
    // Fallback for complex types (arrays, maps, unions, enums, fixed, etc.)
    return (tap) => type.readSync(tap);
  }
}

/**
 * Interpreted reader strategy that delegates to type.read() methods.
 *
 * This strategy provides simpler, more straightforward deserialization by
 * delegating all reads to the type's own read methods. It's useful for:
 * - Debugging and testing
 * - Scenarios where compilation overhead isn't worth the performance gain
 * - Future extensibility without recompilation
 */
export class InterpretedReaderStrategy implements RecordReaderStrategy {
  /**
   * Compiles an async reader that delegates to the type's read method.
   */
  public compileFieldReader(
    fieldType: Type,
    _getRecordReader: (type: Type) => CompiledReader,
  ): CompiledReader {
    return (tap) => fieldType.read(tap);
  }

  /**
   * Compiles a sync reader that delegates to the type's readSync method.
   */
  public compileSyncFieldReader(
    fieldType: Type,
    _getRecordReader: (type: Type) => CompiledSyncReader,
  ): CompiledSyncReader {
    return (tap) => fieldType.readSync(tap);
  }

  /**
   * Assembles an async record reader from field readers.
   */
  public assembleRecordReader(
    context: RecordReaderContext,
    fieldReaders: CompiledReader[],
  ): CompiledReader {
    const { fieldNames } = context;
    const fieldCount = fieldNames.length;

    return async (tap) => {
      const result: Record<string, unknown> = {};
      for (let i = 0; i < fieldCount; i++) {
        setRecordField(
          result,
          fieldNames[i]!,
          await fieldReaders[i]!(tap),
        );
      }
      return result;
    };
  }

  /**
   * Assembles a sync record reader from field readers.
   */
  public assembleSyncRecordReader(
    context: RecordReaderContext,
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncReader {
    const { fieldNames } = context;
    const fieldCount = fieldNames.length;

    return (tap) => {
      const result: Record<string, unknown> = {};
      for (let i = 0; i < fieldCount; i++) {
        setRecordField(result, fieldNames[i]!, fieldReaders[i]!(tap));
      }
      return result;
    };
  }
}

/**
 * Default strategy instance used when no strategy is specified.
 */
export const defaultReaderStrategy: RecordReaderStrategy =
  new CompiledReaderStrategy();
