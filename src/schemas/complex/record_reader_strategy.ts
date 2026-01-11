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
}

/**
 * Checks if a type is a RecordType without importing it (avoids circular dependency).
 * Uses a static marker property instead of constructor name to avoid minification issues.
 */
function isRecordType(type: Type): boolean {
  return "__AVRO_RECORD_TYPE__" in type.constructor;
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
        result[fieldNames[i]!] = await fieldReaders[i]!(tap);
      }
      return result;
    };
  }

  /**
   * Assembles a sync record reader from field readers.
   *
   * Optimization: For small records (<=10 fields), generates specialized readers
   * with direct property assignment to avoid loop overhead. For larger records,
   * falls back to a loop implementation.
   */
  public assembleSyncRecordReader(
    context: RecordReaderContext,
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncReader {
    const { fieldNames } = context;
    const fieldCount = fieldNames.length;

    if (fieldCount <= 10) {
      return this.#generateSpecializedSyncReader(fieldNames, fieldReaders);
    }

    return (tap) => {
      const result: Record<string, unknown> = {};
      for (let i = 0; i < fieldCount; i++) {
        result[fieldNames[i]!] = fieldReaders[i]!(tap);
      }
      return result;
    };
  }

  #generateSpecializedSyncReader(
    fieldNames: string[],
    fieldReaders: CompiledSyncReader[],
  ): CompiledSyncReader {
    const n = fieldNames.length;
    const r = fieldReaders;
    const f = fieldNames;

    switch (n) {
      case 0:
        return () => ({});
      case 1:
        return (tap) => ({ [f[0]!]: r[0]!(tap) });
      case 2:
        return (tap) => ({ [f[0]!]: r[0]!(tap), [f[1]!]: r[1]!(tap) });
      case 3:
        return (tap) => ({
          [f[0]!]: r[0]!(tap),
          [f[1]!]: r[1]!(tap),
          [f[2]!]: r[2]!(tap),
        });
      case 4:
        return (tap) => ({
          [f[0]!]: r[0]!(tap),
          [f[1]!]: r[1]!(tap),
          [f[2]!]: r[2]!(tap),
          [f[3]!]: r[3]!(tap),
        });
      case 5:
        return (tap) => ({
          [f[0]!]: r[0]!(tap),
          [f[1]!]: r[1]!(tap),
          [f[2]!]: r[2]!(tap),
          [f[3]!]: r[3]!(tap),
          [f[4]!]: r[4]!(tap),
        });
      case 6:
        return (tap) => ({
          [f[0]!]: r[0]!(tap),
          [f[1]!]: r[1]!(tap),
          [f[2]!]: r[2]!(tap),
          [f[3]!]: r[3]!(tap),
          [f[4]!]: r[4]!(tap),
          [f[5]!]: r[5]!(tap),
        });
      case 7:
        return (tap) => ({
          [f[0]!]: r[0]!(tap),
          [f[1]!]: r[1]!(tap),
          [f[2]!]: r[2]!(tap),
          [f[3]!]: r[3]!(tap),
          [f[4]!]: r[4]!(tap),
          [f[5]!]: r[5]!(tap),
          [f[6]!]: r[6]!(tap),
        });
      case 8:
        return (tap) => ({
          [f[0]!]: r[0]!(tap),
          [f[1]!]: r[1]!(tap),
          [f[2]!]: r[2]!(tap),
          [f[3]!]: r[3]!(tap),
          [f[4]!]: r[4]!(tap),
          [f[5]!]: r[5]!(tap),
          [f[6]!]: r[6]!(tap),
          [f[7]!]: r[7]!(tap),
        });
      case 9:
        return (tap) => ({
          [f[0]!]: r[0]!(tap),
          [f[1]!]: r[1]!(tap),
          [f[2]!]: r[2]!(tap),
          [f[3]!]: r[3]!(tap),
          [f[4]!]: r[4]!(tap),
          [f[5]!]: r[5]!(tap),
          [f[6]!]: r[6]!(tap),
          [f[7]!]: r[7]!(tap),
          [f[8]!]: r[8]!(tap),
        });
      default:
        return (tap) => ({
          [f[0]!]: r[0]!(tap),
          [f[1]!]: r[1]!(tap),
          [f[2]!]: r[2]!(tap),
          [f[3]!]: r[3]!(tap),
          [f[4]!]: r[4]!(tap),
          [f[5]!]: r[5]!(tap),
          [f[6]!]: r[6]!(tap),
          [f[7]!]: r[7]!(tap),
          [f[8]!]: r[8]!(tap),
          [f[9]!]: r[9]!(tap),
        });
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
        result[fieldNames[i]!] = await fieldReaders[i]!(tap);
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
        result[fieldNames[i]!] = fieldReaders[i]!(tap);
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
