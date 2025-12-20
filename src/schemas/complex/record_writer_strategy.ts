import type { WritableTapLike } from "../../serialization/tap.ts";
import type { SyncWritableTapLike } from "../../serialization/sync_tap.ts";
import type { Type } from "../type.ts";
import { throwInvalidError } from "../error.ts";
import { BooleanType } from "../primitive/boolean_type.ts";
import { BytesType } from "../primitive/bytes_type.ts";
import { DoubleType } from "../primitive/double_type.ts";
import { FloatType } from "../primitive/float_type.ts";
import { IntType } from "../primitive/int_type.ts";
import { LongType } from "../primitive/long_type.ts";
import { NullType } from "../primitive/null_type.ts";
import { StringType } from "../primitive/string_type.ts";

/**
 * Compiled async writer function signature.
 */
export type CompiledWriter = (
  tap: WritableTapLike,
  value: unknown,
) => Promise<void>;

/**
 * Compiled sync writer function signature.
 */
export type CompiledSyncWriter = (
  tap: SyncWritableTapLike,
  value: unknown,
) => void;

/**
 * Context passed to strategy methods for assembling record writers.
 */
export interface RecordWriterContext {
  /** Field names in order. */
  fieldNames: string[];
  /** Field types in order. */
  fieldTypes: Type[];
  /** Default value getters for each field (undefined if no default). */
  fieldDefaultGetters: Array<(() => unknown) | undefined>;
  /** Whether each field has a default value. */
  fieldHasDefault: boolean[];
  /** Whether to validate values during writes. */
  validate: boolean;
  /** Reference to the parent RecordType for error reporting. */
  recordType: Type;
  /** Callback to check if a value is a valid record object. */
  isRecord: (value: unknown) => value is Record<string, unknown>;
}

/**
 * Strategy interface for compiling record field writers.
 *
 * Implementations can choose different approaches:
 * - CompiledWriterStrategy: Inlines primitive tap methods for performance
 * - InterpretedWriterStrategy: Delegates to type.write() for simplicity
 */
export interface RecordWriterStrategy {
  /**
   * Compiles an async writer for a single field type.
   * @param fieldType The type of the field.
   * @param validate Whether to validate values.
   * @param getRecordWriter Callback to get a writer for nested RecordTypes (handles recursion).
   */
  compileFieldWriter(
    fieldType: Type,
    validate: boolean,
    getRecordWriter: (type: Type, validate: boolean) => CompiledWriter,
  ): CompiledWriter;

  /**
   * Compiles a sync writer for a single field type.
   * @param fieldType The type of the field.
   * @param validate Whether to validate values.
   * @param getRecordWriter Callback to get a writer for nested RecordTypes (handles recursion).
   */
  compileSyncFieldWriter(
    fieldType: Type,
    validate: boolean,
    getRecordWriter: (type: Type, validate: boolean) => CompiledSyncWriter,
  ): CompiledSyncWriter;

  /**
   * Assembles field writers into a complete async record writer.
   * @param context The record writer context.
   * @param fieldWriters Pre-compiled writers for each field.
   */
  assembleRecordWriter(
    context: RecordWriterContext,
    fieldWriters: CompiledWriter[],
  ): CompiledWriter;

  /**
   * Assembles field writers into a complete sync record writer.
   * @param context The record writer context.
   * @param fieldWriters Pre-compiled writers for each field.
   */
  assembleSyncRecordWriter(
    context: RecordWriterContext,
    fieldWriters: CompiledSyncWriter[],
  ): CompiledSyncWriter;
}

/**
 * Checks if a type is a RecordType without importing it (avoids circular dependency).
 */
function isRecordType(type: Type): boolean {
  return type.constructor.name === "RecordType";
}

/**
 * Compiled writer strategy that inlines primitive tap methods for performance.
 *
 * This is the default strategy that provides optimized serialization by:
 * - Inlining primitive type writes directly to tap methods
 * - Using unchecked writes when validation is disabled
 * - Supporting recursive record types via the getRecordWriter callback
 */
export class CompiledWriterStrategy implements RecordWriterStrategy {
  /**
   * Compiles an async writer for a single field type with primitive inlining.
   */
  public compileFieldWriter(
    fieldType: Type,
    validate: boolean,
    getRecordWriter: (type: Type, validate: boolean) => CompiledWriter,
  ): CompiledWriter {
    if (isRecordType(fieldType)) {
      return getRecordWriter(fieldType, validate);
    }

    if (!validate) {
      return CompiledWriterStrategy.#compileUncheckedWriter(fieldType);
    }

    return (tap, value) => fieldType.write(tap, value as never);
  }

  /**
   * Compiles a sync writer for a single field type with primitive inlining.
   */
  public compileSyncFieldWriter(
    fieldType: Type,
    validate: boolean,
    getRecordWriter: (type: Type, validate: boolean) => CompiledSyncWriter,
  ): CompiledSyncWriter {
    if (isRecordType(fieldType)) {
      return getRecordWriter(fieldType, validate);
    }

    if (!validate) {
      return CompiledWriterStrategy.#compileUncheckedSyncWriter(fieldType);
    }

    return (tap, value) => fieldType.writeSync(tap, value as never);
  }

  /**
   * Assembles an async record writer from field writers.
   */
  public assembleRecordWriter(
    context: RecordWriterContext,
    fieldWriters: CompiledWriter[],
  ): CompiledWriter {
    const {
      fieldNames,
      fieldDefaultGetters,
      fieldHasDefault,
      validate,
      recordType,
      isRecord,
    } = context;
    const fieldCount = fieldNames.length;
    const hasAnyDefaults = fieldHasDefault.some((hasDefault) => hasDefault);

    return async (tap, value) => {
      if (validate && !isRecord(value)) {
        throwInvalidError([], value, recordType);
      }
      const record = value as Record<string, unknown>;
      if (!validate && !hasAnyDefaults) {
        for (let i = 0; i < fieldCount; i++) {
          const name = fieldNames[i]!;
          await fieldWriters[i]!(tap, record[name]);
        }
        return;
      }
      for (let i = 0; i < fieldCount; i++) {
        const name = fieldNames[i]!;
        const hasValue = Object.hasOwn(record, name);
        let toWrite: unknown;
        if (hasValue) {
          toWrite = record[name];
        } else {
          const getter = fieldDefaultGetters[i];
          if (getter) {
            toWrite = getter();
          } else {
            if (validate) {
              throwInvalidError([name], undefined, recordType);
            }
            toWrite = undefined;
          }
        }
        await fieldWriters[i]!(tap, toWrite);
      }
    };
  }

  /**
   * Assembles a sync record writer from field writers.
   */
  public assembleSyncRecordWriter(
    context: RecordWriterContext,
    fieldWriters: CompiledSyncWriter[],
  ): CompiledSyncWriter {
    const {
      fieldNames,
      fieldDefaultGetters,
      fieldHasDefault,
      validate,
      recordType,
      isRecord,
    } = context;
    const fieldCount = fieldNames.length;
    const hasAnyDefaults = fieldHasDefault.some((hasDefault) => hasDefault);

    return (tap, value) => {
      if (validate && !isRecord(value)) {
        throwInvalidError([], value, recordType);
      }
      const record = value as Record<string, unknown>;
      if (!validate && !hasAnyDefaults) {
        for (let i = 0; i < fieldCount; i++) {
          const name = fieldNames[i]!;
          fieldWriters[i]!(tap, record[name]);
        }
        return;
      }
      for (let i = 0; i < fieldCount; i++) {
        const name = fieldNames[i]!;
        const hasValue = Object.hasOwn(record, name);
        let toWrite: unknown;
        if (hasValue) {
          toWrite = record[name];
        } else {
          const getter = fieldDefaultGetters[i];
          if (getter) {
            toWrite = getter();
          } else {
            if (validate) {
              throwInvalidError([name], undefined, recordType);
            }
            toWrite = undefined;
          }
        }
        fieldWriters[i]!(tap, toWrite);
      }
    };
  }

  static #compileUncheckedWriter(type: Type): CompiledWriter {
    if (type instanceof NullType) {
      return async () => {};
    }
    if (type instanceof BooleanType) {
      return (tap, value) => tap.writeBoolean(value as boolean);
    }
    if (type instanceof IntType) {
      return (tap, value) => tap.writeInt(value as number);
    }
    if (type instanceof LongType) {
      return (tap, value) => tap.writeLong(value as bigint);
    }
    if (type instanceof FloatType) {
      return (tap, value) => tap.writeFloat(value as number);
    }
    if (type instanceof DoubleType) {
      return (tap, value) => tap.writeDouble(value as number);
    }
    if (type instanceof BytesType) {
      return (tap, value) => tap.writeBytes(value as Uint8Array);
    }
    if (type instanceof StringType) {
      return (tap, value) => tap.writeString(value as string);
    }
    return (tap, value) => type.writeUnchecked(tap, value as never);
  }

  static #compileUncheckedSyncWriter(type: Type): CompiledSyncWriter {
    if (type instanceof NullType) {
      return () => {};
    }
    if (type instanceof BooleanType) {
      return (tap, value) => tap.writeBoolean(value as boolean);
    }
    if (type instanceof IntType) {
      return (tap, value) => tap.writeInt(value as number);
    }
    if (type instanceof LongType) {
      return (tap, value) => tap.writeLong(value as bigint);
    }
    if (type instanceof FloatType) {
      return (tap, value) => tap.writeFloat(value as number);
    }
    if (type instanceof DoubleType) {
      return (tap, value) => tap.writeDouble(value as number);
    }
    if (type instanceof BytesType) {
      return (tap, value) => tap.writeBytes(value as Uint8Array);
    }
    if (type instanceof StringType) {
      return (tap, value) => tap.writeString(value as string);
    }
    return (tap, value) => type.writeSyncUnchecked(tap, value as never);
  }
}

/**
 * Interpreted writer strategy that delegates to type.write() methods.
 *
 * This strategy provides simpler, more straightforward serialization by
 * delegating all writes to the type's own write methods. It's useful for:
 * - Debugging and testing
 * - Scenarios where compilation overhead isn't worth the performance gain
 * - Future extensibility without recompilation
 */
export class InterpretedWriterStrategy implements RecordWriterStrategy {
  /**
   * Compiles an async writer that delegates to the type's write method.
   */
  public compileFieldWriter(
    fieldType: Type,
    validate: boolean,
    _getRecordWriter: (type: Type, validate: boolean) => CompiledWriter,
  ): CompiledWriter {
    if (validate) {
      return (tap, value) => fieldType.write(tap, value as never);
    }
    return (tap, value) => fieldType.writeUnchecked(tap, value as never);
  }

  /**
   * Compiles a sync writer that delegates to the type's writeSync method.
   */
  public compileSyncFieldWriter(
    fieldType: Type,
    validate: boolean,
    _getRecordWriter: (type: Type, validate: boolean) => CompiledSyncWriter,
  ): CompiledSyncWriter {
    if (validate) {
      return (tap, value) => fieldType.writeSync(tap, value as never);
    }
    return (tap, value) => fieldType.writeSyncUnchecked(tap, value as never);
  }

  /**
   * Assembles an async record writer from field writers.
   */
  public assembleRecordWriter(
    context: RecordWriterContext,
    fieldWriters: CompiledWriter[],
  ): CompiledWriter {
    const {
      fieldNames,
      fieldDefaultGetters,
      fieldHasDefault,
      validate,
      recordType,
      isRecord,
    } = context;
    const fieldCount = fieldNames.length;
    const hasAnyDefaults = fieldHasDefault.some((hasDefault) => hasDefault);

    return async (tap, value) => {
      if (validate && !isRecord(value)) {
        throwInvalidError([], value, recordType);
      }
      const record = value as Record<string, unknown>;
      if (!validate && !hasAnyDefaults) {
        for (let i = 0; i < fieldCount; i++) {
          const name = fieldNames[i]!;
          await fieldWriters[i]!(tap, record[name]);
        }
        return;
      }
      for (let i = 0; i < fieldCount; i++) {
        const name = fieldNames[i]!;
        const hasValue = Object.hasOwn(record, name);
        let toWrite: unknown;
        if (hasValue) {
          toWrite = record[name];
        } else {
          const getter = fieldDefaultGetters[i];
          if (getter) {
            toWrite = getter();
          } else {
            if (validate) {
              throwInvalidError([name], undefined, recordType);
            }
            toWrite = undefined;
          }
        }
        await fieldWriters[i]!(tap, toWrite);
      }
    };
  }

  /**
   * Assembles a sync record writer from field writers.
   */
  public assembleSyncRecordWriter(
    context: RecordWriterContext,
    fieldWriters: CompiledSyncWriter[],
  ): CompiledSyncWriter {
    const {
      fieldNames,
      fieldDefaultGetters,
      fieldHasDefault,
      validate,
      recordType,
      isRecord,
    } = context;
    const fieldCount = fieldNames.length;
    const hasAnyDefaults = fieldHasDefault.some((hasDefault) => hasDefault);

    return (tap, value) => {
      if (validate && !isRecord(value)) {
        throwInvalidError([], value, recordType);
      }
      const record = value as Record<string, unknown>;
      if (!validate && !hasAnyDefaults) {
        for (let i = 0; i < fieldCount; i++) {
          const name = fieldNames[i]!;
          fieldWriters[i]!(tap, record[name]);
        }
        return;
      }
      for (let i = 0; i < fieldCount; i++) {
        const name = fieldNames[i]!;
        const hasValue = Object.hasOwn(record, name);
        let toWrite: unknown;
        if (hasValue) {
          toWrite = record[name];
        } else {
          const getter = fieldDefaultGetters[i];
          if (getter) {
            toWrite = getter();
          } else {
            if (validate) {
              throwInvalidError([name], undefined, recordType);
            }
            toWrite = undefined;
          }
        }
        fieldWriters[i]!(tap, toWrite);
      }
    };
  }
}

/**
 * Default strategy instance used when no strategy is specified.
 */
export const defaultWriterStrategy: RecordWriterStrategy =
  new CompiledWriterStrategy();
