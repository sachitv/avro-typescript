import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import { NamedType } from "./named_type.ts";
import type { Resolver } from "../resolver.ts";
import type { JSONType, Type } from "../type.ts";
import type { ErrorHook } from "../error.ts";
import type { ResolvedNames } from "./resolve_names.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/tap_sync.ts";

// Re-export extracted modules for backward compatibility
export {
  RecordField,
  type RecordFieldOrder,
  type RecordFieldParams,
} from "./record_field.ts";
export { type FieldMapping, RecordResolver } from "./record_resolver.ts";
export {
  type CompiledSyncWriter,
  type CompiledWriter,
  CompiledWriterStrategy,
  defaultWriterStrategy,
  InterpretedWriterStrategy,
  type RecordWriterContext,
  type RecordWriterStrategy,
} from "./record_writer_strategy.ts";
export { RecordWriterCache } from "./record_writer_cache.ts";
export {
  type CompiledReader,
  CompiledReaderStrategy,
  type CompiledSyncReader,
  defaultReaderStrategy,
  InterpretedReaderStrategy,
  type RecordReaderContext,
  type RecordReaderStrategy,
} from "./record_reader_strategy.ts";
export { RecordReaderCache } from "./record_reader_cache.ts";

import {
  RecordField,
  type RecordFieldOrder,
  type RecordFieldParams,
} from "./record_field.ts";
import { type FieldMapping, RecordResolver } from "./record_resolver.ts";
import type {
  CompiledSyncWriter,
  CompiledWriter,
  RecordWriterStrategy,
} from "./record_writer_strategy.ts";
import { defaultWriterStrategy } from "./record_writer_strategy.ts";
import { RecordWriterCache } from "./record_writer_cache.ts";
import type {
  CompiledReader,
  CompiledSyncReader,
  RecordReaderStrategy,
} from "./record_reader_strategy.ts";
import { defaultReaderStrategy } from "./record_reader_strategy.ts";
import { RecordReaderCache } from "./record_reader_cache.ts";

/**
 * Parameters for creating a RecordType.
 */
export interface RecordTypeParams extends ResolvedNames {
  /**
   * Fields can be provided eagerly as an array or lazily via a thunk (a
   * parameterless function returning the array). The lazy form lets schema
   * factories register the record under its name before constructing field
   * types, enabling recursive references to the record.
   */
  fields: RecordFieldParams[] | (() => RecordFieldParams[]);
  /**
   * Whether to validate values during write operations.
   *
   * When set to `false`, record writes may skip runtime type checks in hot
   * paths for performance. This is unsafe for untrusted inputs.
   */
  validate?: boolean;
  /**
   * Optional writer strategy for customizing how record writers are compiled.
   *
   * - \`CompiledWriterStrategy\` (default): Inlines primitive tap methods for performance.
   * - \`InterpretedWriterStrategy\`: Delegates to type.write() for simplicity.
   *
   * Custom strategies can be provided for instrumentation, debugging, or
   * alternative compilation approaches.
   */
  writerStrategy?: RecordWriterStrategy;
  /**
   * Optional reader strategy for customizing how record readers are compiled.
   *
   * - \`CompiledReaderStrategy\` (default): Inlines primitive tap methods for performance.
   * - \`InterpretedReaderStrategy\`: Delegates to type.read() for simplicity.
   */
  readerStrategy?: RecordReaderStrategy;
}

/**
 * Avro \`record\` type supporting ordered fields, aliases, and schema evolution.
 */
export class RecordType extends NamedType<Record<string, unknown>> {
  static readonly __AVRO_RECORD_TYPE__ = true;
  #fields: RecordField[];
  #fieldNameToIndex: Map<string, number>;
  #fieldsThunk?: () => RecordFieldParams[];
  #writerCache: RecordWriterCache;
  #readerCache: RecordReaderCache;
  #fieldNames: string[];
  #fieldTypes: Type[];
  #fieldHasDefault: boolean[];
  #fieldDefaultGetters: Array<(() => unknown) | undefined>;

  /**
   * Creates a new RecordType.
   * @param params The record type parameters.
   */
  constructor(params: RecordTypeParams) {
    const { fields, validate, writerStrategy, readerStrategy, ...names } =
      params;
    super(names, validate ?? true);

    this.#fields = [];
    this.#fieldNameToIndex = new Map();
    this.#fieldNames = [];
    this.#fieldTypes = [];
    this.#fieldHasDefault = [];
    this.#fieldDefaultGetters = [];
    this.#writerCache = new RecordWriterCache(
      writerStrategy ?? defaultWriterStrategy,
    );
    this.#readerCache = new RecordReaderCache(
      readerStrategy ?? defaultReaderStrategy,
    );

    if (typeof fields === "function") {
      // Defer field materialization until the first time the record is used.
      // This mirrors the classic Avro parsing strategy where named types are
      // registered before their fields are resolved, allowing recursive schemas.
      this.#fieldsThunk = fields;
    } else {
      this.#setFields(fields);
    }
  }

  /**
   * Gets the writer strategy used by this record type.
   */
  public getWriterStrategy(): RecordWriterStrategy {
    return this.#writerCache.getStrategy();
  }

  public getReaderStrategy(): RecordReaderStrategy {
    return this.#readerCache.getStrategy();
  }

  /**
   * Gets the fields of the record.
   */
  public getFields(): ReadonlyArray<RecordField> {
    this.#ensureFields();
    return this.#fields.slice();
  }

  /**
   * Gets a specific field by name.
   */
  public getField(name: string): RecordField | undefined {
    this.#ensureFields();
    const index = this.#fieldNameToIndex.get(name);
    if (index === undefined) {
      return undefined;
    }
    return this.#fields[index];
  }

  /**
   * Checks if the given value conforms to this record type.
   * @param value The value to check.
   * @param errorHook Optional error hook for validation errors.
   * @param path The current path in the schema for error reporting.
   * @returns True if the value is valid, false otherwise.
   */
  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    this.#ensureFields();
    if (!this.#isRecord(value)) {
      if (errorHook) {
        errorHook(path.slice(), value, this);
      }
      return false;
    }

    const record = value as Record<string, unknown>;

    for (const field of this.#fields) {
      if (!this.#checkField(field, record, errorHook, path)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Writes the record without runtime validation.
   */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: Record<string, unknown>,
  ): Promise<void> {
    this.#ensureFields();
    const writer = this.#getOrCreateCompiledWriter(false);
    await writer(tap, value);
  }

  /**
   * Writes the record synchronously without runtime validation.
   */
  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: Record<string, unknown>,
  ): void {
    this.#ensureFields();
    const writer = this.#getOrCreateCompiledSyncWriter(false);
    writer(tap, value);
  }

  #getOrCreateCompiledWriter(validate: boolean): CompiledWriter {
    this.#ensureFields();
    return this.#writerCache.getOrCreateWriter(
      validate,
      {
        fieldNames: this.#fieldNames,
        fieldTypes: this.#fieldTypes,
        fieldDefaultGetters: this.#fieldDefaultGetters,
        fieldHasDefault: this.#fieldHasDefault,
        validate,
        recordType: this,
        isRecord: this.#isRecord.bind(this),
      },
      (type, shouldValidate) => {
        if (type instanceof RecordType) {
          return type.#getOrCreateCompiledWriter(shouldValidate);
        }
        return (tap, value) => {
          if (shouldValidate) {
            return type.write(tap, value as never);
          }
          return type.writeUnchecked(tap, value as never);
        };
      },
    );
  }

  #getOrCreateCompiledSyncWriter(validate: boolean): CompiledSyncWriter {
    this.#ensureFields();
    return this.#writerCache.getOrCreateSyncWriter(
      validate,
      {
        fieldNames: this.#fieldNames,
        fieldTypes: this.#fieldTypes,
        fieldDefaultGetters: this.#fieldDefaultGetters,
        fieldHasDefault: this.#fieldHasDefault,
        validate,
        recordType: this,
        isRecord: this.#isRecord.bind(this),
      },
      (type, shouldValidate) => {
        if (type instanceof RecordType) {
          return type.#getOrCreateCompiledSyncWriter(shouldValidate);
        }
        return (tap, value) => {
          if (shouldValidate) {
            return type.writeSync(tap, value as never);
          }
          return type.writeSyncUnchecked(tap, value as never);
        };
      },
    );
  }

  /**
   * Reads a record value from the tap.
   * @param tap The readable tap to read from.
   * @returns The read record value.
   */
  public override async read(
    tap: ReadableTapLike,
  ): Promise<Record<string, unknown>> {
    this.#ensureFields();
    const reader = this.#getOrCreateCompiledReader();
    return (await reader(tap)) as Record<string, unknown>;
  }

  /**
   * Reads every field synchronously using each field type's sync reader.
   */
  public override readSync(tap: SyncReadableTapLike): Record<string, unknown> {
    this.#ensureFields();
    const reader = this.#getOrCreateCompiledSyncReader();
    return reader(tap) as Record<string, unknown>;
  }

  #getOrCreateCompiledReader(): CompiledReader {
    this.#ensureFields();
    return this.#readerCache.getOrCreateReader(
      {
        fieldNames: this.#fieldNames,
        fieldTypes: this.#fieldTypes,
      },
      // This callback is only invoked by the strategy for RecordType fields.
      // Non-record types are handled directly by the strategy's compileFieldReader.
      (type) => (type as RecordType).#getOrCreateCompiledReader(),
    );
  }

  #getOrCreateCompiledSyncReader(): CompiledSyncReader {
    this.#ensureFields();
    return this.#readerCache.getOrCreateSyncReader(
      {
        fieldNames: this.#fieldNames,
        fieldTypes: this.#fieldTypes,
      },
      // This callback is only invoked by the strategy for RecordType fields.
      // Non-record types are handled directly by the strategy's compileSyncFieldReader.
      (type) => (type as RecordType).#getOrCreateCompiledSyncReader(),
    );
  }

  /**
   * Skips a record value in the tap.
   * @param tap The readable tap to skip in.
   */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    this.#ensureFields();
    for (const field of this.#fields) {
      await field.getType().skip(tap);
    }
  }

  /**
   * Skips all fields synchronously without materializing values.
   */
  public override skipSync(tap: SyncReadableTapLike): void {
    this.#ensureFields();
    for (const field of this.#fields) {
      field.getType().skipSync(tap);
    }
  }

  /**
   * Clones a record value, ensuring it conforms to the schema.
   * @param value The value to clone.
   * @returns The cloned record value.
   */
  public override cloneFromValue(value: unknown): Record<string, unknown> {
    this.#ensureFields();
    if (!this.#isRecord(value)) {
      throw new Error("Cannot clone non-record value.");
    }

    const recordValue = value as Record<string, unknown>;
    const result: Record<string, unknown> = {};
    for (const field of this.#fields) {
      this.#cloneField(field, recordValue, result);
    }
    return result;
  }

  /**
   * Compares two record values for ordering.
   * @param val1 The first record value.
   * @param val2 The second record value.
   * @returns A negative number if val1 < val2, zero if equal, positive if val1 > val2.
   */
  public override compare(
    val1: Record<string, unknown>,
    val2: Record<string, unknown>,
  ): number {
    this.#ensureFields();
    if (!this.#isRecord(val1) || !this.#isRecord(val2)) {
      throw new Error("Record comparison requires object values.");
    }

    for (const field of this.#fields) {
      const order = field.getOrder();
      if (order === "ignore") {
        continue;
      }

      const v1 = this.#getComparableValue(val1, field);
      const v2 = this.#getComparableValue(val2, field);
      let comparison = field.getType().compare(v1, v2);
      if (comparison !== 0) {
        if (order === "descending") {
          comparison = -comparison;
        }
        return comparison;
      }
    }

    return 0;
  }

  /**
   * Generates a random record value conforming to this schema.
   * @returns A random record value.
   */
  public override random(): Record<string, unknown> {
    this.#ensureFields();
    const result: Record<string, unknown> = {};
    for (const field of this.#fields) {
      result[field.getName()] = field.getType().random();
    }
    return result;
  }

  /**
   * Converts this record type to its JSON schema representation.
   * @returns The JSON representation of the record type.
   */
  public override toJSON(): JSONType {
    this.#ensureFields();
    const fieldsJson = this.#fields.map((field) => {
      const fieldJson: JSONType = {
        name: field.getName(),
        type: field.getType().toJSON(),
      };
      if (field.hasDefault()) {
        (fieldJson as Record<string, unknown>).default = field.getDefault();
      }
      const aliases = field.getAliases();
      if (aliases.length > 0) {
        (fieldJson as Record<string, unknown>).aliases = aliases;
      }
      if (field.getOrder() !== "ascending") {
        (fieldJson as Record<string, unknown>).order = field.getOrder();
      }
      return fieldJson;
    });

    return {
      name: this.getFullName(),
      type: "record",
      fields: fieldsJson,
    };
  }

  /**
   * Matches two record values from the taps for comparison.
   * @param tap1 The first readable tap.
   * @param tap2 The second readable tap.
   * @returns A comparison result.
   */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    this.#ensureFields();
    for (const field of this.#fields) {
      const order = this.#getOrderValue(field.getOrder());
      const type = field.getType();
      if (order !== 0) {
        const result = (await type.match(tap1, tap2)) * order;
        if (result !== 0) {
          return result;
        }
      } else {
        await type.skip(tap1);
        await type.skip(tap2);
      }
    }
    return 0;
  }

  /**
   * Matches two buffers of serialized records synchronously, honoring field order.
   */
  public override matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    this.#ensureFields();
    for (const field of this.#fields) {
      const order = this.#getOrderValue(field.getOrder());
      const type = field.getType();
      if (order !== 0) {
        const result = type.matchSync(tap1, tap2) * order;
        if (result !== 0) {
          return result;
        }
      } else {
        type.skipSync(tap1);
        type.skipSync(tap2);
      }
    }
    return 0;
  }

  #ensureFields(): void {
    if (this.#fieldsThunk) {
      const builder = this.#fieldsThunk;
      this.#fieldsThunk = undefined;
      const resolved = builder();
      this.#setFields(resolved);
    }
  }

  #setFields(candidate: unknown): void {
    if (!Array.isArray(candidate)) {
      throw new Error("RecordType requires a fields array.");
    }

    this.#fields = [];
    this.#fieldNameToIndex.clear();
    this.#fieldNames = [];
    this.#fieldTypes = [];
    this.#fieldHasDefault = [];
    this.#fieldDefaultGetters = [];
    this.#writerCache.clear();
    this.#readerCache.clear();

    candidate.forEach((fieldParams) => {
      const field = new RecordField(fieldParams);
      const name = field.getName();
      if (this.#fieldNameToIndex.has(name)) {
        throw new Error(
          `Duplicate record field name: \`${name}\``,
        );
      }
      const type = field.getType();
      const hasDefault = field.hasDefault();
      this.#fieldNameToIndex.set(name, this.#fields.length);
      this.#fieldNames.push(name);
      this.#fieldTypes.push(type);
      this.#fieldHasDefault.push(hasDefault);
      this.#fieldDefaultGetters.push(
        hasDefault ? () => field.getDefault() : undefined,
      );
      this.#fields.push(field);
    });
  }

  #getOrderValue(order: RecordFieldOrder): number {
    switch (order) {
      case "ascending":
        return 1;
      case "descending":
        return -1;
      case "ignore":
        return 0;
    }
  }

  /**
   * Creates a resolver for schema evolution from the writer type to this reader type.
   * @param writerType The writer type to resolve from.
   * @returns A resolver for reading writer data with this schema.
   */
  public override createResolver(writerType: Type): Resolver {
    this.#ensureFields();
    if (!(writerType instanceof RecordType)) {
      return super.createResolver(writerType);
    }

    const acceptableNames = new Set<string>([
      this.getFullName(),
      ...this.getAliases(),
    ]);

    if (!acceptableNames.has(writerType.getFullName())) {
      throw new Error(
        `Schema evolution not supported from writer type: ${writerType.getFullName()} to reader type: ${this.getFullName()}`,
      );
    }

    const readerFields = this.#fields;
    const writerFields = writerType.getFields();

    const readerNameToIndex = new Map<string, number>();
    readerFields.forEach((field, index) => {
      readerNameToIndex.set(field.getName(), index);
      field.getAliases().forEach((alias) => {
        readerNameToIndex.set(alias, index);
      });
    });

    const assignedReaderIndexes = new Set<number>();
    const mappings: FieldMapping[] = [];

    writerFields.forEach((writerField) => {
      let readerIndex = readerNameToIndex.get(writerField.getName());
      if (readerIndex === undefined) {
        for (const alias of writerField.getAliases()) {
          const idx = readerNameToIndex.get(alias);
          if (idx !== undefined) {
            readerIndex = idx;
            break;
          }
        }
      }

      if (readerIndex === undefined) {
        mappings.push({
          readerIndex: -1,
          writerField,
        });
        return;
      }

      if (assignedReaderIndexes.has(readerIndex)) {
        throw new Error(
          `Multiple writer fields map to reader field: ${
            readerFields[readerIndex].getName()
          }`,
        );
      }

      const readerField = readerFields[readerIndex];
      const readerType = readerField.getType();
      const writerFieldType = writerField.getType();
      const resolver = readerType === writerFieldType
        ? undefined
        : readerType.createResolver(writerFieldType);

      mappings.push({
        readerIndex,
        writerField,
        resolver,
      });

      assignedReaderIndexes.add(readerIndex);
    });

    readerFields.forEach((field, index) => {
      if (!assignedReaderIndexes.has(index) && !field.hasDefault()) {
        throw new Error(
          `Field '${field.getName()}' missing from writer schema and has no default.`,
        );
      }
    });

    return new RecordResolver(this, mappings, readerFields);
  }

  #isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }

  #checkField(
    field: RecordField,
    record: Record<string, unknown>,
    errorHook: ErrorHook | undefined,
    path: string[],
  ): boolean {
    const name = field.getName();
    const hasValue = Object.hasOwn(record, name);
    if (!hasValue) {
      if (!field.hasDefault()) {
        if (errorHook) {
          // Allocate path array only when reporting error
          errorHook([...path, name], undefined, this);
        }
        return false;
      }
      return true;
    }

    const fieldValue = record[name];
    // Optimization: Use push/pop instead of spread to avoid allocation during recursion
    if (errorHook) {
      path.push(name);
    }
    const valid = field.getType().check(fieldValue, errorHook, path);
    if (errorHook) {
      path.pop();
    }
    return valid || (errorHook !== undefined);
  }

  #cloneField(
    field: RecordField,
    record: Record<string, unknown>,
    result: Record<string, unknown>,
  ): void {
    const name = field.getName();
    const hasValue = Object.hasOwn(record, name);
    const fieldValue = hasValue ? record[name] : undefined;
    if (!hasValue) {
      if (!field.hasDefault()) {
        throw new Error(
          `Missing value for record field ${name} with no default.`,
        );
      }
      result[name] = field.getDefault();
      return;
    }
    result[name] = field.getType().cloneFromValue(fieldValue);
  }

  #getComparableValue(
    record: Record<string, unknown>,
    field: RecordField,
  ): unknown {
    const name = field.getName();
    const hasValue = Object.hasOwn(record, name);
    const fieldValue = hasValue ? record[name] : undefined;
    if (hasValue) {
      return fieldValue;
    }
    if (field.hasDefault()) {
      return field.getDefault();
    }
    throw new Error(
      `Missing comparable value for field '${name}' with no default.`,
    );
  }
}
