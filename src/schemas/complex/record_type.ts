import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import { WritableTap } from "../../serialization/tap.ts";
import { CountingWritableTap } from "../../serialization/counting_writable_tap.ts";
import { SyncCountingWritableTap } from "../../serialization/sync_counting_writable_tap.ts";
import { NamedType } from "./named_type.ts";
import { Resolver } from "../resolver.ts";
import { type JSONType, Type } from "../type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";
import { isValidName, type ResolvedNames } from "./resolve_names.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/sync_tap.ts";
import { SyncWritableTap } from "../../serialization/sync_tap.ts";
import { BooleanType } from "../primitive/boolean_type.ts";
import { BytesType } from "../primitive/bytes_type.ts";
import { DoubleType } from "../primitive/double_type.ts";
import { FloatType } from "../primitive/float_type.ts";
import { IntType } from "../primitive/int_type.ts";
import { LongType } from "../primitive/long_type.ts";
import { NullType } from "../primitive/null_type.ts";
import { StringType } from "../primitive/string_type.ts";

type CompiledSyncWriter = (tap: SyncWritableTapLike, value: unknown) => void;
type CompiledWriter = (tap: WritableTapLike, value: unknown) => Promise<void>;

/**
 * Specifies the sort order for record fields.
 */
export type RecordFieldOrder = "ascending" | "descending" | "ignore";

/**
 * Parameters for defining a record field.
 */
export interface RecordFieldParams {
  /** The name of the field. */
  name: string;
  /** The type of the field. */
  type: Type;
  /** Optional aliases for the field. */
  aliases?: string[];
  /** Optional ordering for the field. */
  order?: RecordFieldOrder;
  /** Optional default value for the field. */
  default?: unknown;
}

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
}

/**
 * Represents a field in an Avro record type.
 */
export class RecordField {
  #name: string;
  #type: Type;
  #aliases: string[];
  #order: RecordFieldOrder;
  #hasDefault: boolean;
  #defaultValue?: unknown;

  /**
   * Constructs a new RecordField instance.
   */
  constructor(params: RecordFieldParams) {
    const { name, type, aliases = [], order = "ascending" } = params;

    if (typeof name !== "string" || !isValidName(name)) {
      throw new Error(`Invalid record field name: ${name}`);
    }
    if (!(type instanceof Type)) {
      throw new Error(`Invalid field type for ${name}`);
    }

    if (!RecordField.#isValidOrder(order)) {
      throw new Error(`Invalid record field order: ${order}`);
    }

    this.#name = name;
    this.#type = type;
    this.#order = order;

    const aliasSet = new Set<string>();
    const resolvedAliases: string[] = [];
    for (const alias of aliases) {
      if (typeof alias !== "string" || !isValidName(alias)) {
        throw new Error(`Invalid record field alias: ${alias}`);
      }
      if (!aliasSet.has(alias)) {
        aliasSet.add(alias);
        resolvedAliases.push(alias);
      }
    }
    this.#aliases = resolvedAliases;

    this.#hasDefault = Object.prototype.hasOwnProperty.call(params, "default");
    if (this.#hasDefault) {
      this.#defaultValue = this.#type.cloneFromValue(params.default as unknown);
    }
  }

  /**
   * Gets the name of the field.
   */
  public getName(): string {
    return this.#name;
  }

  /**
   * Gets the type of the field.
   */
  public getType(): Type {
    return this.#type;
  }

  /**
   * Gets the aliases of the field.
   */
  public getAliases(): string[] {
    return this.#aliases.slice();
  }

  /**
   * Gets the order of the field.
   */
  public getOrder(): RecordFieldOrder {
    return this.#order;
  }

  /**
   * Returns true if the field has a default value.
   */
  public hasDefault(): boolean {
    return this.#hasDefault;
  }

  /**
   * Gets the default value for the field.
   * @returns The default value.
   * @throws Error if the field has no default.
   */
  public getDefault(): unknown {
    if (!this.#hasDefault) {
      throw new Error(`Field '${this.#name}' has no default.`);
    }
    return this.#type.cloneFromValue(this.#defaultValue as unknown);
  }

  /**
   * Checks if the given name matches the field name or any of its aliases.
   * @param name The name to check.
   * @returns True if the name matches, false otherwise.
   */
  public nameMatches(name: string): boolean {
    return name === this.#name || this.#aliases.includes(name);
  }

  static #isValidOrder(order: string): order is RecordFieldOrder {
    return order === "ascending" || order === "descending" ||
      order === "ignore";
  }
}

interface FieldMapping {
  readerIndex: number;
  writerField: RecordField;
  resolver?: Resolver;
}

/**
 * Avro `record` type supporting ordered fields, aliases, and schema evolution.
 */
export class RecordType extends NamedType<Record<string, unknown>> {
  #fields: RecordField[];
  #fieldNameToIndex: Map<string, number>;
  #fieldsThunk?: () => RecordFieldParams[];
  #validateWrites: boolean;
  #compiledSyncWriterStrict?: CompiledSyncWriter;
  #compiledSyncWriterUnchecked?: CompiledSyncWriter;
  #compiledWriterStrict?: CompiledWriter;
  #compiledWriterUnchecked?: CompiledWriter;
  #fieldNames: string[];
  #fieldTypes: Type[];
  #fieldHasDefault: boolean[];
  #fieldDefaultGetters: Array<(() => unknown) | undefined>;

  /**
   * Creates a new RecordType.
   * @param params The record type parameters.
   */
  constructor(params: RecordTypeParams) {
    const { fields, validate, ...names } = params;
    super(names);

    this.#fields = [];
    this.#fieldNameToIndex = new Map();
    this.#fieldNames = [];
    this.#fieldTypes = [];
    this.#fieldHasDefault = [];
    this.#fieldDefaultGetters = [];
    this.#validateWrites = validate ?? true;

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
   * Writes the given record value to the tap.
   * @param tap The writable tap to write to.
   * @param value The record value to write.
   */
  public override async write(
    tap: WritableTapLike,
    value: Record<string, unknown>,
  ): Promise<void> {
    this.#ensureFields();
    const writer = this.#validateWrites
      ? (this.#compiledWriterStrict ??= this.#getOrCreateCompiledWriter(true))
      : (this.#compiledWriterUnchecked ??= this.#getOrCreateCompiledWriter(
        false,
      ));
    await writer(tap, value);
  }

  /**
   * Writes the record without runtime validation.
   */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: Record<string, unknown>,
  ): Promise<void> {
    this.#ensureFields();
    const writer = this.#compiledWriterUnchecked ??= this
      .#getOrCreateCompiledWriter(
        false,
      );
    await writer(tap, value);
  }

  /**
   * Serializes every field synchronously onto the tap, honoring defaults.
   */
  public override writeSync(
    tap: SyncWritableTapLike,
    value: Record<string, unknown>,
  ): void {
    this.#ensureFields();
    const writer = this.#validateWrites
      ? (this.#compiledSyncWriterStrict ??= this.#getOrCreateCompiledSyncWriter(
        true,
      ))
      : (this.#compiledSyncWriterUnchecked ??= this
        .#getOrCreateCompiledSyncWriter(false));
    writer(tap, value);
  }

  /**
   * Writes the record synchronously without runtime validation.
   */
  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: Record<string, unknown>,
  ): void {
    this.#ensureFields();
    const writer = this.#compiledSyncWriterUnchecked ??= this
      .#getOrCreateCompiledSyncWriter(false);
    writer(tap, value);
  }

  #getOrCreateCompiledWriter(validate: boolean): CompiledWriter {
    this.#ensureFields();
    const cached = validate
      ? this.#compiledWriterStrict
      : this.#compiledWriterUnchecked;
    if (cached) {
      return cached;
    }

    // Support recursive record types by installing a placeholder before walking
    // field types; recursive references will see this function and avoid
    // infinite recursion during compilation. The impl array allows the
    // placeholder to delegate to the actual writer once it's created.
    const impl: [CompiledWriter | null] = [null];
    const placeholder: CompiledWriter = (tap, value) => impl[0]!(tap, value);
    if (validate) {
      this.#compiledWriterStrict = placeholder;
    } else {
      this.#compiledWriterUnchecked = placeholder;
    }

    const fieldCount = this.#fieldNames.length;
    const writers = new Array<CompiledWriter>(fieldCount);
    for (let i = 0; i < fieldCount; i++) {
      const fieldType = this.#fieldTypes[i]!;
      if (!validate) {
        writers[i] = RecordType.#compileUncheckedWriter(fieldType);
        continue;
      }
      if (fieldType instanceof RecordType) {
        writers[i] = fieldType.#getOrCreateCompiledWriter(true);
      } else {
        writers[i] = (tap, value) => fieldType.write(tap, value as never);
      }
    }

    const actualWriter: CompiledWriter = async (tap, value) => {
      if (validate && !this.#isRecord(value)) {
        throwInvalidError([], value, this);
      }
      const record = value as Record<string, unknown>;
      for (let i = 0; i < fieldCount; i++) {
        const name = this.#fieldNames[i]!;
        const hasValue = Object.hasOwn(record, name);
        let toWrite: unknown;
        if (hasValue) {
          toWrite = record[name];
        } else {
          const getter = this.#fieldDefaultGetters[i];
          if (getter) {
            toWrite = getter();
          } else {
            if (validate) {
              throwInvalidError([name], undefined, this);
            }
            toWrite = undefined;
          }
        }
        await writers[i]!(tap, toWrite);
      }
    };

    if (validate) {
      this.#compiledWriterStrict = actualWriter;
    } else {
      this.#compiledWriterUnchecked = actualWriter;
    }
    impl[0] = actualWriter;
    return placeholder;
  }

  #getOrCreateCompiledSyncWriter(validate: boolean): CompiledSyncWriter {
    this.#ensureFields();
    const cached = validate
      ? this.#compiledSyncWriterStrict
      : this.#compiledSyncWriterUnchecked;
    if (cached) {
      return cached;
    }

    // Support recursive record types by installing a placeholder before walking
    // field types; recursive references will see this function and avoid
    // infinite recursion during compilation. The impl array allows the
    // placeholder to delegate to the actual writer once it's created.
    const impl: [CompiledSyncWriter | null] = [null];
    const placeholder: CompiledSyncWriter = (tap, value) =>
      impl[0]!(tap, value);
    if (validate) {
      this.#compiledSyncWriterStrict = placeholder;
    } else {
      this.#compiledSyncWriterUnchecked = placeholder;
    }

    const fieldCount = this.#fieldNames.length;
    const writers = new Array<CompiledSyncWriter>(fieldCount);
    for (let i = 0; i < fieldCount; i++) {
      const fieldType = this.#fieldTypes[i]!;
      if (!validate) {
        writers[i] = RecordType.#compileUncheckedSyncWriter(fieldType);
        continue;
      }
      if (fieldType instanceof RecordType) {
        writers[i] = fieldType.#getOrCreateCompiledSyncWriter(true);
      } else {
        writers[i] = (tap, value) => fieldType.writeSync(tap, value as never);
      }
    }

    const actualWriter: CompiledSyncWriter = (tap, value) => {
      if (validate && !this.#isRecord(value)) {
        throwInvalidError([], value, this);
      }
      const record = value as Record<string, unknown>;
      for (let i = 0; i < fieldCount; i++) {
        const name = this.#fieldNames[i]!;
        const hasValue = Object.hasOwn(record, name);
        let toWrite: unknown;
        if (hasValue) {
          toWrite = record[name];
        } else {
          const getter = this.#fieldDefaultGetters[i];
          if (getter) {
            toWrite = getter();
          } else {
            if (validate) {
              throwInvalidError([name], undefined, this);
            }
            toWrite = undefined;
          }
        }
        writers[i]!(tap, toWrite);
      }
    };

    impl[0] = actualWriter;
    if (validate) {
      this.#compiledSyncWriterStrict = actualWriter;
    } else {
      this.#compiledSyncWriterUnchecked = actualWriter;
    }
    return placeholder;
  }

  static #compileUncheckedWriter(type: Type): CompiledWriter {
    if (type instanceof RecordType) {
      return type.#getOrCreateCompiledWriter(false);
    }
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
    if (type instanceof RecordType) {
      return type.#getOrCreateCompiledSyncWriter(false);
    }
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

  /**
   * Reads a record value from the tap.
   * @param tap The readable tap to read from.
   * @returns The read record value.
   */
  public override async read(
    tap: ReadableTapLike,
  ): Promise<Record<string, unknown>> {
    this.#ensureFields();
    const result: Record<string, unknown> = {};
    for (const field of this.#fields) {
      result[field.getName()] = await field.getType().read(tap);
    }
    return result;
  }

  /**
   * Reads every field synchronously using each field type's sync reader.
   */
  public override readSync(tap: SyncReadableTapLike): Record<string, unknown> {
    this.#ensureFields();
    const result: Record<string, unknown> = {};
    for (const field of this.#fields) {
      result[field.getName()] = field.getType().readSync(tap);
    }
    return result;
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
   * Converts the given record value to a buffer.
   * @param value The record value to convert.
   * @returns The buffer representation of the value.
   */
  public override async toBuffer(
    value: Record<string, unknown>,
  ): Promise<ArrayBuffer> {
    this.#ensureFields();
    if (this.#validateWrites && !this.#isRecord(value)) {
      throwInvalidError([], value, this);
    }

    const writer = this.#validateWrites
      ? (this.#compiledWriterStrict ??= this.#getOrCreateCompiledWriter(true))
      : (this.#compiledWriterUnchecked ??= this.#getOrCreateCompiledWriter(
        false,
      ));

    // Pass 1: measure encoded size without allocating per-field buffers.
    const sizeTap = new CountingWritableTap();
    await writer(sizeTap as unknown as WritableTapLike, value);
    const size = sizeTap.getPos();

    // Pass 2: allocate exactly once and serialize into it.
    const buffer = new ArrayBuffer(size);
    const tap = new WritableTap(buffer);
    await writer(tap, value);
    return buffer;
  }

  /**
   * Encodes every field synchronously into a single buffer.
   */
  public override toSyncBuffer(value: Record<string, unknown>): ArrayBuffer {
    this.#ensureFields();
    if (this.#validateWrites && !this.#isRecord(value)) {
      throwInvalidError([], value, this);
    }

    const writer = this.#validateWrites
      ? (this.#compiledSyncWriterStrict ??= this.#getOrCreateCompiledSyncWriter(
        true,
      ))
      : (this.#compiledSyncWriterUnchecked ??= this
        .#getOrCreateCompiledSyncWriter(false));

    // Pass 1: measure encoded size without allocating per-field buffers.
    const sizeTap = new SyncCountingWritableTap();
    writer(sizeTap as unknown as SyncWritableTapLike, value);
    const size = sizeTap.getPos();

    // Pass 2: allocate exactly once and serialize into it.
    const buffer = new ArrayBuffer(size);
    const tap = new SyncWritableTap(buffer);
    writer(tap, value);
    return buffer;
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
    this.#compiledSyncWriterStrict = undefined;
    this.#compiledSyncWriterUnchecked = undefined;
    this.#compiledWriterStrict = undefined;
    this.#compiledWriterUnchecked = undefined;

    candidate.forEach((fieldParams) => {
      const field = new RecordField(fieldParams);
      const name = field.getName();
      if (this.#fieldNameToIndex.has(name)) {
        throw new Error(
          `Duplicate record field name: ${name}`,
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
      const writerType = writerField.getType();
      const resolver = readerType === writerType
        ? undefined
        : readerType.createResolver(writerType);

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
          errorHook([...path, name], undefined, this);
        }
        return false;
      }
      return true;
    }

    const fieldValue = record[name];
    const nextPath = errorHook ? [...path, name] : undefined;
    const valid = field.getType().check(fieldValue, errorHook, nextPath);
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

class RecordResolver extends Resolver<Record<string, unknown>> {
  #mappings: FieldMapping[];
  #readerFields: RecordField[];

  constructor(
    reader: RecordType,
    mappings: FieldMapping[],
    readerFields: RecordField[],
  ) {
    super(reader);
    this.#mappings = mappings;
    this.#readerFields = readerFields;
  }

  public override async read(
    tap: ReadableTapLike,
  ): Promise<Record<string, unknown>> {
    const result: Record<string, unknown> = {};
    const seen = new Array(this.#readerFields.length).fill(false);

    for (const mapping of this.#mappings) {
      if (mapping.readerIndex === -1) {
        await mapping.writerField.getType().skip(tap);
        continue;
      }

      const value = mapping.resolver
        ? await mapping.resolver.read(tap)
        : await mapping.writerField.getType().read(tap);

      const readerField = this.#readerFields[mapping.readerIndex];
      result[readerField.getName()] = value;
      seen[mapping.readerIndex] = true;
    }

    for (let i = 0; i < this.#readerFields.length; i++) {
      if (!seen[i]) {
        const field = this.#readerFields[i];
        result[field.getName()] = field.getDefault();
      }
    }

    return result;
  }

  /**
   * Resolves incoming data synchronously, applying missing defaults as needed.
   */
  public override readSync(
    tap: SyncReadableTapLike,
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    const seen = new Array(this.#readerFields.length).fill(false);

    for (const mapping of this.#mappings) {
      if (mapping.readerIndex === -1) {
        mapping.writerField.getType().skipSync(tap);
        continue;
      }

      const value = mapping.resolver
        ? mapping.resolver.readSync(tap)
        : mapping.writerField.getType().readSync(tap);

      const readerField = this.#readerFields[mapping.readerIndex];
      result[readerField.getName()] = value;
      seen[mapping.readerIndex] = true;
    }

    for (let i = 0; i < this.#readerFields.length; i++) {
      if (!seen[i]) {
        const field = this.#readerFields[i];
        result[field.getName()] = field.getDefault();
      }
    }

    return result;
  }
}
