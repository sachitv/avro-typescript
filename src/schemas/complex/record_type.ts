import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import { NamedType } from "./named_type.ts";
import { Resolver } from "../resolver.ts";
import { type JSONType, Type } from "../type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";
import { isValidName, type ResolvedNames } from "./resolve_names.ts";

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

  /**
   * Creates a new RecordType.
   * @param params The record type parameters.
   */
  constructor(params: RecordTypeParams) {
    const { fields, ...names } = params;
    super(names);

    this.#fields = [];
    this.#fieldNameToIndex = new Map();

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
    if (!this.#isRecord(value)) {
      throwInvalidError([], value, this);
    }

    for (const field of this.#fields) {
      await this.#writeField(field, value, tap);
    }
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
   * Converts the given record value to a buffer.
   * @param value The record value to convert.
   * @returns The buffer representation of the value.
   */
  public override async toBuffer(
    value: Record<string, unknown>,
  ): Promise<ArrayBuffer> {
    this.#ensureFields();
    if (!this.#isRecord(value)) {
      throwInvalidError([], value, this);
    }

    const buffers: Uint8Array[] = [];
    for (const field of this.#fields) {
      buffers.push(await this.#getFieldBuffer(field, value));
    }

    const totalSize = buffers.reduce((sum, buf) => sum + buf.byteLength, 0);
    const combined = new Uint8Array(totalSize);
    let offset = 0;
    for (const buf of buffers) {
      combined.set(buf, offset);
      offset += buf.byteLength;
    }

    return combined.buffer;
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

    candidate.forEach((fieldParams) => {
      const field = new RecordField(fieldParams);
      if (this.#fieldNameToIndex.has(field.getName())) {
        throw new Error(
          `Duplicate record field name: ${field.getName()}`,
        );
      }
      this.#fieldNameToIndex.set(field.getName(), this.#fields.length);
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

  #extractFieldValue(
    record: Record<string, unknown>,
    field: RecordField,
  ): { hasValue: boolean; fieldValue: unknown } {
    const name = field.getName();
    const hasValue = Object.hasOwn(record, name);
    const fieldValue = hasValue ? record[name] : undefined;
    return { hasValue, fieldValue };
  }

  #checkField(
    field: RecordField,
    record: Record<string, unknown>,
    errorHook: ErrorHook | undefined,
    path: string[],
  ): boolean {
    const { hasValue, fieldValue } = this.#extractFieldValue(record, field);
    if (!hasValue) {
      if (!field.hasDefault()) {
        if (errorHook) {
          errorHook([...path, field.getName()], undefined, this);
        }
        return false;
      }
      return true;
    }

    const nextPath = errorHook ? [...path, field.getName()] : undefined;
    const valid = field.getType().check(fieldValue, errorHook, nextPath);
    return valid || (errorHook !== undefined);
  }

  async #writeField(
    field: RecordField,
    record: Record<string, unknown>,
    tap: WritableTapLike,
  ): Promise<void> {
    const { hasValue, fieldValue } = this.#extractFieldValue(record, field);
    let toWrite = fieldValue;
    if (!hasValue) {
      if (!field.hasDefault()) {
        throwInvalidError([field.getName()], undefined, this);
      }
      toWrite = field.getDefault();
    }
    await field.getType().write(tap, toWrite as unknown);
  }

  async #getFieldBuffer(
    field: RecordField,
    record: Record<string, unknown>,
  ): Promise<Uint8Array> {
    const { hasValue, fieldValue } = this.#extractFieldValue(record, field);
    let toEncode = fieldValue;
    if (!hasValue) {
      if (!field.hasDefault()) {
        throwInvalidError([field.getName()], undefined, this);
      }
      toEncode = field.getDefault();
    }
    return new Uint8Array(await field.getType().toBuffer(toEncode));
  }

  #cloneField(
    field: RecordField,
    record: Record<string, unknown>,
    result: Record<string, unknown>,
  ): void {
    const { hasValue, fieldValue } = this.#extractFieldValue(record, field);
    if (!hasValue) {
      if (!field.hasDefault()) {
        throw new Error(
          `Missing value for record field ${field.getName()} with no default.`,
        );
      }
      result[field.getName()] = field.getDefault();
      return;
    }
    result[field.getName()] = field.getType().cloneFromValue(fieldValue);
  }

  #getComparableValue(
    record: Record<string, unknown>,
    field: RecordField,
  ): unknown {
    const { hasValue, fieldValue } = this.#extractFieldValue(record, field);
    if (hasValue) {
      return fieldValue;
    }
    if (field.hasDefault()) {
      return field.getDefault();
    }
    throw new Error(
      `Missing comparable value for field '${field.getName()}' with no default.`,
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
}
