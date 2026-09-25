/**
 * Writes record field defaults back to their Avro JSON form.
 *
 * A field default is parsed from JSON into the field type's runtime value
 * (see `RecordField`), so a `long` default is held as a `bigint`, a `map`
 * default as a `Map`, and `bytes`/`fixed` defaults as a `Uint8Array`. None of
 * these can be passed to `JSON.stringify` as they are. {@link defaultToJSON}
 * is the inverse of that parse: it walks the value alongside its type and
 * returns the JSON encoding the Avro specification defines for defaults.
 *
 * @module
 */

import { ArrayType } from "./complex/array_type.ts";
import { FixedType } from "./complex/fixed_type.ts";
import { MapType } from "./complex/map_type.ts";
import { RecordType } from "./complex/record_type.ts";
import { getBranchTypeName, UnionType } from "./complex/union_type.ts";
import { LogicalType } from "./logical/logical_type.ts";
import { BytesType } from "./primitive/bytes_type.ts";
import { DoubleType } from "./primitive/double_type.ts";
import { FloatType } from "./primitive/float_type.ts";
import { LongType } from "./primitive/long_type.ts";
import type { JSONType, Type } from "./type.ts";

const MAX_SAFE_LONG = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * Returns the Avro JSON encoding of a field default held as a runtime value.
 *
 * `long` values become JSON numbers, `float`/`double` values numbers,
 * `bytes`/`fixed` values strings with one code point (0-255) per byte, maps
 * and records objects, arrays arrays, and logical values the encoding of
 * their underlying value. Union values keep the branch-wrapped form that the
 * field parser reads. Values of other types (null, boolean, int, string,
 * enum) are already JSON and are returned as they are.
 *
 * Throws rather than write a default that would read back differently: a
 * `long` outside the safe integer range, which a JSON number cannot hold
 * exactly, and a non-finite `float` or `double`, which JSON cannot represent.
 *
 * @param type The type of the field.
 * @param value The field's default, as returned by `RecordField.getDefault()`.
 * @param fieldName The field's name, used in error messages.
 * @returns The default's JSON encoding.
 * @internal
 */
export function defaultToJSON(
  type: Type,
  value: unknown,
  fieldName: string,
): JSONType {
  if (type instanceof LogicalType) {
    return defaultToJSON(
      type.getUnderlyingType(),
      type.convertToUnderlying(value),
      fieldName,
    );
  }
  if (type instanceof LongType) {
    const long = value as bigint;
    if (long > MAX_SAFE_LONG || long < -MAX_SAFE_LONG) {
      throw new Error(
        `Cannot write the default of field '${fieldName}' to schema JSON: ` +
          `long ${long} is outside the safe integer range ` +
          `(±${Number.MAX_SAFE_INTEGER}), so a JSON number cannot hold it ` +
          `exactly.`,
      );
    }
    return Number(long);
  }
  if (type instanceof FloatType || type instanceof DoubleType) {
    const number = value as number;
    if (!Number.isFinite(number)) {
      throw new Error(
        `Cannot write the default of field '${fieldName}' to schema JSON: ` +
          `${type.toJSON()} ${number} is not finite, and JSON has no ` +
          `representation for it.`,
      );
    }
    return number;
  }
  if (type instanceof BytesType || type instanceof FixedType) {
    return bytesToJSON(value as Uint8Array);
  }
  if (type instanceof ArrayType) {
    const itemsType = type.getItemsType();
    return (value as unknown[]).map((item) =>
      defaultToJSON(itemsType, item, fieldName)
    );
  }
  if (type instanceof MapType) {
    const valuesType = type.getValuesType();
    // fromEntries defines own properties, so a "__proto__" key stays a key.
    return Object.fromEntries(
      Array.from(
        (value as Map<string, unknown>).entries(),
        ([key, entry]) => [key, defaultToJSON(valuesType, entry, fieldName)],
      ),
    );
  }
  if (type instanceof RecordType) {
    const record = value as Record<string, unknown>;
    return Object.fromEntries(
      type.getFields().map((field) => [
        field.getName(),
        defaultToJSON(field.getType(), record[field.getName()], fieldName),
      ]),
    );
  }
  if (type instanceof UnionType) {
    if (value === null) {
      return null;
    }
    const [[branchName, branchValue]] = Object.entries(
      value as Record<string, unknown>,
    );
    const branchType = type.getTypes().find((branch) =>
      getBranchTypeName(branch) === branchName
    )!;
    return Object.fromEntries([
      [branchName, defaultToJSON(branchType, branchValue, fieldName)],
    ]);
  }
  return value as JSONType;
}

/**
 * Reads a field default given in schema JSON into a value the field type's
 * `cloneFromValue` accepts: the inverse of {@link defaultToJSON}.
 *
 * Primitive and complex types already accept their JSON form (a `long` from a
 * number, `bytes` from a string), so this only rewrites defaults of logical
 * types, wherever they are nested. The Avro specification gives a logical
 * type's default in its underlying type's JSON encoding (`timestamp-millis`
 * as a number, a decimal as a bytes string), which is what
 * {@link defaultToJSON} writes; schemas built in code may instead give the
 * logical runtime value (a `Date`, a `bigint`). A value the logical type
 * accepts as it is is kept; anything else is read as the underlying type's
 * JSON and converted.
 *
 * @param type The type of the field.
 * @param value The default as given in the schema.
 * @returns The default with every logical value in its runtime form.
 * @internal
 */
export function defaultFromJSON(type: Type, value: unknown): unknown {
  if (type instanceof LogicalType) {
    if (type.isValid(value)) {
      return value;
    }
    const underlying = type.getUnderlyingType().cloneFromValue(value);
    return type.convertFromUnderlying(underlying);
  }
  if (type instanceof ArrayType && Array.isArray(value)) {
    const itemsType = type.getItemsType();
    return value.map((item) => defaultFromJSON(itemsType, item));
  }
  if (type instanceof MapType && isPlainObject(value)) {
    const valuesType = type.getValuesType();
    // fromEntries defines own properties, so a "__proto__" key stays a key.
    return Object.fromEntries(
      Object.entries(value).map((
        [key, entry],
      ) => [key, defaultFromJSON(valuesType, entry)]),
    );
  }
  if (type instanceof MapType && value instanceof Map) {
    const valuesType = type.getValuesType();
    return new Map(
      Array.from(
        value.entries(),
        ([key, entry]) => [key, defaultFromJSON(valuesType, entry)],
      ),
    );
  }
  if (type instanceof RecordType && isPlainObject(value)) {
    const record: Record<string, unknown> = { ...value };
    for (const field of type.getFields()) {
      const name = field.getName();
      if (Object.prototype.hasOwnProperty.call(record, name)) {
        record[name] = defaultFromJSON(field.getType(), record[name]);
      }
    }
    return record;
  }
  if (type instanceof UnionType && isPlainObject(value)) {
    const entries = Object.entries(value);
    if (entries.length === 1) {
      const [[branchName, branchValue]] = entries;
      const branchType = type.getTypes().find((branch) =>
        getBranchTypeName(branch) === branchName
      );
      if (branchType !== undefined) {
        return Object.fromEntries([
          [branchName, defaultFromJSON(branchType, branchValue)],
        ]);
      }
    }
  }
  return value;
}

/** Whether `value` is an object literal (not null, an array, a Map, etc.). */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** Encodes bytes as a string with one code point (0-255) per byte. */
function bytesToJSON(bytes: Uint8Array): string {
  let result = "";
  for (const byte of bytes) {
    result += String.fromCharCode(byte);
  }
  return result;
}
