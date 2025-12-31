import { ArrayType } from "../schemas/complex/array_type.ts";
import { BooleanType } from "../schemas/primitive/boolean_type.ts";
import { BytesType } from "../schemas/primitive/bytes_type.ts";
import { DoubleType } from "../schemas/primitive/double_type.ts";
import { EnumType, type EnumTypeParams } from "../schemas/complex/enum_type.ts";
import {
  FixedType,
  type FixedTypeParams,
} from "../schemas/complex/fixed_type.ts";
import { FloatType } from "../schemas/primitive/float_type.ts";
import { IntType } from "../schemas/primitive/int_type.ts";
import { LongType } from "../schemas/primitive/long_type.ts";
import { MapType } from "../schemas/complex/map_type.ts";
import {
  type RecordFieldParams,
  RecordType,
  type RecordTypeParams,
  type RecordWriterStrategy,
} from "../schemas/complex/record_type.ts";
import { resolveNames } from "../schemas/complex/resolve_names.ts";
import { StringType } from "../schemas/primitive/string_type.ts";
import { UnionType } from "../schemas/complex/union_type.ts";
import { NullType } from "../schemas/primitive/null_type.ts";
import { Type } from "../schemas/type.ts";
import { safeStringify } from "../schemas/json.ts";
import { DecimalLogicalType } from "../schemas/logical/decimal_logical_type.ts";
import { UuidLogicalType } from "../schemas/logical/uuid_logical_type.ts";
import {
  DateLogicalType,
  LocalTimestampMicrosLogicalType,
  LocalTimestampMillisLogicalType,
  LocalTimestampNanosLogicalType,
  TimeMicrosLogicalType,
  TimeMillisLogicalType,
  TimestampMicrosLogicalType,
  TimestampMillisLogicalType,
  TimestampNanosLogicalType,
} from "../schemas/logical/temporal_logical_types.ts";
import { DurationLogicalType } from "../schemas/logical/duration_logical_type.ts";
import { NamedType } from "../schemas/complex/named_type.ts";

type PrimitiveTypeName =
  | "null"
  | "boolean"
  | "int"
  | "long"
  | "float"
  | "double"
  | "bytes"
  | "string";

/**
 * Represents an Avro schema definition.
 * Can be a pre-constructed Type, a type name (string), a schema object,
 * or an array representing a union.
 */
export type SchemaLike =
  | Type
  | string
  | SchemaObject
  | SchemaLike[];

/**
 * Represents an Avro schema object, which defines the structure of data in Avro format.
 * It can represent primitive types, complex types like records and arrays, or named types.
 */
export interface SchemaObject {
  /** The type of the Avro schema, such as "record", "array", "enum", "fixed", or a primitive type like "string". */
  type: unknown;
  /** The name of the schema for named types like records, enums, and fixed types. */
  name?: unknown;
  /** The namespace for the schema, used to qualify named types and avoid naming conflicts. */
  namespace?: unknown;
  /** Alternative names (aliases) for the schema, allowing backward compatibility or multiple references. */
  aliases?: unknown;
  /** The fields of a record schema, an array of field definitions each specifying name, type, and optional default. */
  fields?: unknown;
  /** The symbols for an enum schema, an array of string values representing the possible enumeration values. */
  symbols?: unknown;
  /** The size in bytes for a fixed schema, specifying the exact length of the fixed-size data. */
  size?: unknown;
  /** The type of items in an array schema, defining the schema for each element in the array. */
  items?: unknown;
  /** The type of values in a map schema, defining the schema for the values associated with string keys. */
  values?: unknown;
  /** The default value for a field in a record or for an enum, used when the field is missing in the data. */
  default?: unknown;
  /** The logical type annotation, such as "date", "decimal", or "uuid", providing semantic meaning to the underlying type. */
  logicalType?: unknown;
  /** Allows additional arbitrary properties in the schema object for extensibility. */
  [key: string]: unknown;
}

interface CreateTypeContext {
  namespace?: string;
  registry: Map<string, Type>;
  validate: boolean;
  writerStrategy?: RecordWriterStrategy;
}

/**
 * Options for creating an Avro type from a schema.
 */
export interface CreateTypeOptions {
  /**
   * Default namespace to use for named types.
   */
  namespace?: string;
  /**
   * Registry of shared named types.
   */
  registry?: Map<string, Type>;
  /**
   * Whether to validate values during serialization writes.
   *
   * When set to `false`, types may skip runtime type/range checks on hot write
   * paths for performance. This is unsafe for untrusted input and should only
   * be used when values are already known to match the schema.
   */
  validate?: boolean;
  /**
   * Optional writer strategy for record types.
   *
   * Controls how record field writers are compiled:
   * - `CompiledWriterStrategy` (default): Inlines primitive tap methods for performance.
   * - `InterpretedWriterStrategy`: Delegates to type.write() for simplicity.
   *
   * Custom strategies can be provided for instrumentation, debugging, or
   * alternative compilation approaches.
   */
  writerStrategy?: RecordWriterStrategy;
}

function createPrimitiveType(name: PrimitiveTypeName, validate: boolean): Type {
  switch (name) {
    case "null":
      return new NullType(validate);
    case "boolean":
      return new BooleanType(validate);
    case "int":
      return new IntType(validate);
    case "long":
      return new LongType(validate);
    case "float":
      return new FloatType(validate);
    case "double":
      return new DoubleType(validate);
    case "bytes":
      return new BytesType(validate);
    case "string":
      return new StringType(validate);
  }
}

/**
 * Constructs an Avro {@link Type} from a schema definition.
 */
export function createType(
  schema: SchemaLike,
  options: CreateTypeOptions = {},
): Type {
  const registry = options.registry ?? new Map<string, Type>();
  const context: CreateTypeContext = {
    namespace: options.namespace,
    registry,
    validate: options.validate ?? true,
    writerStrategy: options.writerStrategy,
  };
  return constructType(schema, context);
}

function constructType(schema: SchemaLike, context: CreateTypeContext): Type {
  if (schema instanceof Type) {
    return schema;
  }

  if (typeof schema === "string") {
    return createFromTypeName(schema, context);
  }

  if (Array.isArray(schema)) {
    return createUnionType(schema, context);
  }

  if (schema === null || typeof schema !== "object") {
    throw new Error(
      `Unsupported Avro schema: ${safeStringify(schema)}`,
    );
  }

  const logicalType = schema.logicalType;
  if (logicalType !== undefined) {
    return createLogicalType(schema, logicalType, context);
  }

  const { type } = schema;

  if (typeof type === "string") {
    if (isRecordTypeName(type)) {
      return createRecordType(schema, type, context);
    }
    if (type === "enum") {
      return createEnumType(schema, context);
    }
    if (type === "fixed") {
      return createFixedType(schema, context);
    }
    if (type === "array") {
      return createArrayType(schema, context);
    }
    if (type === "map") {
      return createMapType(schema, context);
    }
    if (isPrimitiveTypeName(type)) {
      return createPrimitiveType(type, context.validate);
    }
    return createFromTypeName(type, {
      namespace: extractNamespace(schema, context.namespace),
      registry: context.registry,
      validate: context.validate,
    });
  }

  if (Array.isArray(type)) {
    // When we're dealing with an array as a schema, it is a Union.
    // > https://avro.apache.org/docs/1.12.0/specification/#unions
    // > `Unions, as mentioned above, are represented using JSON arrays`.
    return createUnionType(type as SchemaLike[], context);
  }

  if (type && typeof type === "object") {
    return constructType(type as SchemaLike, context);
  }

  throw new Error(
    `Schema is missing a valid "type" property: ${safeStringify(schema)}`,
  );
}

function createLogicalType(
  schema: SchemaObject,
  logicalType: unknown,
  context: CreateTypeContext,
): Type {
  const buildUnderlying = (): Type => {
    const underlyingSchema = { ...schema };
    delete underlyingSchema.logicalType;
    return constructType(underlyingSchema as SchemaLike, context);
  };

  if (typeof logicalType !== "string") {
    return buildUnderlying();
  }

  const underlying = buildUnderlying();

  const replaceIfNamed = (logical: Type): Type => {
    if (underlying instanceof NamedType) {
      context.registry.set(underlying.getFullName(), logical);
    }
    return logical;
  };

  try {
    switch (logicalType) {
      case "decimal": {
        if (
          underlying instanceof BytesType || underlying instanceof FixedType
        ) {
          const precision = schema.precision;
          const scaleValue = schema.scale;
          if (typeof precision !== "number") {
            return underlying;
          }
          if (scaleValue !== undefined && typeof scaleValue !== "number") {
            return underlying;
          }
          const logical = new DecimalLogicalType(underlying, {
            precision,
            scale: scaleValue as number | undefined,
          }, context.validate);
          return replaceIfNamed(logical);
        }
        return underlying;
      }
      case "uuid": {
        if (
          underlying instanceof StringType || underlying instanceof FixedType
        ) {
          const logical = new UuidLogicalType(underlying, context.validate);
          return replaceIfNamed(logical);
        }
        return underlying;
      }
      case "date": {
        if (underlying instanceof IntType) {
          return new DateLogicalType(underlying, context.validate);
        }
        return underlying;
      }
      case "time-millis": {
        if (underlying instanceof IntType) {
          return new TimeMillisLogicalType(underlying, context.validate);
        }
        return underlying;
      }
      case "time-micros": {
        if (underlying instanceof LongType) {
          return new TimeMicrosLogicalType(underlying, context.validate);
        }
        return underlying;
      }
      case "timestamp-millis": {
        if (underlying instanceof LongType) {
          return new TimestampMillisLogicalType(underlying, context.validate);
        }
        return underlying;
      }
      case "timestamp-micros": {
        if (underlying instanceof LongType) {
          return new TimestampMicrosLogicalType(underlying, context.validate);
        }
        return underlying;
      }
      case "timestamp-nanos": {
        if (underlying instanceof LongType) {
          return new TimestampNanosLogicalType(underlying, context.validate);
        }
        return underlying;
      }
      case "local-timestamp-millis": {
        if (underlying instanceof LongType) {
          return new LocalTimestampMillisLogicalType(
            underlying,
            context.validate,
          );
        }
        return underlying;
      }
      case "local-timestamp-micros": {
        if (underlying instanceof LongType) {
          return new LocalTimestampMicrosLogicalType(
            underlying,
            context.validate,
          );
        }
        return underlying;
      }
      case "local-timestamp-nanos": {
        if (underlying instanceof LongType) {
          return new LocalTimestampNanosLogicalType(
            underlying,
            context.validate,
          );
        }
        return underlying;
      }
      case "duration": {
        if (underlying instanceof FixedType) {
          const logical = new DurationLogicalType(underlying, context.validate);
          return replaceIfNamed(logical);
        }
        return underlying;
      }
      default:
        return underlying;
    }
  } catch {
    return underlying;
  }
}

function createFromTypeName(
  name: string,
  context: CreateTypeContext,
): Type {
  if (isPrimitiveTypeName(name as PrimitiveTypeName)) {
    return createPrimitiveType(name as PrimitiveTypeName, context.validate);
  }

  const fullName = qualifyReference(name, context.namespace);
  const found = context.registry.get(fullName);
  if (found) {
    return found;
  }

  materializeLazyRecordFields(context.registry);

  const materialized = context.registry.get(fullName);
  if (materialized) {
    return materialized;
  }

  throw new Error(`Undefined Avro type reference: ${name}`);
}

function createRecordType(
  schema: SchemaObject,
  typeName: "record" | "error" | "request",
  context: CreateTypeContext,
): RecordType {
  const name = schema.name;
  if (typeof name !== "string" || name.length === 0) {
    throw new Error(
      `Record schema requires a non-empty name: ${safeStringify(schema)}`,
    );
  }

  const aliases = toStringArray(schema.aliases);
  const resolved = resolveNames({
    name,
    namespace: extractNamespace(schema, context.namespace),
    aliases,
  });

  const childContext: CreateTypeContext = {
    namespace: resolved.namespace || undefined,
    registry: context.registry,
    validate: context.validate,
    writerStrategy: context.writerStrategy,
  };

  const fieldsValue = schema.fields;
  if (!Array.isArray(fieldsValue)) {
    throw new Error(
      `Record schema requires a fields array: ${safeStringify(schema)}`,
    );
  }

  const buildFields = (): RecordFieldParams[] => {
    return fieldsValue.map((field) => {
      if (field === null || typeof field !== "object") {
        throw new Error(
          `Invalid record field definition: ${safeStringify(field)}`,
        );
      }
      const fieldName = (field as SchemaObject).name;
      if (typeof fieldName !== "string" || fieldName.length === 0) {
        throw new Error(
          `Record field requires a non-empty name: ${safeStringify(field)}`,
        );
      }
      if (!("type" in (field as SchemaObject))) {
        throw new Error(
          `Record field "${fieldName}" is missing a type definition.`,
        );
      }
      const fieldType = constructType(
        (field as SchemaObject).type as SchemaLike,
        childContext,
      );

      const fieldAliases = toStringArray((field as SchemaObject).aliases);
      const order = (field as SchemaObject).order;
      const fieldParams: RecordFieldParams = {
        name: fieldName,
        type: fieldType,
      };
      if (fieldAliases.length > 0) {
        fieldParams.aliases = fieldAliases;
      }
      if (
        order === "ascending" || order === "descending" || order === "ignore"
      ) {
        fieldParams.order = order;
      }
      if ((field as SchemaObject).default !== undefined) {
        fieldParams.default = (field as SchemaObject).default;
      }
      return fieldParams;
    });
  };

  /*
  The Avro spec (section 2.8 "Protocols") treats the implicit "request" type
  declared on each RPC message as a record-like structure that is not a
  standalone named type; it exists solely within that message's scope. Record
  and error types are bona fide named types that can be reused elsewhere, so
  only those should be registered in the shared registry. Skipping "request"
  avoids polluting the registry with per-message-only definitions.
  */
  const shouldRegister = typeName !== "request";
  if (shouldRegister && context.registry.has(resolved.fullName)) {
    throw new Error(
      `Duplicate Avro type name: ${resolved.fullName}`,
    );
  }

  const params: RecordTypeParams = {
    ...resolved,
    fields: buildFields,
    validate: context.validate,
    writerStrategy: context.writerStrategy,
  };
  const record = new RecordType(params);

  if (shouldRegister) {
    context.registry.set(resolved.fullName, record);
  }

  return record;
}

function createEnumType(
  schema: SchemaObject,
  context: CreateTypeContext,
): EnumType {
  const name = schema.name;
  if (typeof name !== "string" || name.length === 0) {
    throw new Error(
      `Enum schema requires a non-empty name: ${safeStringify(schema)}`,
    );
  }

  const aliases = toStringArray(schema.aliases);
  const resolved = resolveNames({
    name,
    namespace: extractNamespace(schema, context.namespace),
    aliases,
  });

  const symbols = schema.symbols;
  if (!Array.isArray(symbols) || symbols.some((s) => typeof s !== "string")) {
    throw new Error(
      `Enum schema requires an array of string symbols: ${
        safeStringify(schema)
      }`,
    );
  }

  if (context.registry.has(resolved.fullName)) {
    throw new Error(
      `Duplicate Avro type name: ${resolved.fullName}`,
    );
  }

  const params: EnumTypeParams = {
    ...resolved,
    symbols: symbols.slice() as string[],
    validate: context.validate,
  };
  if (schema.default !== undefined) {
    params.default = schema.default as string;
  }

  const enumType = new EnumType(params);
  context.registry.set(resolved.fullName, enumType);
  return enumType;
}

function createFixedType(
  schema: SchemaObject,
  context: CreateTypeContext,
): FixedType {
  const name = schema.name;
  if (typeof name !== "string" || name.length === 0) {
    throw new Error(
      `Fixed schema requires a non-empty name: ${safeStringify(schema)}`,
    );
  }

  const aliases = toStringArray(schema.aliases);
  const resolved = resolveNames({
    name,
    namespace: extractNamespace(schema, context.namespace),
    aliases,
  });

  if (context.registry.has(resolved.fullName)) {
    throw new Error(
      `Duplicate Avro type name: ${resolved.fullName}`,
    );
  }

  const params: FixedTypeParams = {
    ...resolved,
    size: schema.size as number,
    validate: context.validate,
  };

  const fixed = new FixedType(params);
  context.registry.set(resolved.fullName, fixed);
  return fixed;
}

function createArrayType(
  schema: SchemaObject,
  context: CreateTypeContext,
): ArrayType {
  if (!("items" in schema)) {
    throw new Error(
      `Array schema requires an "items" definition: ${safeStringify(schema)}`,
    );
  }
  const itemsType = constructType(schema.items as SchemaLike, context);
  return new ArrayType({ items: itemsType, validate: context.validate });
}

function createMapType(
  schema: SchemaObject,
  context: CreateTypeContext,
): MapType {
  if (!("values" in schema)) {
    throw new Error(
      `Map schema requires a "values" definition: ${safeStringify(schema)}`,
    );
  }
  const valuesType = constructType(schema.values as SchemaLike, context);
  return new MapType({ values: valuesType, validate: context.validate });
}

function createUnionType(
  schemas: SchemaLike[],
  context: CreateTypeContext,
): UnionType {
  if (schemas.length === 0) {
    throw new Error("Union schema requires at least one branch type.");
  }
  const types = schemas.map((branch) => constructType(branch, context));
  return new UnionType({ types, validate: context.validate });
}

function isPrimitiveTypeName(value: unknown): value is PrimitiveTypeName {
  return value === "null" || value === "boolean" || value === "int" ||
    value === "long" || value === "float" || value === "double" ||
    value === "bytes" || value === "string";
}

function materializeLazyRecordFields(registry: Map<string, Type>): void {
  for (const type of registry.values()) {
    if (type instanceof RecordType) {
      // Ensures nested named types defined inside record fields are registered.
      type.getFields();
    }
  }
}

function isRecordTypeName(
  value: string,
): value is "record" | "error" | "request" {
  return value === "record" || value === "error" || value === "request";
}

function toStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry) => typeof entry === "string") as string[];
}

function extractNamespace(
  schema: SchemaObject,
  fallback?: string,
): string | undefined {
  const namespace = schema.namespace;
  if (typeof namespace === "string") {
    return namespace.length > 0 ? namespace : undefined;
  }
  return fallback;
}

function qualifyReference(name: string, namespace?: string): string {
  if (!namespace || name.includes(".")) {
    return name;
  }
  return `${namespace}.${name}`;
}
