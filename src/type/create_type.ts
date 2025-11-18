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

export type SchemaLike =
  | Type
  | string
  | SchemaObject
  | SchemaLike[];

interface SchemaObject {
  type: unknown;
  name?: unknown;
  namespace?: unknown;
  aliases?: unknown;
  fields?: unknown;
  symbols?: unknown;
  size?: unknown;
  items?: unknown;
  values?: unknown;
  default?: unknown;
  logicalType?: unknown;
  [key: string]: unknown;
}

interface CreateTypeContext {
  namespace?: string;
  registry: Map<string, Type>;
}

export interface CreateTypeOptions {
  namespace?: string;
  registry?: Map<string, Type>;
}

const PRIMITIVE_FACTORIES: Record<PrimitiveTypeName, () => Type> = {
  "null": () => new NullType(),
  "boolean": () => new BooleanType(),
  "int": () => new IntType(),
  "long": () => new LongType(),
  "float": () => new FloatType(),
  "double": () => new DoubleType(),
  "bytes": () => new BytesType(),
  "string": () => new StringType(),
};

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
      return PRIMITIVE_FACTORIES[type]();
    }
    return createFromTypeName(type, {
      namespace: extractNamespace(schema, context.namespace),
      registry: context.registry,
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
          });
          return replaceIfNamed(logical);
        }
        return underlying;
      }
      case "uuid": {
        if (
          underlying instanceof StringType || underlying instanceof FixedType
        ) {
          const logical = new UuidLogicalType(underlying);
          return replaceIfNamed(logical);
        }
        return underlying;
      }
      case "date": {
        if (underlying instanceof IntType) {
          return new DateLogicalType(underlying);
        }
        return underlying;
      }
      case "time-millis": {
        if (underlying instanceof IntType) {
          return new TimeMillisLogicalType(underlying);
        }
        return underlying;
      }
      case "time-micros": {
        if (underlying instanceof LongType) {
          return new TimeMicrosLogicalType(underlying);
        }
        return underlying;
      }
      case "timestamp-millis": {
        if (underlying instanceof LongType) {
          return new TimestampMillisLogicalType(underlying);
        }
        return underlying;
      }
      case "timestamp-micros": {
        if (underlying instanceof LongType) {
          return new TimestampMicrosLogicalType(underlying);
        }
        return underlying;
      }
      case "timestamp-nanos": {
        if (underlying instanceof LongType) {
          return new TimestampNanosLogicalType(underlying);
        }
        return underlying;
      }
      case "local-timestamp-millis": {
        if (underlying instanceof LongType) {
          return new LocalTimestampMillisLogicalType(underlying);
        }
        return underlying;
      }
      case "local-timestamp-micros": {
        if (underlying instanceof LongType) {
          return new LocalTimestampMicrosLogicalType(underlying);
        }
        return underlying;
      }
      case "local-timestamp-nanos": {
        if (underlying instanceof LongType) {
          return new LocalTimestampNanosLogicalType(underlying);
        }
        return underlying;
      }
      case "duration": {
        if (underlying instanceof FixedType) {
          const logical = new DurationLogicalType(underlying);
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
    return PRIMITIVE_FACTORIES[name as PrimitiveTypeName]();
  }

  const fullName = qualifyReference(name, context.namespace);
  const found = context.registry.get(fullName);
  if (!found) {
    throw new Error(`Undefined Avro type reference: ${name}`);
  }
  return found;
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
  return new ArrayType({ items: itemsType });
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
  return new MapType({ values: valuesType });
}

function createUnionType(
  schemas: SchemaLike[],
  context: CreateTypeContext,
): UnionType {
  if (schemas.length === 0) {
    throw new Error("Union schema requires at least one branch type.");
  }
  const types = schemas.map((branch) => constructType(branch, context));
  return new UnionType({ types });
}

function isPrimitiveTypeName(value: unknown): value is PrimitiveTypeName {
  return typeof value === "string" && value in PRIMITIVE_FACTORIES;
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
