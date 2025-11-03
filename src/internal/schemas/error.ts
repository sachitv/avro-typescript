import { Type } from "./type.ts";

export type ErrorHook<T = unknown> = (
  path: string[],
  invalidValue: unknown,
  schemaType: Type<T>,
) => void;

/**
 * Custom error class for Avro schema validation failures.
 */
export class ValidationError<T = unknown> extends Error {
  public readonly path: string[];
  public readonly value: unknown;
  public readonly type: Type<T>;

  constructor(path: string[], invalidValue: unknown, schemaType: Type<T>) {
    const serializedValue = typeof invalidValue === "bigint"
      ? `${invalidValue}n`
      : safeStringify(invalidValue);
    super(
      `Invalid value: ${serializedValue} for type: ${schemaType.constructor.name}`,
    );
    this.name = "ValidationError";
    this.path = path;
    this.value = invalidValue;
    this.type = schemaType;
  }
}

export function throwInvalidError<T = unknown>(
  path: string[],
  invalidValue: unknown,
  schemaType: Type<T>,
): never {
  throw new ValidationError(path, invalidValue, schemaType);
}

function safeStringify(value: unknown): string {
  try {
    const json = JSON.stringify(value);
    return json === undefined ? String(value) : json;
  } catch {
    return String(value);
  }
}
