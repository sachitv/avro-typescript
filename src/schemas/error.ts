import type { Type } from "./type.ts";
import { safeStringify } from "./json.ts";

/**
 * Hook function for handling validation errors.
 */
export type ErrorHook<T = unknown> = (
  path: string[],
  invalidValue: unknown,
  schemaType: Type<T>,
) => void;

/**
 * Custom error class for Avro schema validation failures.
 */
export class ValidationError<T = unknown> extends Error {
  /** The path to the invalid value within the schema/data. */
  public readonly path: string[];
  /** The value that failed validation. */
  public readonly value: unknown;
  /** The schema type that rejected the value. */
  public readonly type: Type<T>;

  /**
   * Creates a new ValidationError.
   * @param path The path to the invalid value.
   * @param invalidValue The invalid value.
   * @param schemaType The schema type.
   */
  constructor(path: string[], invalidValue: unknown, schemaType: Type<T>) {
    const serializedValue = safeStringify(invalidValue);
    const serializedJSON = safeStringify(schemaType.toJSON());
    let message =
      `Invalid value: \'${serializedValue}\' for type: ${serializedJSON}`;
    if (path.length > 0) {
      message += ` at path: ${renderPathAsTree(path)}`;
    }
    super(message);
    this.name = "ValidationError";
    this.path = path;
    this.value = invalidValue;
    this.type = schemaType;
  }
}

/**
 * Throws a ValidationError for invalid values.
 */
export function throwInvalidError<T = unknown>(
  path: string[],
  invalidValue: unknown,
  schemaType: Type<T>,
): never {
  throw new ValidationError(path, invalidValue, schemaType);
}

/**
 * Renders a path array as a formatted tree string.
 */
export function renderPathAsTree(path: string[]): string {
  if (path.length === 0) return "";
  let result = path[0];
  for (let i = 1; i < path.length; i++) {
    result += "\n" + "  ".repeat(i) + path[i];
  }
  if (path.length > 1) {
    result = "\n" + result;
  }
  return result;
}
