import { Type } from "./type.ts";
import { safeStringify } from "./json.ts";

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

export function throwInvalidError<T = unknown>(
  path: string[],
  invalidValue: unknown,
  schemaType: Type<T>,
): never {
  throw new ValidationError(path, invalidValue, schemaType);
}

export function renderPathAsTree(path: string[]): string {
  if (path.length === 0) return "";
  let result = path[0];
  for (let i = 1; i < path.length; i++) {
    result += "\n" + "  ".repeat(i) + path[i];
  }
  return result;
}
