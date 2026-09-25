/** @internal */
export function _safeJSONStringify(obj: unknown, indent = 2): string {
  const cache: unknown[] = [];
  const retVal = JSON.stringify(obj, (_key, value) => {
    if (typeof value === "bigint") return String(value);
    if (typeof value === "object" && value !== null) {
      if (cache.includes(value)) return "[Circular]";
      cache.push(value);
    }
    return value;
  }, indent);
  cache.length = 0;
  return retVal;
}

/**
 * Safely stringifies a value to JSON, handling BigInts and circular references.
 * @param value The value to stringify.
 * @returns The string representation.
 */
export function safeStringify(value: unknown): string {
  // Converts supported primitive values and complex objects into stable JSON
  // while handling circular references and bigint values.
  if (
    typeof value === "string" || typeof value === "bigint" ||
    typeof value === "number" || typeof value === "boolean" || value === null ||
    typeof value === "undefined"
  ) {
    return String(value);
  }
  const jsonStr = _safeJSONStringify(value);
  if (jsonStr === undefined) {
    return String(value);
  }
  return `\n${jsonStr}\n`;
}

/** A JSON integer literal: an optional minus sign and digits only. */
const INTEGER_LITERAL = /^-?\d+$/;

/**
 * Parses JSON like `JSON.parse`, except that an integer literal outside the
 * safe integer range is read as a `bigint` with all its digits instead of a
 * rounded number. Schemas write `long` defaults that large as JSON numbers
 * (see `LongType.defaultToJSON`), and this keeps them exact.
 *
 * Needs the reviver's `context.source` (ES2025); on a runtime without it,
 * such literals are read as numbers, as `JSON.parse` does.
 *
 * @typeParam T The type the caller expects; like `JSON.parse`, the value is
 * not checked against it.
 * @param text The JSON text.
 * @returns The parsed value.
 * @internal
 */
export function parseJSON<T = unknown>(text: string): T {
  return JSON.parse(
    text,
    (_key: string, value: unknown, context?: { source?: string }) => {
      const source = context?.source;
      if (
        typeof value === "number" && !Number.isSafeInteger(value) &&
        source !== undefined && INTEGER_LITERAL.test(source)
      ) {
        return BigInt(source);
      }
      return value;
    },
  );
}
