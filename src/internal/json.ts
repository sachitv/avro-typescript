/**
 * JSON helpers shared by the schema and protocol readers.
 *
 * @module
 */

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
