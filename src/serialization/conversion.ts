const MAX_SAFE_INTEGER_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE_INTEGER_BIGINT = BigInt(Number.MIN_SAFE_INTEGER);

/**
 * Converts a bigint to a number, throwing a RangeError if the bigint is outside the safe integer range.
 * @param value The bigint to convert.
 * @param context A string describing the context of the conversion, used in the error message if an error occurs.
 * @returns The converted number.
 * @throws {RangeError} if the bigint is outside the safe integer range.
 */
export function bigIntToSafeNumber(value: bigint, context: string): number {
  if (value > MAX_SAFE_INTEGER_BIGINT || value < MIN_SAFE_INTEGER_BIGINT) {
    throw new RangeError(
      `${context} value ${value} is outside the safe integer range.`,
    );
  }
  return Number(value);
}
