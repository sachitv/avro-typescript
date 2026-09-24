/**
 * Bounds-checked varint decoding for the container parsers.
 *
 * Avro encodes lengths and counts as zig-zag varints of 1 to 10 bytes. The tap
 * classes read them from input they expect to be complete; the container
 * parsers work on partial input, so `readSafeLong` reports running out of
 * bytes by returning `undefined` rather than throwing, and the parsers turn
 * that into a `NeedMore`. It returns a `number` rather than a `bigint`
 * because everything the container layer reads is a length, count, or offset,
 * and it rejects values outside the safe integer range.
 *
 * @module
 */

/** A zig-zag varint decoded from a byte array. */
export interface VarintRead {
  /** The decoded value. */
  value: number;
  /** Index of the first byte after the varint. */
  next: number;
}

/** An Avro long never needs more than ten varint bytes. */
const MAX_VARINT_BYTES = 10;

/**
 * Seven varint bytes carry 49 payload bits, which a double holds exactly, so
 * the common short varint is decoded without BigInt arithmetic.
 */
const NUMBER_VARINT_BYTES = 7;

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);

/**
 * Decodes the zig-zag varint long starting at `offset`.
 *
 * @param bytes The bytes to read from.
 * @param offset Index of the varint's first byte.
 * @param fail Called with a description when the varint is longer than an
 * Avro long allows or its value is outside the safe integer range.
 * @returns The value and the index after it, or `undefined` when `bytes` ends
 * before the varint does.
 */
export function readSafeLong(
  bytes: Uint8Array,
  offset: number,
  fail: (reason: string) => never,
): VarintRead | undefined {
  // An Avro long is encoded in two steps, which this function undoes in
  // reverse order:
  //
  // 1. Zig-zag: the signed value is mapped to an unsigned "raw" value so that
  //    small negative numbers stay small. Non-negative n becomes 2n (even) and
  //    negative n becomes -2n - 1 (odd):
  //
  //      value:  0  -1   1  -2   2  -3  ...
  //      raw:    0   1   2   3   4   5  ...
  //
  // 2. Varint: raw is split into 7-bit groups, least significant first. Each
  //    group is stored in one byte whose high bit (0x80) is set when more
  //    bytes follow and clear on the last byte. A 64-bit raw value needs at
  //    most ten bytes (10 * 7 = 70 bits).
  //
  // Example: -300 zig-zags to raw 599 = 0b100_1010111, which is stored as
  // 0xd7 (0x80 | 0b1010111) then 0x04 (0b100, high bit clear: last byte).

  let pos = offset;
  // Raw is accumulated in a double. Bitwise operators would truncate to 32
  // bits, so each group is multiplied by its place value instead of shifted.
  let raw = 0;
  // Place value of the next 7-bit group: 128^i for byte i.
  let scale = 1;
  for (let i = 0; i < NUMBER_VARINT_BYTES; i++) {
    if (pos >= bytes.length) {
      // The varint continues past the bytes we have: ask for more.
      return undefined;
    }
    const byte = bytes[pos++]!;
    // Drop the continuation bit and add the 7 payload bits at their place.
    raw += (byte & 0x7f) * scale;
    if (byte < 0x80) {
      // Continuation bit clear: this was the last byte. Undo the zig-zag. raw
      // is below 2^49 here, so it and its parity are exact.
      //   even raw: value = raw / 2              (e.g. raw 4 -> 2)
      //   odd raw:  value = -(raw + 1) / 2       (e.g. raw 599 -> -300)
      // raw + 1 is even in the odd case, so the division is exact too.
      const value = raw % 2 === 0 ? raw / 2 : -(raw + 1) / 2;
      return { value, next: pos };
    }
    scale *= 128;
  }

  // Seven bytes were not enough, so raw may need up to 64 bits: more than a
  // double holds exactly. Carry on in BigInt from the 49 bits read so far.
  let bigRaw = BigInt(raw);
  let bigScale = BigInt(scale);
  for (let i = NUMBER_VARINT_BYTES; i < MAX_VARINT_BYTES; i++) {
    if (pos >= bytes.length) {
      return undefined;
    }
    const byte = bytes[pos++]!;
    bigRaw += BigInt(byte & 0x7f) * bigScale;
    if (byte < 0x80) {
      // The same zig-zag decode, written with BigInt bit operations:
      //   bigRaw >> 1n     is raw / 2, rounded down.
      //   -(bigRaw & 1n)   is 0n for even raw and -1n (all bits set) for odd.
      // XOR with 0n leaves raw / 2 unchanged (non-negative values); XOR with
      // -1n flips every bit, giving -(raw / 2) - 1 = -(raw + 1) / 2 (negative
      // values). Example: raw 599 -> 299 ^ -1n = -300.
      const value = (bigRaw >> 1n) ^ -(bigRaw & 1n);
      // Values past 2^53 in either direction would lose precision as a number.
      if (value > MAX_SAFE || value < MIN_SAFE) {
        fail(`value ${value} is outside the safe integer range`);
      }
      return { value: Number(value), next: pos };
    }
    bigScale <<= 7n;
  }
  // Ten bytes all had the continuation bit set: no valid long is that long.
  return fail(`varint is longer than ${MAX_VARINT_BYTES} bytes`);
}
