/**
 * Calculates the number of bytes required to encode a value as a zigzag varint.
 * @param value The number or bigint to encode.
 * @returns The size in bytes (1-10).
 */
export function calculateVarintSize(value: number | bigint): number {
  const val = typeof value === 'number' ? BigInt(value) : value;
  // Zigzag encoding: (val << 1) ^ (val >> 63)
  const zigzag = (val << 1n) ^ (val >> 63n);
  // Now calculate varint size for the unsigned zigzag value
  if (zigzag < 128n) return 1;
  if (zigzag < 16384n) return 2;
  if (zigzag < 2097152n) return 3;
  if (zigzag < 268435456n) return 4;
  if (zigzag < 34359738368n) return 5;
  if (zigzag < 4398046511104n) return 6;
  if (zigzag < 562949953421312n) return 7;
  if (zigzag < 72057594037927936n) return 8;
  if (zigzag < 9223372036854775808n) return 9;
  return 10;
}