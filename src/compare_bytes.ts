import { clampLengthForView } from "./clamp.ts";

/**
 * Compares two byte ranges from DataView objects.
 * Returns a negative value if viewA is less than viewB, a positive value if viewA is greater than viewB,
 * and 0 if they are equal.
 * @param viewA The first DataView to compare.
 * @param offsetA The starting offset in viewA.
 * @param lengthA The length of the byte range in viewA.
 * @param viewB The second DataView to compare.
 * @param offsetB The starting offset in viewB.
 * @param lengthB The length of the byte range in viewB.
 * @returns A negative, positive, or zero number indicating the comparison result.
 */
export function compareByteRanges(
  viewA: DataView,
  offsetA: number,
  lengthA: number,
  viewB: DataView,
  offsetB: number,
  lengthB: number,
): number {
  const safeOffsetA = offsetA >= 0 ? offsetA : 0;
  const safeOffsetB = offsetB >= 0 ? offsetB : 0;
  const clampedLengthA = clampLengthForView(viewA, safeOffsetA, lengthA);
  const clampedLengthB = clampLengthForView(viewB, safeOffsetB, lengthB);
  const len = Math.min(clampedLengthA, clampedLengthB);

  for (let i = 0; i < len; i++) {
    const diff = viewA.getUint8(safeOffsetA + i) -
      viewB.getUint8(safeOffsetB + i);
    if (diff !== 0) {
      return diff < 0 ? -1 : 1;
    }
  }

  if (clampedLengthA === clampedLengthB) {
    return 0;
  }
  return clampedLengthA < clampedLengthB ? -1 : 1;
}

/**
 * Compares two Uint8Array objects.
 * Returns a negative value if a is less than b, a positive value if a is greater than b,
 * and 0 if they are equal.
 * @param a The first Uint8Array to compare.
 * @param b The second Uint8Array to compare.
 * @returns A negative, positive, or zero number indicating the comparison result.
 */
export function compareUint8Arrays(a: Uint8Array, b: Uint8Array): number {
  const viewA = new DataView(a.buffer, a.byteOffset, a.byteLength);
  const viewB = new DataView(b.buffer, b.byteOffset, b.byteLength);
  return compareByteRanges(viewA, 0, a.length, viewB, 0, b.length);
}
