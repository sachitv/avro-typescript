/**
 * Inverts the bits of the bytes in a Uint8Array up to a specified length.
 * @param arr The Uint8Array to invert.
 * @param len The number of bytes to invert from the beginning of the array.
 */
export function invert(arr: Uint8Array, len: number): void {
  const view = new DataView(arr.buffer, arr.byteOffset, arr.byteLength);
  let remaining = Math.min(Math.max(len, 0), arr.length);
  while (remaining--) {
    const current = view.getUint8(remaining);
    view.setUint8(remaining, (~current) & 0xff);
  }
}
