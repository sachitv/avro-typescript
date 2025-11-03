export function readUIntLE(
  view: DataView,
  offset: number,
  byteLength: number,
): number {
  let value = 0;
  for (let i = 0; i < byteLength; i++) {
    const index = offset + i;
    if (index >= view.byteLength) {
      break;
    }
    value |= view.getUint8(index) << (8 * i);
  }
  return value >>> 0;
}
