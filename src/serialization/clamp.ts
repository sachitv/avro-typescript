/**
 * Clamps a requested length for a DataView based on its byte length and an offset.
 * Ensures the returned length does not exceed the available bytes in the view from the offset.
 *
 * @param view The DataView to clamp the length for.
 * @param offset The starting offset within the DataView.
 * @param length The requested length.
 * @returns The clamped length.
 */
export function clampLengthForView(
  view: DataView,
  offset: number,
  length: number,
): number {
  if (offset >= view.byteLength) {
    return 0;
  }
  const available = view.byteLength - offset;
  if (length <= 0) {
    return 0;
  }
  return length > available ? available : length;
}
