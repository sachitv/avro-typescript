/**
 * Throws when a block's trailing sync marker does not match the file header's
 * sync marker, which indicates a corrupted or misaligned container file.
 *
 * @param actual The sync marker read after the block data.
 * @param expected The sync marker from the file header.
 * @param blockOffset The byte offset at which the block started.
 * @throws Error if the markers differ.
 */
export function assertSyncMarker(
  actual: Uint8Array,
  expected: Uint8Array,
  blockOffset: number,
): void {
  for (let i = 0; i < expected.length; i++) {
    if (actual[i] !== expected[i]) {
      throw new Error(
        `Invalid AVRO file: sync marker mismatch for block at offset ${blockOffset}`,
      );
    }
  }
}
