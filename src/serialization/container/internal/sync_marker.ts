/**
 * Sync marker checks and searches for Avro object container files.
 *
 * Every container file has a random 16-byte sync marker, stored at the end of
 * the header and repeated after every block. Readers use it in two ways:
 *
 * - To validate a block: the marker after its data must equal the header's
 *   (`matchesSync`).
 * - To find block boundaries without walking from the start of the file:
 *   scanning forward (`findNextSync`) resynchronizes from an arbitrary offset,
 *   and scanning backward from the end (`findPrevSync`) finds where the last
 *   block starts, such as a footer block that points to an index.
 *
 * Record data or metadata can contain the marker bytes by chance, so a match
 * from a search is only a candidate until the block after it parses and ends
 * in the marker.
 *
 * @module
 */

/** Length in bytes of an Avro container sync marker. */
export const SYNC_SIZE = 16;

/**
 * Tells whether `sync` occurs in `bytes` at `offset`.
 * @param bytes The bytes to inspect.
 * @param offset Index at which the marker is expected.
 * @param sync The file's sync marker.
 * @returns `false` when the marker differs or `bytes` ends before it would.
 */
export function matchesSync(
  bytes: Uint8Array,
  offset: number,
  sync: Uint8Array,
): boolean {
  if (offset < 0 || offset + sync.length > bytes.length) {
    return false;
  }
  for (let i = 0; i < sync.length; i++) {
    if (bytes[offset + i] !== sync[i]) {
      return false;
    }
  }
  return true;
}

/**
 * Finds the first occurrence of `sync` that starts at or after `from`.
 *
 * Starting a forward scan from an arbitrary offset lands on the next block
 * boundary: the returned index plus the marker length is where a block starts.
 * Record data can contain the marker bytes by chance, so callers should confirm
 * a match by parsing the block that follows it.
 *
 * @param bytes The bytes to search.
 * @param sync The file's sync marker.
 * @param from Index at which the search starts.
 * @returns The marker's start index, or -1 when none is found.
 */
export function findNextSync(
  bytes: Uint8Array,
  sync: Uint8Array,
  from = 0,
): number {
  const last = bytes.length - sync.length;
  let i = bytes.indexOf(sync[0]!, Math.max(0, from));
  while (i !== -1 && i <= last) {
    if (matchesSync(bytes, i, sync)) {
      return i;
    }
    i = bytes.indexOf(sync[0]!, i + 1);
  }
  return -1;
}

/**
 * Finds the last occurrence of `sync` that ends at or before `before`.
 *
 * `before` and the returned index are positions in `bytes`, not in the file.
 * When `bytes` is only the file's tail, add the tail's file offset to turn a
 * match into a file offset.
 *
 * To find where the last block of a file starts, search the tail with `before`
 * set to the position of the final sync marker in that tail
 * (`tail.length - SYNC_SIZE` when the tail ends at the end of the file). The
 * match is the preceding block's marker, or the header's marker in a
 * single-block file, and the last block starts right after it. As with
 * {@link findNextSync}, confirm a match by parsing the block. If it does not
 * parse, look for an earlier candidate by searching again with `before` set to
 * `match + SYNC_SIZE - 1`.
 *
 * @param bytes The bytes to search.
 * @param sync The file's sync marker.
 * @param before Position in `bytes` that the marker must end at or before.
 * @returns The marker's start index, or -1 when none is found.
 */
export function findPrevSync(
  bytes: Uint8Array,
  sync: Uint8Array,
  before: number = bytes.length,
): number {
  const start = Math.min(before, bytes.length) - sync.length;
  if (start < 0) {
    return -1;
  }
  let i = bytes.lastIndexOf(sync[0]!, start);
  while (i !== -1) {
    if (matchesSync(bytes, i, sync)) {
      return i;
    }
    // lastIndexOf treats a negative start as counting from the end.
    if (i === 0) {
      return -1;
    }
    i = bytes.lastIndexOf(sync[0]!, i - 1);
  }
  return -1;
}
