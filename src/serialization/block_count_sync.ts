import type { SyncReadableTapLike } from "./tap_sync.ts";

/**
 * Reads a sync array or map block header and returns its item count.
 *
 * Block counts are Avro longs, so this accepts every count the async block
 * readers do; an int-only read would reject valid counts whose varint is
 * wider than int32 allows. `readLength` still decodes the common short varint
 * without allocating a bigint, which matters because small nested arrays and
 * maps read two block headers per value. A negative count announces a
 * size-prefixed block, whose byte size is skipped here.
 *
 * @param tap The tap positioned at a block header.
 * @param context Names the count in the RangeError raised when it is outside
 * the safe integer range.
 * @returns The number of items in the block; zero marks the end.
 */
export function readBlockCountSync(
  tap: SyncReadableTapLike,
  context: string,
): number {
  const count = tap.readLength(context);
  if (count < 0) {
    tap.skipLong();
    return -count;
  }
  return count;
}
