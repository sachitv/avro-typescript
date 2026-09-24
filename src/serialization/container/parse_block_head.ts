/**
 * Parses the prefix of a data block in an Avro object container file.
 *
 * After the header, a container file is a sequence of blocks, each laid out as
 * `[record count][data byte length][data][16-byte sync marker]`. The two
 * leading varints tell a reader how many records the block holds, where its
 * data is, and where the next block starts, without decompressing or decoding
 * anything. That is what lets readers skip blocks, index them, and read a
 * single block at a known offset.
 *
 * `parseBlockHead` takes bytes that start at the block, plus the block's file
 * offset for error messages, so it works on a slice of a larger buffer.
 *
 * @module
 */

import { InvalidBlockError } from "./errors.ts";
import { type NeedMore, needMore } from "./need_more.ts";
import { readSafeLong } from "./internal/varint.ts";

/**
 * The prefix of a container data block. A block is laid out as
 * `[count][byteLength][data: byteLength bytes][sync marker: 16 bytes]`.
 */
export interface BlockHead {
  /**
   * Number of records in the block.
   *
   * Avro encodes the count as a long, but it is a `number` here and
   * {@link parseBlockHead} rejects counts above `Number.MAX_SAFE_INTEGER`.
   * Only records that encode to zero bytes (such as the `"null"` schema or an
   * empty record) could make such a count valid, since every other record
   * takes at least one byte of block data, and no reader could yield 2^53
   * records anyway. Rejecting it gives a clear error instead of a count that
   * silently loses precision.
   */
  count: number;
  /** Length of the block's (possibly compressed) data in bytes. */
  byteLength: number;
  /** Length of the two varints; the data starts this many bytes into the block. */
  headLength: number;
}

/**
 * Parses the record count and data length that start a data block.
 *
 * `byteLength` comes from the file and is not checked against anything, so
 * callers should confirm the block fits in the source before reading its data.
 *
 * @param bytes Bytes that start at the block's first byte.
 * @param fileOffset The block's offset in the file, used only in error
 * messages. Required so a slice never reports the wrong offset.
 * @returns The block prefix, or a {@link NeedMore} without `minBytes` when
 * `bytes` ends inside one of the varints.
 * @throws InvalidBlockError when either value is negative, not a safe integer,
 * or encoded in more than ten bytes.
 */
export function parseBlockHead(
  bytes: Uint8Array,
  fileOffset: number,
): BlockHead | NeedMore {
  const fail = (reason: string): never => {
    throw new InvalidBlockError(fileOffset, reason);
  };

  const count = readSafeLong(bytes, 0, fail);
  if (count === undefined) {
    return needMore();
  }
  if (count.value < 0) {
    fail(`negative record count ${count.value}`);
  }

  const byteLength = readSafeLong(bytes, count.next, fail);
  if (byteLength === undefined) {
    return needMore();
  }
  if (byteLength.value < 0) {
    fail(`negative byte length ${byteLength.value}`);
  }

  return {
    count: count.value,
    byteLength: byteLength.value,
    headLength: byteLength.next,
  };
}
