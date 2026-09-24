/**
 * The "not enough bytes yet" result shared by the container parsers.
 *
 * The container core parses bytes it is handed and never does I/O, so when
 * those bytes end partway through a header or block prefix it cannot wait for
 * more. Throwing would make a short read look like a corrupt file, so a parser
 * returns a `NeedMore` instead; the reader fetches more bytes however its
 * source allows and calls the parser again. This keeps one parser usable from
 * in-memory buffers, random-access files, and streams alike.
 *
 * `isNeedMore` tells the result apart from a parsed value. `needMore` is the
 * only way to build one with a `minBytes`, which the `PositiveByteCount` brand
 * enforces at compile time.
 *
 * @module
 */

/**
 * Type-only key that brands {@link PositiveByteCount}. It is declared, not
 * defined, so it has no runtime value and cannot be used to build a brand;
 * exporting it only lets documentation tools describe the public type.
 */
export declare const positiveByteCount: unique symbol;

/**
 * A byte count checked to be a positive safe integer. Only {@link needMore}
 * creates one, so a {@link NeedMore} can never carry a zero, negative, or
 * fractional `minBytes`, not even from an object literal.
 */
export type PositiveByteCount = number & {
  readonly [positiveByteCount]: true;
};

/**
 * Returned by the container parsers when the bytes they were given end before
 * the structure being parsed does. Nothing is consumed; the caller fetches more
 * bytes and calls the parser again with the longer buffer.
 *
 * The parsers keep no state between calls, so every retry parses from the
 * start of the buffer again. Callers should grow their buffer geometrically
 * (for example, doubling a chunk size) rather than one byte at a time, and
 * treat `minBytes` as a hint to skip ahead, never as the next read size on its
 * own.
 *
 * `minBytes` comes from length prefixes in the file, which may be corrupt or
 * hostile. {@link parseHeader} caps it with `maxHeaderBytes`; callers should
 * also check it against the source size when they know it.
 */
export interface NeedMore {
  /** Discriminant that marks this result as a request for more bytes. */
  readonly needMore: true;
  /**
   * When present, a lower bound on the buffer length the next call needs,
   * measured from index 0 of the buffer that was passed in and always greater
   * than that buffer's length. It is a bound for the next call only: once it
   * is met, the parser may ask again with a larger bound.
   *
   * `minBytes` is a total length, not a count of additional bytes: with 2
   * bytes buffered and `minBytes: 4`, fetch 2 more.
   *
   * When absent, the parser stopped inside a variable-length field (a varint)
   * and only knows it needs at least one more byte.
   */
  readonly minBytes?: PositiveByteCount;
}

/**
 * Creates a {@link NeedMore} result.
 * @param minBytes Lower bound on the buffer length the next call needs, or
 * `undefined` when the parser cannot tell. The property is omitted entirely
 * when `undefined`.
 * @throws RangeError when `minBytes` is not a positive safe integer.
 */
export function needMore(minBytes?: number): NeedMore {
  if (minBytes === undefined) {
    return { needMore: true };
  }
  if (!Number.isSafeInteger(minBytes) || minBytes <= 0) {
    throw new RangeError(
      `minBytes must be a positive safe integer, got ${minBytes}`,
    );
  }
  return { needMore: true, minBytes: minBytes as PositiveByteCount };
}

/**
 * Tells a parser's {@link NeedMore} result apart from a parsed value.
 * @param result A value returned by a container parser.
 */
export function isNeedMore<T extends object>(
  result: T | NeedMore,
): result is NeedMore {
  return (result as Partial<NeedMore>).needMore === true;
}
