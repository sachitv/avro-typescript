/**
 * The sections of an Avro object container file header, one parser each.
 *
 * `parseHeader` (in `../parse_header.ts`) is the
 * entry point; it calls these in file order: {@link checkMagic},
 * {@link readMetadata} (built from {@link readMapBlockCount},
 * {@link readMetadataEntry}, and {@link readLengthPrefixed}),
 * {@link readSyncMarker}, then {@link parseSchema}. They live in `internal/`, outside the
 * container API, so each section can be tested on its own without becoming
 * something other code depends on.
 *
 * Every section works on a prefix of the file and reports a short input as a
 * `NeedMore`. None of them enforces the header size limit; `parseHeader` does
 * that in one place.
 *
 * @module
 */

import { MAGIC_BYTES } from "../../avro_constants.ts";
import { parseJSON } from "../../../schemas/json.ts";
import { InvalidHeaderError, InvalidMagicError } from "../errors.ts";
import { isNeedMore, type NeedMore, needMore } from "../need_more.ts";
import { readSafeLong } from "./varint.ts";
import { SYNC_SIZE } from "./sync_marker.ts";

/** Metadata key that holds the writer schema as JSON. */
export const SCHEMA_KEY = "avro.schema";

const utf8 = new TextDecoder();

/**
 * Throws an {@link InvalidHeaderError} with the standard header-error prefix.
 * @param reason What is wrong with the header.
 */
export function failHeader(reason: string): never {
  throw new InvalidHeaderError(`Invalid AVRO file header: ${reason}`);
}

/**
 * Checks that `bytes` starts with the Avro magic `Obj\x01`.
 *
 * @param bytes A prefix of the file.
 * @returns `undefined` when all four magic bytes match, or a {@link NeedMore}
 * when fewer than four bytes are available and those match.
 * @throws InvalidMagicError as soon as an available byte differs.
 */
export function checkMagic(bytes: Uint8Array): NeedMore | undefined {
  const available = Math.min(bytes.length, MAGIC_BYTES.length);
  for (let i = 0; i < available; i++) {
    if (bytes[i] !== MAGIC_BYTES[i]) {
      throw new InvalidMagicError();
    }
  }
  if (bytes.length < MAGIC_BYTES.length) {
    return needMore(MAGIC_BYTES.length);
  }
  return undefined;
}

/**
 * The start and end indexes of a field inside the parser's input.
 */
export interface Span {
  /** Index of the field's first byte. */
  start: number;
  /** Index just past the field's last byte. */
  end: number;
}

/**
 * Reads the whole metadata map starting at `pos`.
 *
 * Avro writes a map as a series of blocks, each a count followed by that many
 * entries, and ends it with a block whose count is 0. The library's writer
 * uses one block, but other writers may split the map.
 *
 * Entries are only located while scanning. Keys are decoded and values copied
 * once the whole map is available, so a call that ends in a {@link NeedMore}
 * does no per-entry decoding or copying that the retry would repeat.
 *
 * @param bytes A prefix of the file.
 * @param pos Index of the map's first block count.
 * @returns The metadata and the index after the map's terminating 0, or a
 * {@link NeedMore}. Later entries replace earlier ones with the same key.
 * Values are copies, independent of `bytes`.
 * @throws InvalidHeaderError when a count or length is malformed.
 */
export function readMetadata(
  bytes: Uint8Array,
  pos: number,
): { meta: Map<string, Uint8Array>; next: number } | NeedMore {
  const entries: { key: Span; value: Span }[] = [];
  while (true) {
    const block = readMapBlockCount(bytes, pos);
    if (isNeedMore(block)) {
      return block;
    }
    pos = block.next;
    if (block.items === 0) {
      break;
    }
    for (let i = 0; i < block.items; i++) {
      const entry = readMetadataEntry(bytes, pos);
      if (isNeedMore(entry)) {
        return entry;
      }
      entries.push(entry);
      pos = entry.value.end;
    }
  }

  const meta = new Map<string, Uint8Array>();
  for (const { key, value } of entries) {
    // Copied so the header never aliases a buffer the caller may reuse.
    meta.set(
      utf8.decode(bytes.subarray(key.start, key.end)),
      bytes.slice(value.start, value.end),
    );
  }
  return { meta, next: pos };
}

/**
 * Reads the item count that starts a map block.
 *
 * A negative count means the same number of items, followed by the block's
 * size in bytes. That size lets a reader skip the block, which a parser that
 * reads every entry does not need, so it is only checked to be non-negative
 * and then discarded.
 *
 * @param bytes A prefix of the file.
 * @param pos Index of the block count.
 * @returns The number of entries (0 for the map's end) and the index of the
 * first entry, or a {@link NeedMore} without `minBytes`.
 * @throws InvalidHeaderError when a varint is malformed or the block size is
 * negative.
 */
export function readMapBlockCount(
  bytes: Uint8Array,
  pos: number,
): { items: number; next: number } | NeedMore {
  const count = readSafeLong(bytes, pos, failHeader);
  if (count === undefined) {
    return needMore();
  }
  if (count.value >= 0) {
    return { items: count.value, next: count.next };
  }
  const size = readSafeLong(bytes, count.next, failHeader);
  if (size === undefined) {
    return needMore();
  }
  if (size.value < 0) {
    failHeader(`negative metadata block size ${size.value}`);
  }
  return { items: -count.value, next: size.next };
}

/**
 * Locates one metadata entry: a string key, then a bytes value. Nothing is
 * decoded or copied; see {@link readMetadata}.
 *
 * @param bytes A prefix of the file.
 * @param pos Index of the key's length prefix.
 * @returns Where the key and value are in `bytes` (the entry ends at
 * `value.end`), or a {@link NeedMore}.
 * @throws InvalidHeaderError when either length is negative or malformed.
 */
export function readMetadataEntry(
  bytes: Uint8Array,
  pos: number,
): { key: Span; value: Span } | NeedMore {
  const key = readLengthPrefixed(bytes, pos, () => "metadata key");
  if (isNeedMore(key)) {
    return key;
  }
  const value = readLengthPrefixed(
    bytes,
    key.end,
    // Only decoded to name the key in an error message.
    () => `metadata value "${utf8.decode(bytes.subarray(key.start, key.end))}"`,
  );
  if (isNeedMore(value)) {
    return value;
  }
  return { key, value };
}

/**
 * Locates a length-prefixed field (Avro `bytes` or `string`) without copying it.
 *
 * @param bytes A prefix of the file.
 * @param pos Index of the length prefix.
 * @param describeField Names the field in error messages. Only called when
 * throwing, so it may decode bytes without slowing down valid input.
 * @returns The field's span, or a {@link NeedMore}: without `minBytes` when
 * the length prefix is cut off, with the field's end when the field's
 * contents are.
 * @throws InvalidHeaderError when the length is negative, malformed, or so
 * large that the field's end is not a safe integer.
 */
export function readLengthPrefixed(
  bytes: Uint8Array,
  pos: number,
  describeField: () => string,
): Span | NeedMore {
  const length = readSafeLong(bytes, pos, failHeader);
  if (length === undefined) {
    return needMore();
  }
  if (length.value < 0) {
    failHeader(`negative length for ${describeField()}`);
  }
  // A hostile length near MAX_SAFE_INTEGER would push the end past the safe
  // range, where needMore() rejects it with a RangeError instead of an error
  // from the container hierarchy.
  if (length.value > Number.MAX_SAFE_INTEGER - length.next) {
    failHeader(`length ${length.value} for ${describeField()} is too large`);
  }
  const end = length.next + length.value;
  if (end > bytes.length) {
    return needMore(end);
  }
  return { start: length.next, end };
}

/**
 * Reads the 16-byte sync marker that ends the header.
 *
 * @param bytes A prefix of the file.
 * @param pos Index of the marker's first byte.
 * @returns A copy of the marker and the index after it (the header length), or
 * a {@link NeedMore} for the marker's end.
 */
export function readSyncMarker(
  bytes: Uint8Array,
  pos: number,
): { sync: Uint8Array; next: number } | NeedMore {
  const next = pos + SYNC_SIZE;
  if (next > bytes.length) {
    return needMore(next);
  }
  return { sync: bytes.slice(pos, next), next };
}

/**
 * Parses the writer schema JSON stored under `avro.schema`.
 *
 * @param meta The header's metadata.
 * @returns The parsed JSON, with integer literals outside the safe integer
 * range read as `bigint`s (see `parseJSON`). Building a `Type` from it is left
 * to the caller.
 * @throws InvalidHeaderError when the entry is missing or not valid JSON; the
 * `SyntaxError` is kept as the `cause`.
 */
export function parseSchema(meta: ReadonlyMap<string, Uint8Array>): unknown {
  const schemaBytes = meta.get(SCHEMA_KEY);
  if (schemaBytes === undefined) {
    throw new InvalidHeaderError("AVRO schema not found in metadata");
  }
  try {
    return parseJSON(utf8.decode(schemaBytes));
  } catch (cause) {
    throw new InvalidHeaderError(
      "Invalid AVRO file header: avro.schema is not valid JSON",
      { cause },
    );
  }
}
