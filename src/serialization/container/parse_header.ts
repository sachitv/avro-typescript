/**
 * Parses the header at the start of an Avro object container file.
 *
 * The header is the magic bytes `Obj\x01`, a metadata map (holding the writer
 * schema under `avro.schema` and the codec under `avro.codec`), and the
 * 16-byte sync marker that also ends every block. Every reader parses it
 * first, and its length is the offset of the first block.
 *
 * `parseHeader` works on a prefix of the file and returns a `NeedMore` when
 * the prefix is too short, so a reader can start with a small first read and
 * grow it only for files with large metadata. `maxHeaderBytes` bounds that
 * growth, since metadata lengths come from the file itself. The schema is
 * returned as parsed JSON; building a `Type` from it is left to the caller.
 *
 * @module
 */

import {
  checkMagic,
  failHeader,
  parseSchema,
  readMetadata,
  readSyncMarker,
} from "./internal/header_sections.ts";
import { MAGIC_BYTES } from "../avro_constants.ts";
import { resolveCodecName } from "./codec.ts";
import { isNeedMore, type NeedMore } from "./need_more.ts";

/** The parsed header of an Avro object container file. */
export interface ContainerHeader {
  /** Header metadata. Values are copies, independent of the input bytes. */
  meta: Map<string, Uint8Array>;
  /**
   * The writer schema, parsed from the `avro.schema` metadata JSON. Integer
   * literals outside the safe integer range (such as a large `long` default)
   * are held as `bigint`s so no digits are lost.
   */
  schema: unknown;
  /** The codec name; `"null"` when `avro.codec` is missing or empty. */
  codec: string;
  /** The 16-byte sync marker, copied from the input bytes. */
  sync: Uint8Array;
  /** Header length in bytes, which is also the offset of the first block. */
  length: number;
}

/** Options for {@link parseHeader}. */
export interface ParseHeaderOptions {
  /**
   * Largest header, in bytes, the parser accepts. Metadata lengths come from
   * the file, so without a cap a corrupt or hostile length prefix could make a
   * caller that follows `minBytes` read or buffer arbitrarily much. Defaults to
   * {@link DEFAULT_MAX_HEADER_BYTES}.
   */
  maxHeaderBytes?: number;
}

/** Default header size limit: 16 MiB. */
export const DEFAULT_MAX_HEADER_BYTES = 16 * 1024 * 1024;

/**
 * Parses an object container file header from the start of `bytes`.
 *
 * The header is the magic bytes, a metadata map of string keys to byte values,
 * and a sync marker. Parsing is synchronous and does no I/O: when `bytes` ends
 * before the header does, a {@link NeedMore} is returned and the caller retries
 * with a longer prefix of the file.
 *
 * Each section is parsed by its own internal helper, in file order:
 * `checkMagic`, `readMetadata`, `readSyncMarker`, then `parseSchema` (see
 * `internal/header_sections.ts`). The helpers report a short input as a
 * {@link NeedMore}; this function is the one place that holds those requests
 * to `maxHeaderBytes`. The helpers never see more than `maxHeaderBytes` bytes,
 * even when the caller passes a whole file, so an oversized header costs no
 * more to reject than the limit allows.
 *
 * @param bytes A prefix of the file.
 * @param options Parser limits.
 * @returns The header, or a {@link NeedMore}. `minBytes` is present when the
 * parser knows how long a prefix it needs, and never exceeds `maxHeaderBytes`.
 * @throws InvalidMagicError when the available bytes do not start with the
 * Avro magic, even if fewer than four bytes are available.
 * @throws InvalidHeaderError when the metadata map is malformed, the schema is
 * missing, the schema is not valid JSON, or the header needs more than
 * `maxHeaderBytes` bytes.
 * @throws RangeError when `maxHeaderBytes` is not a positive safe integer.
 */
export function parseHeader(
  bytes: Uint8Array,
  options?: ParseHeaderOptions,
): ContainerHeader | NeedMore {
  const maxHeaderBytes = resolveMaxHeaderBytes(options);
  // Never scan past the limit, even when the caller passes a whole file. A
  // header longer than the limit then runs out of bytes in this view, and
  // withinLimit rejects the resulting request for more.
  if (bytes.length > maxHeaderBytes) {
    bytes = bytes.subarray(0, maxHeaderBytes);
  }

  const magic = checkMagic(bytes);
  if (magic !== undefined) {
    return withinLimit(magic, bytes.length, maxHeaderBytes);
  }

  const metadata = readMetadata(bytes, MAGIC_BYTES.length);
  if (isNeedMore(metadata)) {
    return withinLimit(metadata, bytes.length, maxHeaderBytes);
  }

  const sync = readSyncMarker(bytes, metadata.next);
  if (isNeedMore(sync)) {
    return withinLimit(sync, bytes.length, maxHeaderBytes);
  }

  const { meta } = metadata;
  return {
    meta,
    schema: parseSchema(meta),
    codec: resolveCodecName(meta),
    sync: sync.sync,
    length: sync.next,
  };
}

/** Reads and validates `maxHeaderBytes`, applying the default. */
function resolveMaxHeaderBytes(options?: ParseHeaderOptions): number {
  const maxHeaderBytes = options?.maxHeaderBytes ?? DEFAULT_MAX_HEADER_BYTES;
  if (!Number.isSafeInteger(maxHeaderBytes) || maxHeaderBytes <= 0) {
    throw new RangeError(
      `maxHeaderBytes must be a positive safe integer, got ${maxHeaderBytes}`,
    );
  }
  return maxHeaderBytes;
}

/** Throws when a header of `needed` bytes would exceed the limit. */
function assertWithinLimit(needed: number, maxHeaderBytes: number): void {
  if (needed > maxHeaderBytes) {
    failHeader(
      `header needs at least ${needed} bytes, more than maxHeaderBytes (${maxHeaderBytes})`,
    );
  }
}

/**
 * Passes a section's request for more bytes on to the caller, unless meeting
 * it would take the header past the limit. A request without `minBytes` still
 * needs one byte beyond the `available` bytes.
 */
function withinLimit(
  request: NeedMore,
  available: number,
  maxHeaderBytes: number,
): NeedMore {
  assertWithinLimit(request.minBytes ?? available + 1, maxHeaderBytes);
  return request;
}
