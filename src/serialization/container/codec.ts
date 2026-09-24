/**
 * Codec handling shared by the async and sync container readers.
 *
 * A container file's codec, named by the `avro.codec` metadata entry, is the
 * compression applied to each block's data; the header, block prefixes, and
 * sync markers are never compressed. These helpers decide which codec a file
 * uses and which decoder handles it, but never decompress anything: decoders
 * are async for the async readers and sync for the sync readers, so the
 * helpers are generic over the decoder type and leave decoding to the caller.
 *
 * - `resolveCodecName` reads the name, defaulting to `"null"` (uncompressed).
 * - `assertNoBuiltInOverride` rejects custom decoders that shadow built-ins.
 * - `selectDecoder` picks the decoder, or throws `UnsupportedCodecError`.
 *
 * Lookups use own properties only, so a file that declares a codec such as
 * `"toString"` is rejected instead of resolving through `Object.prototype`.
 *
 * @module
 */

import { UnsupportedCodecError } from "./errors.ts";

/** Metadata key that names the codec used to compress block data. */
export const CODEC_KEY = "avro.codec";

const utf8 = new TextDecoder();

/**
 * Reads the codec name from container metadata. A missing or empty
 * `avro.codec` entry means the data is uncompressed (`"null"`).
 * @param meta The header's metadata map.
 */
export function resolveCodecName(
  meta: ReadonlyMap<string, Uint8Array>,
): string {
  const bytes = meta.get(CODEC_KEY);
  if (bytes === undefined || bytes.length === 0) {
    return "null";
  }
  return utf8.decode(bytes);
}

/**
 * Rejects custom decoders that would replace a built-in codec.
 * @param builtIns Decoders the parser always provides.
 * @param custom Decoders supplied by the caller.
 * @throws Error naming the first custom codec that shadows a built-in.
 */
export function assertNoBuiltInOverride(
  builtIns: Readonly<Record<string, unknown>>,
  custom: Readonly<Record<string, unknown>>,
): void {
  for (const codec of Object.keys(custom)) {
    if (Object.hasOwn(builtIns, codec)) {
      throw new Error(`Cannot override built-in decoder for codec: ${codec}`);
    }
  }
}

/**
 * Looks up the decoder for `codec`, preferring built-ins. Only own properties
 * count, so names such as `"toString"` are not resolved through the prototype.
 * Generic over the decoder type so the async and sync parsers share it.
 * @param codec The codec name from the file header.
 * @param builtIns Decoders the parser always provides.
 * @param custom Decoders supplied by the caller.
 * @throws UnsupportedCodecError when neither registry has the codec.
 */
export function selectDecoder<D>(
  codec: string,
  builtIns: Readonly<Record<string, D>>,
  custom: Readonly<Record<string, D>>,
): D {
  if (Object.hasOwn(builtIns, codec)) {
    return builtIns[codec]!;
  }
  if (Object.hasOwn(custom, codec)) {
    return custom[codec]!;
  }
  throw new UnsupportedCodecError(codec);
}
