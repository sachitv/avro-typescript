/**
 * Text encoding utilities with performance optimizations.
 *
 * Optimizations:
 * - Export encoder for direct encodeInto() access (browser equivalent of utf8Write)
 * - encodeInto() provides zero-copy UTF-8 encoding for modern browsers
 * - Used in writeString() for Unicode string optimization
 */
export const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Encodes a string into a Uint8Array using UTF-8 encoding.
 * @param input The string to encode.
 * @returns A Uint8Array representing the encoded string.
 */
export const encode = (input: string): Uint8Array => encoder.encode(input);

/**
 * Returns the number of bytes needed to encode a string as UTF-8.
 *
 * This is equivalent to `encode(input).length` but avoids allocating a
 * `Uint8Array`.
 */
export function utf8ByteLength(input: string): number {
  let length = 0;

  for (let index = 0; index < input.length; index++) {
    const codeUnit = input.charCodeAt(index);

    if (codeUnit < 0x80) {
      length += 1;
      continue;
    }

    if (codeUnit < 0x800) {
      length += 2;
      continue;
    }

    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const nextCodeUnit = input.charCodeAt(index + 1);
      if (nextCodeUnit >= 0xdc00 && nextCodeUnit <= 0xdfff) {
        // Valid surrogate pair => code point >= 0x10000.
        length += 4;
        index++;
        continue;
      }
      // Unpaired high surrogate; TextEncoder encodes this as U+FFFD.
      length += 3;
      continue;
    }

    if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      // Unpaired low surrogate; TextEncoder encodes this as U+FFFD.
      length += 3;
      continue;
    }

    length += 3;
  }

  return length;
}

/**
 * Decodes a Uint8Array into a string using UTF-8 encoding.
 * @param bytes The Uint8Array to decode.
 * @returns The decoded string.
 */
export const decode = (bytes: Uint8Array): string => decoder.decode(bytes);

/**
 * Threshold for using fromCharCode ASCII fast-path vs TextDecoder.
 *
 * Originally tuned to 32 when the fast path paid a `subarray` allocation per
 * call (see packages/benchmarks/text_encoding_bench.ts). With the range-based
 * decode the fast path is allocation-free and the crossover moves out:
 * measured 2026-07-10 on Deno 2.9.2/arm64, range fromCharCode vs
 * subarray+TextDecoder was 64 vs 87 ns at 40 bytes, 83 vs 86 ns at 48 bytes,
 * and 107 vs 86 ns at 64 bytes.
 */
const FROM_CHAR_CODE_THRESHOLD = 48;

/**
 * Decodes a byte range of a Uint8Array into a string, using an ASCII
 * fast-path for small strings.
 *
 * Taking the range as (start, end) instead of a pre-sliced view lets hot read
 * paths decode directly out of their backing buffer without allocating a
 * `subarray` wrapper per string; the wrapper is only created for the
 * TextDecoder fallback (non-ASCII or large strings).
 *
 * Short ASCII strings (up to FROM_CHAR_CODE_THRESHOLD bytes) decode via
 * String.fromCharCode; longer or non-ASCII strings fall back to TextDecoder,
 * which scales better.
 *
 * @param bytes The Uint8Array containing the encoded string.
 * @param start The inclusive start offset of the encoded string.
 * @param end The exclusive end offset of the encoded string.
 * @returns The decoded string.
 */
export function decodeUtf8Range(
  bytes: Uint8Array,
  start: number,
  end: number,
): string {
  // Clamp the range exactly like `bytes.subarray(start, end)` would. Without
  // this, a malformed length that points past the buffer reads
  // `bytes[i] === undefined`, which passes the ASCII check (`undefined > 127`
  // is false) and `String.fromCharCode(undefined)` fabricates NUL characters
  // instead of decoding only the available bytes.
  if (end > bytes.length) {
    end = bytes.length;
  }
  if (start < 0) {
    start = 0;
  }
  const len = end - start;
  if (len <= 0) {
    return "";
  }

  // For small strings, try the ASCII fast-path
  if (len <= FROM_CHAR_CODE_THRESHOLD) {
    // Check if all bytes are ASCII (< 128)
    let isAscii = true;
    for (let i = start; i < end; i++) {
      if (bytes[i]! > 127) {
        isAscii = false;
        break;
      }
    }

    if (isAscii) {
      // Use String.fromCharCode for ASCII strings
      // Process in chunks of 8 for better performance
      let result = "";
      let i = start;
      const end8 = end - (len % 8);

      // Process 8 bytes at a time
      for (; i < end8; i += 8) {
        result += String.fromCharCode(
          bytes[i]!,
          bytes[i + 1]!,
          bytes[i + 2]!,
          bytes[i + 3]!,
          bytes[i + 4]!,
          bytes[i + 5]!,
          bytes[i + 6]!,
          bytes[i + 7]!,
        );
      }

      // Process the 0-7 remaining bytes with a single fromCharCode call.
      // A per-byte append here would allocate one rope string per byte, which
      // dominates GC pressure on record workloads full of short field names.
      switch (end - i) {
        case 1:
          result += String.fromCharCode(bytes[i]!);
          break;
        case 2:
          result += String.fromCharCode(bytes[i]!, bytes[i + 1]!);
          break;
        case 3:
          result += String.fromCharCode(
            bytes[i]!,
            bytes[i + 1]!,
            bytes[i + 2]!,
          );
          break;
        case 4:
          result += String.fromCharCode(
            bytes[i]!,
            bytes[i + 1]!,
            bytes[i + 2]!,
            bytes[i + 3]!,
          );
          break;
        case 5:
          result += String.fromCharCode(
            bytes[i]!,
            bytes[i + 1]!,
            bytes[i + 2]!,
            bytes[i + 3]!,
            bytes[i + 4]!,
          );
          break;
        case 6:
          result += String.fromCharCode(
            bytes[i]!,
            bytes[i + 1]!,
            bytes[i + 2]!,
            bytes[i + 3]!,
            bytes[i + 4]!,
            bytes[i + 5]!,
          );
          break;
        case 7:
          result += String.fromCharCode(
            bytes[i]!,
            bytes[i + 1]!,
            bytes[i + 2]!,
            bytes[i + 3]!,
            bytes[i + 4]!,
            bytes[i + 5]!,
            bytes[i + 6]!,
          );
          break;
      }

      return result;
    }
  }

  // Fall back to TextDecoder for non-ASCII or large strings
  if (start === 0 && end === bytes.length) {
    return decoder.decode(bytes);
  }
  return decoder.decode(bytes.subarray(start, end));
}

/**
 * Decodes a Uint8Array into a string, using an ASCII fast-path for small strings.
 *
 * Convenience wrapper over {@link decodeUtf8Range} for callers that already
 * hold an exact-sized view.
 *
 * @param bytes The Uint8Array to decode.
 * @returns The decoded string.
 */
export function decodeWithFastPath(bytes: Uint8Array): string {
  return decodeUtf8Range(bytes, 0, bytes.length);
}
