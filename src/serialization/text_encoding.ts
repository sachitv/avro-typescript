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
