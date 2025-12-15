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
 * Decodes a Uint8Array into a string using UTF-8 encoding.
 * @param bytes The Uint8Array to decode.
 * @returns The decoded string.
 */
export const decode = (bytes: Uint8Array): string => decoder.decode(bytes);
