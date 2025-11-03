const encoder = new TextEncoder();
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
