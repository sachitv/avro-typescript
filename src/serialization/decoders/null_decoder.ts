import type { Decoder } from "./decoder.ts";

/**
 * Built-in null decoder (no compression).
 */
export class NullDecoder implements Decoder {
  /**
   * Decodes data by passing it through unchanged.
   * @param compressedData The data to decode.
   * @returns The decoded (uncompressed) data.
   */
  decode(compressedData: Uint8Array): Promise<Uint8Array> {
    return Promise.resolve(compressedData);
  }
}
