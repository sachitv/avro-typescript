import type { Encoder } from "./encoder.ts";

/**
 * Built-in null encoder (no compression).
 */
export class NullEncoder implements Encoder {
  /**
   * Encodes data by passing it through unchanged.
   * @param uncompressedData The data to encode.
   * @returns The encoded (uncompressed) data.
   */
  encode(uncompressedData: Uint8Array): Promise<Uint8Array> {
    return Promise.resolve(uncompressedData);
  }
}
