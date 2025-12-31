import type { SyncEncoder } from "./encoder_sync.ts";

/**
 * Built-in sync null encoder (no compression).
 */
export class NullEncoderSync implements SyncEncoder {
  /**
   * Encodes data by returning the buffer unchanged.
   * @param uncompressedData The data to encode.
   * @returns The same data buffer.
   */
  encode(uncompressedData: Uint8Array): Uint8Array {
    return uncompressedData;
  }
}
