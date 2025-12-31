import type { SyncDecoder } from "./decoder_sync.ts";

/**
 * Built-in sync null decoder that returns data unchanged.
 */
export class NullDecoderSync implements SyncDecoder {
  /**
   * Decodes data by returning the same buffer.
   * @param compressedData The input data.
   * @returns The same data buffer.
   */
  decode(compressedData: Uint8Array): Uint8Array {
    return compressedData;
  }
}
