import type { SyncDecoder } from "./sync_decoder.ts";

/**
 * Built-in sync null decoder that returns data unchanged.
 */
export class NullSyncDecoder implements SyncDecoder {
  /**
   * Decodes data by returning the same buffer.
   * @param compressedData The input data.
   * @returns The same data buffer.
   */
  decode(compressedData: Uint8Array): Uint8Array {
    return compressedData;
  }
}
