/**
 * Interface for custom synchronous Avro codec decoders.
 */
export interface SyncDecoder {
  /**
   * Decompresses the compressed block synchronously.
   * @param compressedData The compressed block data.
   * @returns The decompressed bytes.
   */
  decode(compressedData: Uint8Array): Uint8Array;
}

/**
 * Registry mapping codec names to synchronous decoder implementations.
 */
export type SyncDecoderRegistry = Record<string, SyncDecoder>;
