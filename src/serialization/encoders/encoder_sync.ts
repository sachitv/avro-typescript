/**
 * Interface for custom synchronous Avro codec encoders.
 */
export interface SyncEncoder {
  /**
   * Compresses the data synchronously using the codec's algorithm.
   * @param uncompressedData Raw block data to compress.
   * @returns The compressed result.
   */
  encode(uncompressedData: Uint8Array): Uint8Array;
}

/**
 * Registry of codec names to sync encoder implementations.
 */
export type SyncEncoderRegistry = Record<string, SyncEncoder>;
