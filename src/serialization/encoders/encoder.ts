/**
 * Interface for custom Avro codec encoders.
 */
export interface Encoder {
  /**
   * Compresses the given data using the codec's algorithm.
   *
   * @param uncompressedData The raw block data that should be compressed
   * @returns Promise that resolves to the compressed data
   */
  encode(uncompressedData: Uint8Array): Promise<Uint8Array>;
}

/**
 * Registry of codec name to encoder implementation.
 */
export type EncoderRegistry = Record<string, Encoder>;
