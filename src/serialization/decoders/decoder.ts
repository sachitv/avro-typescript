/**
 * Interface for custom Avro codec decoders.
 */
export interface Decoder {
  /**
   * Decompresses the given data using the codec's algorithm.
   *
   * @param compressedData The compressed data from the Avro file block
   * @returns Promise that resolves to the decompressed data
   */
  decode(compressedData: Uint8Array): Promise<Uint8Array>;
}

/**
 * Registry of codec name to decoder implementation.
 */
export type DecoderRegistry = Record<string, Decoder>;
