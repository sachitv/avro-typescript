/**
 * Interfaces describing synchronous stream-backed readable and writable buffers.
 */

/**
 * Interface describing a readable buffer backed by a synchronous stream-like source.
 * This provides sequential access to byte chunks without relying on async primitives.
 */
export interface ISyncStreamReadableBuffer {
  /**
   * Reads the next chunk of data from the stream.
   * Returns undefined when the stream is exhausted.
   */
  readNext(): Uint8Array | undefined;

  /**
   * Closes the stream and releases any held resources.
   */
  close(): void;
}

/**
 * Interface describing a writable buffer backed by a synchronous stream-like sink.
 * This provides sequential writing capabilities.
 */
export interface ISyncStreamWritableBuffer {
  /**
   * Writes bytes to the stream.
   */
  writeBytes(data: Uint8Array): void;

  /**
   * Writes a slice of bytes to the stream without requiring a subarray view.
   */
  writeBytesFrom(data: Uint8Array, offset: number, length: number): void;

  /**
   * Closes the stream and releases any held resources.
   */
  close(): void;
}
