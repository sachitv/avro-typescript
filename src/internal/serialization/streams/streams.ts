/**
 * Interface describing a readable buffer backed by a ReadableStream.
 * This provides sequential reading capabilities.
 */
export interface IStreamReadableBuffer {
  /**
   * Reads the next chunk of data from the stream.
   * @returns A Promise that resolves to the next Uint8Array chunk, or undefined if the stream is exhausted.
   */
  readNext(): Promise<Uint8Array | undefined>;

  /**
   * Closes the stream and releases the reader lock.
   */
  close(): Promise<void>;
}

/**
 * Interface describing a writable buffer backed by a WritableStream.
 * This provides sequential writing capabilities.
 */
export interface IStreamWritableBuffer {
  /**
   * Writes bytes to the stream.
   */
  writeBytes(data: Uint8Array): Promise<void>;

  /**
   * Closes the stream.
   */
  close(): Promise<void>;
}
