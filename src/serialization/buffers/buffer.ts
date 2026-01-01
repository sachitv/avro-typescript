/**
 * Interface describing a random-access readable buffer.
 */

export interface IReadableBuffer {
  /**
   * Reads a portion of the buffer starting at offset with the given size.
   *
   * @throws ReadBufferError when the requested range is invalid or exceeds the
   * available bounds.
   */
  read(offset: number, size: number): Promise<Uint8Array>;

  /**
   * Checks if more data can be read starting at the given offset.
   */
  canReadMore(offset: number): Promise<boolean>;
}

/**
 * Interface describing an append-only writable buffer.
 */

export interface IWritableBuffer {
  /**
   * Appends bytes to the buffer, advancing its internal write cursor when the
   * operation succeeds.
   *
   * @throws WriteBufferError when the buffer cannot accept the requested bytes.
   */
  appendBytes(data: Uint8Array): Promise<void>;

  /**
   * Returns whether the buffer can continue accepting writes. Implementations
   * should flip this to `false` after a write would exceed capacity so callers
   * can detect the overflow condition.
   */
  isValid(): Promise<boolean>;

  /**
   * Checks if the buffer can accept appending the given number of bytes.
   */
  canAppendMore(size: number): Promise<boolean>;
}

/**
 * Convenience type for buffers capable of both read and write operations.
 */
export type IReadableAndWritableBuffer = IReadableBuffer & IWritableBuffer;
