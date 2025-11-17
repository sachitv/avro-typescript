/**
 * Interface describing a random-access readable buffer.
 */
export interface IReadableBuffer {
  read(offset: number, size: number): Promise<Uint8Array | undefined>;
}

/**
 * Interface describing an append-only writable buffer.
 */
export interface IWritableBuffer {
  /**
   * Appends bytes to the buffer, advancing its internal write cursor when the
   * operation succeeds.
   */
  appendBytes(data: Uint8Array): Promise<void>;

  /**
   * Returns whether the buffer can continue accepting writes. Implementations
   * should flip this to `false` after a write would exceed capacity so callers
   * can detect the overflow condition.
   */
  isValid(): Promise<boolean>;
}

/**
 * Convenience type for buffers capable of both read and write operations.
 */
export type IReadableAndWritableBuffer = IReadableBuffer & IWritableBuffer;
