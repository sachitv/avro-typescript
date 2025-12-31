/**
 * Synchronous interfaces for readable and writable buffers.
 */

// Re-export tap errors for backward compatibility
export { ReadBufferError, WriteBufferError } from "../tap_errors.ts";

/**
 * Interface describing a random-access readable buffer (synchronous).
 */
export interface ISyncReadable {
  /**
   * Reads a portion of the buffer starting at offset with the given size.
   * Throws a ReadBufferError when the requested range exceeds the bounds of
   * the buffer.
   */
  read(offset: number, size: number): Uint8Array;

  /**
   * Checks if more data can be read starting at the given offset.
   */
  canReadMore(offset: number): boolean;
}

/**
 * Interface describing an append-only writable buffer (synchronous).
 */
export interface ISyncWritable {
  /**
   * Appends bytes to the buffer, advancing its internal write cursor when the
   * operation succeeds.
   */
  appendBytes(data: Uint8Array): void;

  /**
   * Appends a slice of bytes to the buffer without requiring callers to create
   * a subarray view.
   */
  appendBytesFrom(data: Uint8Array, offset: number, length: number): void;

  /**
   * Returns whether the buffer can continue accepting writes. Implementations
   * should flip this to `false` after a write would exceed capacity so callers
   * can detect the overflow condition.
   */
  isValid(): boolean;

  /**
   * Checks if the buffer can accept appending the given number of bytes.
   */
  canAppendMore(size: number): boolean;
}

/**
 * Convenience type for buffers capable of both read and write operations.
 */
export type ISyncReadableAndWritable = ISyncReadable & ISyncWritable;
