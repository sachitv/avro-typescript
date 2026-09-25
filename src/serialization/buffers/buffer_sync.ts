/**
 * Synchronous interfaces for readable and writable buffers.
 */

export { ReadBufferError, WriteBufferError } from "./buffer_error.ts";

/**
 * Interface describing a random-access readable buffer (synchronous).
 */
export interface ISyncReadable {
  /**
   * Reads a portion of the buffer starting at offset with the given size.
   *
   * Returns a readonly view of the buffer data. Callers must not modify the
   * returned Uint8Array, as it may be a live view into the underlying buffer.
   * TypeScript will enforce this at compile time.
   *
   * @throws ReadBufferError when the requested range is invalid or exceeds the
   * available bounds.
   */
  read(offset: number, size: number): Readonly<Uint8Array>;

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
   *
   * Ownership contract: `data` is only borrowed for the duration of the call.
   * Implementations must not retain a reference to `data` (or its underlying
   * `ArrayBuffer`) after returning; anything they keep, or hand on to a
   * consumer that may keep it, must be a copy. Callers may reuse or mutate
   * `data` as soon as the call returns.
   *
   * @throws WriteBufferError when the buffer cannot accept the requested bytes.
   */
  appendBytes(data: Uint8Array): void;

  /**
   * Appends a slice of bytes to the buffer without requiring callers to create
   * a subarray view.
   *
   * Follows the same ownership contract as `appendBytes`. `SyncWritableTap`
   * passes scratch memory here that is shared by every `SyncWritableTap` in
   * the process, so implementations must copy the bytes before returning and
   * must not write through any `SyncWritableTap` before they have done so.
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
