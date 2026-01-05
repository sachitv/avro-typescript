import { type ISyncReadable, ReadBufferError } from "./buffer_sync.ts";

/**
 * A read-only buffer implementation that provides random access read operations
 * on data from a Uint8Array. This class is useful for working with binary data
 * that is already loaded in memory.
 *
 * Reads are performed synchronously from the Uint8Array.
 *
 * Key features:
 * - Uint8Array-backed: Reads data directly from the Uint8Array synchronously.
 * - Random access: Supports reading at arbitrary byte offsets.
 * - Bounds checking: Operations that would exceed buffer bounds throw
 *   ReadBufferError.
 * - Memory efficient: Works with existing Uint8Array data.
 *
 * @example
 * ```typescript
 * const data = new Uint8Array([1, 2, 3, 4]);
 * const buffer = new SyncBlobReadableBuffer(data);
 *
 * // Read some data synchronously
 * const result = buffer.read(0, 4); // Returns Uint8Array([1, 2, 3, 4])
 * ```
 */
export class SyncBlobReadableBuffer implements ISyncReadable {
  #data: Uint8Array;

  /**
   * Creates a new SyncBlobReadableBuffer from the provided Uint8Array.
   *
   * @param data The Uint8Array to read data from.
   */
  public constructor(data: Uint8Array) {
    this.#data = data;
  }

  /**
   * Gets the total length of the buffer in bytes.
   *
   * @returns The buffer length in bytes.
   */
  public length(): number {
    return this.#data.length;
  }

  /**
   * Reads a sequence of bytes from the buffer starting at the specified offset.
   * Reads synchronously from the Uint8Array.
   *
   * @param offset The byte offset to start reading from (0-based).
   * @param size The number of bytes to read.
   * @returns A readonly Uint8Array containing the read bytes.
   * @throws ReadBufferError if the requested range is out of bounds.
   */
  public read(offset: number, size: number): Readonly<Uint8Array> {
    if (offset < 0 || size < 0) {
      throw new ReadBufferError(
        `Offset and size must be non-negative. offset=${offset}, size=${size}`,
        offset,
        size,
        this.#data.length,
      );
    }

    if (offset + size > this.#data.length) {
      throw new ReadBufferError(
        `Read operation exceeds buffer bounds. offset=${offset}, size=${size}, bufferLength=${this.#data.length}`,
        offset,
        size,
        this.#data.length,
      );
    }

    return this.#data.slice(offset, offset + size);
  }

  /**
   * Checks if more data can be read starting at the given offset.
   * @param offset The byte offset to check.
   * @returns True if at least one byte can be read from the offset.
   */
  public canReadMore(offset: number): boolean {
    try {
      this.read(offset, 1);
      return true;
    } catch (err) {
      if (err instanceof ReadBufferError) {
        return false;
      }
      throw err;
    }
  }
}
