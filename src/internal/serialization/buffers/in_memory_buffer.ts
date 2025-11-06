import { IBuffer } from "./buffer.ts";

/**
 * An in-memory buffer implementation that provides random access read and write operations
 * on a fixed-size ArrayBuffer. This class wraps an ArrayBuffer and provides the IBuffer
 * interface for use with serialization utilities like Tap.
 *
 * Key features:
 * - Fixed size: The buffer size is determined at construction and cannot be resized.
 * - Random access: Supports reading and writing at arbitrary byte offsets.
 * - Bounds checking: Operations that would exceed buffer bounds are safely ignored or return undefined.
 * - Efficient: Uses Uint8Array for fast byte-level operations.
 *
 * @example
 * ```typescript
 * const arrayBuffer = new ArrayBuffer(1024);
 * const buffer = new InMemoryBuffer(arrayBuffer);
 *
 * // Write some data
 * await buffer.write(0, new Uint8Array([1, 2, 3, 4]));
 *
 * // Read it back
 * const data = await buffer.read(0, 4); // Returns Uint8Array([1, 2, 3, 4])
 * ```
 */
export class InMemoryBuffer implements IBuffer {
  #buf: Uint8Array;

  /**
   * Creates a new InMemoryBuffer backed by the provided ArrayBuffer.
   *
   * @param buf The ArrayBuffer to wrap. The buffer's size determines the capacity.
   */
  constructor(buf: ArrayBuffer) {
    this.#buf = new Uint8Array(buf);
  }

  /**
   * Gets the total length of the buffer in bytes.
   *
   * @returns The buffer length in bytes.
   */
  // deno-lint-ignore require-await
  public async length(): Promise<number> {
    return this.#buf.length;
  }

  /**
   * Reads a sequence of bytes from the buffer starting at the specified offset.
   *
   * @param offset The byte offset to start reading from (0-based).
   * @param size The number of bytes to read.
   * @returns A Promise that resolves to a new Uint8Array containing the read bytes, or undefined if the read would exceed buffer bounds.
   */
  // deno-lint-ignore require-await
  public async read(
    offset: number,
    size: number,
  ): Promise<Uint8Array | undefined> {
    if (offset + size > this.#buf.length) {
      return undefined;
    }
    return this.#buf.slice(offset, offset + size);
  }

  /**
   * Writes a sequence of bytes to the buffer starting at the specified offset.
   *
   * @param offset The byte offset to start writing to (0-based).
   * @param data The bytes to write. The length of this array determines how many bytes are written.
   * @returns A Promise that resolves when the write is complete. If the write would exceed buffer bounds, no data is written.
   */
  // deno-lint-ignore require-await
  public async write(offset: number, data: Uint8Array): Promise<void> {
    if (offset + data.length > this.#buf.length) {
      return Promise.resolve();
    }
    this.#buf.set(data, offset);
    return Promise.resolve();
  }
}
