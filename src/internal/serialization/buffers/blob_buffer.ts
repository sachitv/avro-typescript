import { IBuffer } from "./buffer.ts";

/**
 * A read-only buffer implementation that provides random access read operations
 * on data from a Blob. This class is useful for working with binary data
 * from files, network responses, or other Blob sources.
 *
 * Reads are performed asynchronously directly from the Blob.
 *
 * Key features:
 * - Blob-backed: Reads data directly from the Blob asynchronously.
 * - Random access: Supports reading at arbitrary byte offsets.
 * - Bounds checking: Operations that would exceed buffer bounds are safely ignored or return undefined.
 * - Memory efficient: Doesn't load data into memory.
 *
 * @example
 * ```typescript
 * const blob = new Blob([new Uint8Array([1, 2, 3, 4])]);
 * const buffer = new BlobBuffer(blob);
 *
 * // Read some data asynchronously
 * const data = await buffer.read(0, 4); // Returns Uint8Array([1, 2, 3, 4])
 * ```
 */
export class BlobBuffer implements IBuffer {
  #blob: Blob;

  /**
   * Creates a new BlobBuffer from the provided Blob.
   *
   * @param blob The Blob to read data from.
   */
  public constructor(blob: Blob) {
    this.#blob = blob;
  }

  /**
   * Gets the total length of the buffer in bytes.
   *
   * @returns The buffer length in bytes.
   */
  // deno-lint-ignore require-await
  public async length(): Promise<number> {
    return this.#blob.size;
  }

  /**
   * Reads a sequence of bytes from the buffer starting at the specified offset.
   * Reads asynchronously from the Blob.
   *
   * @param offset The byte offset to start reading from (0-based).
   * @param size The number of bytes to read.
   * @returns A Promise that resolves to a new Uint8Array containing the read bytes, or undefined if the read would exceed buffer bounds.
   */
  public async read(
    offset: number,
    size: number,
  ): Promise<Uint8Array | undefined> {
    if (offset + size > this.#blob.size) return undefined;

    // Read directly from the Blob
    const sliced = this.#blob.slice(offset, offset + size);
    const arrayBuffer = await sliced.arrayBuffer();
    return new Uint8Array(arrayBuffer);
  }

  /**
   * Writes a sequence of bytes to the buffer starting at the specified offset.
   * This buffer is read-only, so this method always throws an error.
   *
   * @param offset The byte offset to start writing to (0-based).
   * @param data The bytes to write. The length of this array determines how many bytes are written.
   * @throws Error Always throws an error since this buffer is read-only.
   */
  // deno-lint-ignore require-await
  public async write(_offset: number, _data: Uint8Array): Promise<void> {
    throw new Error("BlobBuffer is read-only");
  }
}
