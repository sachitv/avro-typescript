import type { ISyncReadable, ISyncWritable } from "./buffer_sync.ts";
import { ReadBufferError, WriteBufferError } from "./buffer_sync.ts";

/**
 * Shared strict in-memory buffer base with common functionality.
 */
export abstract class SyncInMemoryBufferBase {
  /** The underlying Uint8Array view of the buffer. */
  protected readonly view: Uint8Array;

  /**
   * Constructs a SyncInMemoryBufferBase with the given ArrayBuffer.
   * @param buf The ArrayBuffer to create the view from.
   */
  constructor(buf: ArrayBuffer) {
    this.view = new Uint8Array(buf);
  }

  /**
   * Returns the length of the buffer in bytes.
   * @returns The length of the buffer.
   */
  public length(): number {
    return this.view.length;
  }

  /**
   * @internal Test-only function to get the underlying ArrayBuffer.
   */
  public _testOnlyBuffer(): ArrayBuffer {
    return this.view.buffer as ArrayBuffer;
  }
}

/**
 * Strict read-only in-memory buffer for serialization reads (synchronous).
 * Throws errors on all out-of-bounds operations.
 *
 * Key features:
 * - Fixed size: The buffer size is determined at construction and cannot be resized.
 * - Random access: Supports reading at arbitrary byte offsets.
 * - Strict bounds checking: Operations that exceed buffer bounds throw ReadBufferError.
 * - Efficient: Uses Uint8Array for fast byte-level operations.
 *
 * @example
 * ```typescript
 * const arrayBuffer = new ArrayBuffer(1024);
 * const buffer = new SyncInMemoryReadableBuffer(arrayBuffer);
 *
 * // Read some data
 * const data = buffer.read(0, 4); // Returns Uint8Array of 4 bytes
 *
 * // This will throw:
 * buffer.read(1020, 10); // ReadBufferError: Operation exceeds buffer bounds
 * ```
 */
export class SyncInMemoryReadableBuffer extends SyncInMemoryBufferBase
  implements ISyncReadable {
  /**
   * Checks if the offset and size are within buffer bounds.
   * @param offset The starting offset.
   * @param size The number of bytes to check.
   */
  private checkBounds(offset: number, size: number): void {
    if (offset < 0 || size < 0) {
      throw new ReadBufferError(
        `Offset and size must be non-negative. Got offset=${offset}, size=${size}`,
        offset,
        size,
        this.view.length,
      );
    }
    if (offset + size > this.view.length) {
      throw new ReadBufferError(
        `Operation exceeds buffer bounds. offset=${offset}, size=${size}, bufferLength=${this.view.length}`,
        offset,
        size,
        this.view.length,
      );
    }
  }

  /**
   * Reads a portion of the buffer.
   * @param offset The starting offset.
   * @param size The number of bytes to read.
   * @returns A Uint8Array containing the read bytes.
   */
  public read(
    offset: number,
    size: number,
  ): Uint8Array {
    this.checkBounds(offset, size);
    return this.view.slice(offset, offset + size);
  }

  /**
   * Checks if more data can be read starting at the given offset.
   * @param offset The byte offset to check.
   * @returns True if at least one byte can be read from the offset.
   */
  public canReadMore(offset: number): boolean {
    try {
      this.checkBounds(offset, 1);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * @internal Test-only function to get the underlying ArrayBuffer.
   */
  public override _testOnlyBuffer(): ArrayBuffer {
    return this.view.buffer as ArrayBuffer;
  }
}

/**
 * Strict write-only in-memory buffer for serialization writes (synchronous).
 * Throws errors when attempting to write beyond buffer bounds.
 *
 * Key features:
 * - Fixed size: The buffer size is determined at construction and cannot be resized.
 * - Sequential writes: Maintains an internal offset that advances with each write.
 * - Strict bounds checking: Throws WriteBufferError when write would exceed buffer capacity.
 * - Efficient: Uses Uint8Array for fast byte-level operations.
 *
 * @example
 * ```typescript
 * const arrayBuffer = new ArrayBuffer(1024);
 * const buffer = new SyncInMemoryWritableBuffer(arrayBuffer);
 *
 * // Write some data
 * buffer.appendBytes(new Uint8Array([1, 2, 3, 4]));
 *
 * // This will throw if buffer is full:
 * buffer.appendBytes(new Uint8Array(2000)); // WriteBufferError: Write operation exceeds buffer bounds
 * ```
 */
export class SyncInMemoryWritableBuffer extends SyncInMemoryBufferBase
  implements ISyncWritable {
  #offset: number;
  #initialOffset: number;

  /**
   * Creates a new writable buffer with the specified ArrayBuffer and initial offset.
   * @param buf The underlying ArrayBuffer to write to.
   * @param offset The starting offset within the buffer (default: 0).
   */
  constructor(buf: ArrayBuffer, offset = 0) {
    super(buf);
    if (offset < 0 || offset > this.view.length) {
      throw new WriteBufferError(
        `Initial offset must be within buffer bounds. Got offset=${offset}, bufferLength=${this.view.length}`,
        offset,
        0,
        this.view.length,
      );
    }
    this.#initialOffset = offset;
    this.#offset = offset;
  }

  /**
   * Checks if writing the given data at the specified offset would exceed buffer bounds.
   * Throws a WriteBufferError if the operation would exceed bounds.
   * @param offset The offset to write at.
   * @param data The data to write.
   */
  protected checkWriteBounds(offset: number, data: Uint8Array): void {
    this.checkWriteBoundsSize(offset, data.length);
  }

  /**
   * Checks if writing the given number of bytes at the specified offset would exceed buffer bounds.
   * Throws a WriteBufferError if the operation would exceed bounds.
   * @param offset The offset to write at.
   * @param size The number of bytes to write.
   */
  protected checkWriteBoundsSize(offset: number, size: number): void {
    if (offset < 0) {
      throw new WriteBufferError(
        `Offset must be non-negative. Got offset=${offset}`,
        offset,
        size,
        this.view.length,
      );
    }
    if (offset + size > this.view.length) {
      throw new WriteBufferError(
        `Write operation exceeds buffer bounds. offset=${offset}, dataSize=${size}, bufferLength=${this.view.length}`,
        offset,
        size,
        this.view.length,
      );
    }
  }

  /**
   * Appends the given bytes to the buffer at the current offset and advances the offset.
   * @param data The bytes to append.
   */
  public appendBytes(data: Uint8Array): void {
    this.checkWriteBounds(this.#offset, data);
    if (data.length === 0) {
      return;
    }
    this.view.set(data, this.#offset);
    this.#offset += data.length;
  }

  /**
   * Appends a slice of bytes to the buffer at the current offset and advances the offset.
   * This avoids allocating subarray views in higher-level hot paths.
   */
  public appendBytesFrom(
    data: Uint8Array,
    offset: number,
    length: number,
  ): void {
    if (length === 0) {
      return;
    }
    if (
      !Number.isInteger(offset) || !Number.isInteger(length) ||
      offset < 0 || length < 0 || offset + length > data.length
    ) {
      throw new RangeError(
        `Invalid source range offset=${offset} length=${length} for dataSize=${data.length}`,
      );
    }
    this.checkWriteBoundsSize(this.#offset, length);
    // Use TypedArray.set() for bulk copy instead of byte-by-byte loop
    this.view.set(data.subarray(offset, offset + length), this.#offset);
    this.#offset += length;
  }

  /**
   * Checks if the buffer is in a valid state.
   * Always returns true as this buffer throws on overflow instead of becoming invalid.
   * @returns Always true.
   */
  public isValid(): boolean {
    return true; // Always valid since we throw on overflow instead of marking as invalid
  }

  /**
   * Checks if the buffer can accept appending the given number of bytes.
   * @param size The number of bytes to check.
   * @returns True if the buffer can accept the append.
   */
  public canAppendMore(_size: number): boolean {
    return this.isValid();
  }

  /**
   * @internal Test-only function to get the current write offset position.
   */
  public _testOnlyOffset(): number {
    return this.#offset;
  }

  /**
   * @internal Test-only function to get the underlying ArrayBuffer.
   */
  public override _testOnlyBuffer(): ArrayBuffer {
    return this.view.buffer as ArrayBuffer;
  }

  /**
   * @internal Test-only function to get the remaining space in the buffer.
   */
  public _testOnlyRemaining(): number {
    return this.view.length - this.#offset;
  }

  /**
   * Gets a copy of the buffer containing only the bytes written to the buffer.
   * @returns An ArrayBuffer containing only the written data.
   */
  public getBufferCopy(): ArrayBuffer {
    const sliced = this.view.slice(this.#initialOffset, this.#offset);
    return sliced.buffer.slice(
      sliced.byteOffset,
      sliced.byteOffset + sliced.length,
    );
  }

  /**
   * @internal Test-only method to check write bounds with arbitrary offset.
   */
  public _testCheckWriteBounds(offset: number, data: Uint8Array): void {
    this.checkWriteBounds(offset, data);
  }

  /**
   * @internal Test-only method to check write bounds with arbitrary offset and size.
   */
  public _testCheckWriteBoundsSize(offset: number, size: number): void {
    this.checkWriteBoundsSize(offset, size);
  }
}
