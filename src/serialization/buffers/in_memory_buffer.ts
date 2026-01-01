import type { IReadableBuffer, IWritableBuffer } from "./buffer.ts";
import { ReadBufferError, WriteBufferError } from "./buffer_error.ts";

/**
 * Shared strict in-memory buffer base with common functionality.
 */
export abstract class InMemoryBufferBase {
  /** The underlying Uint8Array view of the buffer. */
  protected readonly view: Uint8Array;

  /**
   * Constructs an InMemoryBufferBase with the given ArrayBuffer.
   * @param buf The ArrayBuffer to create the view from.
   */
  constructor(buf: ArrayBuffer) {
    this.view = new Uint8Array(buf);
  }

  /**
   * Returns the length of the buffer in bytes.
   * @returns The length of the buffer.
   */
  // deno-lint-ignore require-await
  public async length(): Promise<number> {
    return this.view.length;
  }
}

/**
 * Strict read-only in-memory buffer for serialization reads.
 * Throws errors on all out-of-bounds operations.
 *
 * Key features:
 * - Fixed size: The buffer size is determined at construction and cannot be resized.
 * - Random access: Supports reading at arbitrary byte offsets.
 * - Strict bounds checking: Operations that exceed buffer bounds throw RangeError.
 * - Efficient: Uses Uint8Array for fast byte-level operations.
 *
 * @example
 * ```typescript
 * const arrayBuffer = new ArrayBuffer(1024);
 * const buffer = new StrictInMemoryReadableBuffer(arrayBuffer);
 *
 * // Read some data
 * const data = await buffer.read(0, 4); // Returns Uint8Array of 4 bytes
 *
 * // This will throw:
 * await buffer.read(1020, 10); // RangeError: Operation exceeds buffer bounds
 * ```
 */
export class InMemoryReadableBuffer extends InMemoryBufferBase
  implements IReadableBuffer {
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
  // deno-lint-ignore require-await
  public async read(
    offset: number,
    size: number,
  ): Promise<Uint8Array> {
    this.checkBounds(offset, size);
    return this.view.slice(offset, offset + size);
  }

  /**
   * Checks if more data can be read starting at the given offset.
   * @param offset The byte offset to check.
   * @returns True if at least one byte can be read from the offset.
   */
  // deno-lint-ignore require-await
  public async canReadMore(offset: number): Promise<boolean> {
    try {
      this.checkBounds(offset, 1);
      return true;
    } catch {
      return false;
    }
  }
}

/**
 * Strict write-only in-memory buffer for serialization writes.
 * Throws errors when attempting to write beyond buffer bounds.
 *
 * Key features:
 * - Fixed size: The buffer size is determined at construction and cannot be resized.
 * - Sequential writes: Maintains an internal offset that advances with each write.
 * - Strict bounds checking: Throws RangeError when write would exceed buffer capacity.
 * - Efficient: Uses Uint8Array for fast byte-level operations.
 *
 * @example
 * ```typescript
 * const arrayBuffer = new ArrayBuffer(1024);
 * const buffer = new StrictInMemoryWritableBuffer(arrayBuffer);
 *
 * // Write some data
 * await buffer.appendBytes(new Uint8Array([1, 2, 3, 4]));
 *
 * // This will throw if buffer is full:
 * await buffer.appendBytes(new Uint8Array(2000)); // RangeError: Write operation exceeds buffer bounds
 * ```
 */
export class InMemoryWritableBuffer extends InMemoryBufferBase
  implements IWritableBuffer {
  #offset: number;
  #initialOffset: number;

  /**
   * Creates a new writable buffer with the specified ArrayBuffer and initial offset.
   * @param buf The underlying ArrayBuffer to write to.
   * @param offset The starting offset within the buffer (default: 0).
   * @throws WriteBufferError If the initial offset is invalid.
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
   * Throws a RangeError if the operation would exceed bounds.
   * @param offset The offset to write at.
   * @param data The data to write.
   */
  protected checkWriteBounds(offset: number, data: Uint8Array): void {
    if (offset < 0) {
      throw new WriteBufferError(
        `Offset must be non-negative. Got offset=${offset}`,
        offset,
        data.length,
        this.view.length,
      );
    }
    if (offset + data.length > this.view.length) {
      throw new WriteBufferError(
        `Write operation exceeds buffer bounds. offset=${offset}, dataSize=${data.length}, bufferLength=${this.view.length}`,
        offset,
        data.length,
        this.view.length,
      );
    }
  }

  /**
   * Appends the given bytes to the buffer at the current offset and advances the offset.
   * @param data The bytes to append.
   */
  // deno-lint-ignore require-await
  public async appendBytes(data: Uint8Array): Promise<void> {
    this.checkWriteBounds(this.#offset, data);
    if (data.length === 0) {
      return;
    }
    this.view.set(data, this.#offset);
    this.#offset += data.length;
  }

  /**
   * Checks if the buffer is in a valid state.
   * Always returns true as this buffer throws on overflow instead of becoming invalid.
   * @returns Always true.
   */
  // deno-lint-ignore require-await
  public async isValid(): Promise<boolean> {
    return true; // Always valid since we throw on overflow instead of marking as invalid
  }

  /**
   * Checks if the buffer can accept appending the given number of bytes.
   * @param size The number of bytes to check.
   * @returns True if the buffer can accept the append.
   */
  public async canAppendMore(_size: number): Promise<boolean> {
    return await this.isValid();
  }

  /**
   * @internal Test-only function to get the current write offset position.
   */
  public _testOnlyOffset(): number {
    return this.#offset;
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
}
