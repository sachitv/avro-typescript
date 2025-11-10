import { type IReadableBuffer, type IWritableBuffer } from "./buffer.ts";

/**
 * Shared strict in-memory buffer base with common functionality.
 */
abstract class StrictInMemoryBufferBase {
  protected readonly view: Uint8Array;

  constructor(buf: ArrayBuffer) {
    this.view = new Uint8Array(buf);
  }

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
export class StrictInMemoryReadableBuffer extends StrictInMemoryBufferBase
  implements IReadableBuffer {
  [key: string]: unknown;

  private checkBounds(offset: number, size: number): void {
    if (offset < 0 || size < 0) {
      throw new RangeError(
        `Offset and size must be non-negative. Got offset=${offset}, size=${size}`,
      );
    }
    if (offset + size > this.view.length) {
      throw new RangeError(
        `Operation exceeds buffer bounds. offset=${offset}, size=${size}, bufferLength=${this.view.length}`,
      );
    }
  }

  // deno-lint-ignore require-await
  public async read(
    offset: number,
    size: number,
  ): Promise<Uint8Array> {
    this.checkBounds(offset, size);
    return this.view.slice(offset, offset + size);
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
export class StrictInMemoryWritableBuffer extends StrictInMemoryBufferBase
  implements IWritableBuffer {
  #offset: number;

  constructor(buf: ArrayBuffer, offset = 0) {
    super(buf);
    if (offset < 0 || offset > this.view.length) {
      throw new RangeError(
        `Initial offset must be within buffer bounds. Got offset=${offset}, bufferLength=${this.view.length}`,
      );
    }
    this.#offset = offset;
  }

  protected checkWriteBounds(offset: number, data: Uint8Array): void {
    if (offset < 0) {
      throw new RangeError(`Offset must be non-negative. Got offset=${offset}`);
    }
    if (offset + data.length > this.view.length) {
      throw new RangeError(
        `Write operation exceeds buffer bounds. offset=${offset}, dataSize=${data.length}, bufferLength=${this.view.length}`,
      );
    }
  }

  // deno-lint-ignore require-await
  public async appendBytes(data: Uint8Array): Promise<void> {
    this.checkWriteBounds(this.#offset, data);
    if (data.length === 0) {
      return;
    }
    this.view.set(data, this.#offset);
    this.#offset += data.length;
  }

  // deno-lint-ignore require-await
  public async isValid(): Promise<boolean> {
    return true; // Always valid since we throw on overflow instead of marking as invalid
  }

  /**
   * Gets the current write offset position.
   */
  public get offset(): number {
    return this.#offset;
  }

  /**
   * Gets the remaining space in the buffer.
   */
  public get remaining(): number {
    return this.view.length - this.#offset;
  }
}
