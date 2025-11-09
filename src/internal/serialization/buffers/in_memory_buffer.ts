import { type IReadableBuffer, type IWritableBuffer } from "./buffer.ts";

/**
 * Shared in-memory buffer base that exposes the core ArrayBuffer mechanics.
 */
abstract class InMemoryBufferBase {
  protected readonly view: Uint8Array;

  constructor(buf: ArrayBuffer) {
    this.view = new Uint8Array(buf);
  }

  // deno-lint-ignore require-await
  public async length(): Promise<number> {
    return this.view.length;
  }

  protected withinBounds(offset: number, size: number): boolean {
    return offset >= 0 && size >= 0 && offset + size <= this.view.length;
  }

  protected clampOffset(offset: number): number {
    if (Number.isNaN(offset) || !Number.isFinite(offset)) {
      return 0;
    }
    if (offset < 0) {
      return 0;
    }
    if (offset > this.view.length) {
      return this.view.length;
    }
    return offset;
  }
}

/**
 * Read-only in-memory buffer for serialization reads.
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
 * const buffer = new InMemoryReadableBuffer(arrayBuffer);
 *
 * // Write some data
 * const data = await buffer.read(0, 4); // Returns Uint8Array([1, 2, 3, 4])
 * ```
 */
export class InMemoryReadableBuffer extends InMemoryBufferBase
  implements IReadableBuffer {
  // deno-lint-ignore require-await
  public async read(
    offset: number,
    size: number,
  ): Promise<Uint8Array | undefined> {
    if (!this.withinBounds(offset, size)) {
      return undefined;
    }
    return this.view.slice(offset, offset + size);
  }
}

/**
 * Write-only in-memory buffer for serialization writes.
 */
export class InMemoryWritableBuffer extends InMemoryBufferBase
  implements IWritableBuffer {
  #offset: number;
  #overflowed = false;

  constructor(buf: ArrayBuffer, offset = 0) {
    super(buf);
    this.#offset = this.clampOffset(offset);
  }

  // deno-lint-ignore require-await
  public async appendBytes(data: Uint8Array): Promise<void> {
    if (data.length === 0 || this.#overflowed) {
      return;
    }
    const remaining = this.view.length - this.#offset;
    if (data.length > remaining) {
      this.#overflowed = true;
      return;
    }
    this.view.set(data, this.#offset);
    this.#offset += data.length;
  }

  // deno-lint-ignore require-await
  public async isValid(): Promise<boolean> {
    return !this.#overflowed;
  }
}
