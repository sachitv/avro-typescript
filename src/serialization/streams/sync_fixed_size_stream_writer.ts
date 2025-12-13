import type { ISyncStreamWritableBuffer } from "./sync_streams.ts";

/**
 * Simple synchronous stream writer backed by a fixed-size Uint8Array.
 * Writes append sequentially until the buffer reaches capacity.
 */
export class SyncFixedSizeStreamWriter implements ISyncStreamWritableBuffer {
  #target: Uint8Array;
  #offset = 0;
  #isClosed = false;

  /**
   * Creates a new fixed-size writer.
   *
   * @param target The target buffer to write into.
   */
  public constructor(target: Uint8Array) {
    this.#target = target;
  }

  /**
   * Writes bytes to the underlying target buffer.
   *
   * @param data The bytes to write.
   * @throws RangeError If the write would exceed the buffer capacity.
   * @throws Error If the writer has been closed.
   */
  public writeBytes(data: Uint8Array): void {
    if (this.#isClosed) {
      throw new Error("Cannot write to a closed writer");
    }
    if (data.length === 0) {
      return;
    }
    if (this.#offset + data.length > this.#target.length) {
      throw new RangeError("Write operation exceeds buffer capacity");
    }
    this.#target.set(data, this.#offset);
    this.#offset += data.length;
  }

  /**
   * Closes the writer and prevents further writes.
   */
  public close(): void {
    this.#isClosed = true;
  }

  /**
   * Returns a copy of the bytes written to the underlying target buffer.
   */
  public toUint8Array(): Uint8Array {
    return this.#target.slice(0, this.#offset);
  }

  /**
   * Returns the remaining capacity of the writer.
   */
  public remaining(): number {
    return this.#target.length - this.#offset;
  }
}
