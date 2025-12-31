import type { ISyncWritable } from "../buffers/buffer_sync.ts";
import type { ISyncStreamWritableBuffer } from "./streams_sync.ts";

/**
 * Adapter that wraps an ISyncStreamWritableBuffer to provide the ISyncWritable interface.
 * Writes are passed through to the underlying stream sink.
 */
export class SyncStreamWritableBufferAdapter implements ISyncWritable {
  #streamBuffer: ISyncStreamWritableBuffer;
  #isClosed = false;

  /**
   * Creates a new adapter from a synchronous stream writable buffer.
   *
   * @param streamBuffer The stream buffer to adapt.
   */
  public constructor(streamBuffer: ISyncStreamWritableBuffer) {
    this.#streamBuffer = streamBuffer;
  }

  /**
   * Appends bytes to the buffer, advancing its internal write cursor when the
   * operation succeeds.
   */
  public appendBytes(data: Uint8Array): void {
    if (this.#isClosed || data.length === 0) {
      return;
    }
    this.#streamBuffer.writeBytes(data);
  }

  /**
   * Appends a slice of bytes to the buffer.
   *
   * @param data The source bytes.
   * @param offset The starting offset in data.
   * @param length The number of bytes to write.
   */
  public appendBytesFrom(
    data: Uint8Array,
    offset: number,
    length: number,
  ): void {
    if (this.#isClosed || length === 0) {
      return;
    }
    this.#streamBuffer.writeBytesFrom(data, offset, length);
  }

  /**
   * Returns whether the buffer can continue accepting writes.
   * Stream buffers can always accept writes until explicitly closed.
   */
  public isValid(): boolean {
    return !this.#isClosed;
  }

  /**
   * Checks if the buffer can accept appending the given number of bytes.
   *
   * @param size The number of bytes to check.
   * @returns True if the buffer can accept the append.
   */
  public canAppendMore(_size: number): boolean {
    return this.isValid();
  }

  /**
   * Closes the underlying stream buffer.
   * After calling this, further writes will be ignored.
   */
  public close(): void {
    if (!this.#isClosed) {
      this.#isClosed = true;
      this.#streamBuffer.close();
    }
  }
}
