import { type ISyncReadable, ReadBufferError } from "../buffers/buffer_sync.ts";
import type { ISyncStreamReadableBuffer } from "./streams_sync.ts";

/**
 * Adapter that wraps an ISyncStreamReadableBuffer to provide the ISyncReadable interface.
 * The adapter buffers stream content progressively to enable random-access reads.
 */
export class SyncStreamReadableBufferAdapter implements ISyncReadable {
  #streamBuffer: ISyncStreamReadableBuffer;
  #bufferedData: Uint8Array = new Uint8Array(0);
  #eof = false;

  /**
   * Creates a new adapter from a synchronous stream readable buffer.
   *
   * @param streamBuffer The stream buffer to adapt.
   */
  public constructor(streamBuffer: ISyncStreamReadableBuffer) {
    this.#streamBuffer = streamBuffer;
  }

  /**
   * Reads a sequence of bytes from the buffer starting at the specified offset.
   * This buffers stream data as needed to satisfy the read request.
   *
   * @param offset The byte offset to start reading from (0-based).
   * @param size The number of bytes to read.
   * @returns A Uint8Array containing the requested bytes.
   * @throws ReadBufferError If the requested range is invalid or exceeds available data.
   */
  public read(offset: number, size: number): Uint8Array {
    this.#validateParameters(offset, size);
    this.#ensureBufferedUpTo(offset + size);

    if (offset + size > this.#bufferedData.length) {
      throw new ReadBufferError(
        `Requested range exceeds buffered data. offset=${offset}, size=${size}, bufferLength=${this.#bufferedData.length}`,
        offset,
        size,
        this.#bufferedData.length,
      );
    }

    return this.#bufferedData.slice(offset, offset + size);
  }

  /**
   * Checks if more data can be read starting at the given offset.
   *
   * @param offset The byte offset to check.
   * @returns True if at least one byte can be read from the offset.
   */
  public canReadMore(offset: number): boolean {
    try {
      this.read(offset, 1);
      return true;
    } catch (error) {
      if (error instanceof ReadBufferError) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Closes the underlying stream buffer.
   */
  public close(): void {
    this.#streamBuffer.close();
  }

  /**
   * Validates read parameters.
   */
  #validateParameters(offset: number, size: number): void {
    if (offset < 0 || size < 0) {
      throw new ReadBufferError(
        `Offset and size must be non-negative. Got offset=${offset}, size=${size}`,
        offset,
        size,
        this.#bufferedData.length,
      );
    }
  }

  /**
   * Ensures the buffer contains data up to the requested offset.
   */
  #ensureBufferedUpTo(targetOffset: number): void {
    while (!this.#eof && this.#bufferedData.length < targetOffset) {
      const chunk = this.#streamBuffer.readNext();
      if (chunk === undefined) {
        this.#eof = true;
        break;
      }
      if (chunk.length === 0) {
        continue;
      }
      const currentLength = this.#bufferedData.length;
      const newBuffer = new Uint8Array(currentLength + chunk.length);
      newBuffer.set(this.#bufferedData);
      newBuffer.set(chunk, currentLength);
      this.#bufferedData = newBuffer;
    }
  }
}
