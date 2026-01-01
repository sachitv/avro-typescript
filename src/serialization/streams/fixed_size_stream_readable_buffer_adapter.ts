import type { IReadableBuffer } from "../buffers/buffer.ts";
import { ReadBufferError } from "../buffers/buffer_error.ts";
import type { IStreamReadableBuffer } from "./streams.ts";
import { CircularBuffer } from "../../internal/collections/circular_buffer.ts";

/**
 * A limited buffer adapter that maintains a rolling window of stream data.
 * This provides memory-efficient random access within a sliding window,
 * but cannot access data that has been discarded from the window.
 *
 * Key features:
 * - Rolling window: Maintains only a limited amount of data in memory
 * - Forward access: Can read data ahead in the stream
 * - Limited backward access: Can only access data within the current window
 * - Memory efficient: Suitable for large streams where full buffering is impractical
 *
 * Limitations:
 * - Cannot access data before the current window start
 * - Window size determines maximum backward seek distance
 */
export class FixedSizeStreamReadableBufferAdapter implements IReadableBuffer {
  #streamBuffer: IStreamReadableBuffer;
  #windowSize: number;
  #circularBuffer: CircularBuffer;
  #eof: boolean;

  /**
   * Creates a new limited buffer adapter.
   *
   * @param streamBuffer The stream buffer to read from
   * @param windowSize The size of the rolling window in bytes
   * @param circularBuffer Optional circular buffer for testing
   */
  public constructor(
    streamBuffer: IStreamReadableBuffer,
    windowSize: number,
    circularBuffer?: CircularBuffer,
  ) {
    if (windowSize <= 0) {
      throw new RangeError("Window size must be positive");
    }
    this.#streamBuffer = streamBuffer;
    this.#windowSize = windowSize;
    this.#circularBuffer = circularBuffer ?? new CircularBuffer(windowSize);
    this.#eof = false;
  }

  /**
   * Gets the total length of the buffer.
   * For limited buffers, this returns the current buffer end position.
   * Note: This may not represent the total stream length if not fully buffered.
   *
   * @returns The current buffer length in bytes
   */
  // deno-lint-ignore require-await
  public async length(): Promise<number> {
    return this.#circularBuffer.windowEnd();
  }

  /**
   * Reads a sequence of bytes from the buffer starting at the specified offset.
   * The offset is relative to the stream start.
   *
   * @param offset The byte offset to start reading from (0-based, stream-relative)
   * @param size The number of bytes to read
   * @returns A Promise that resolves to a new Uint8Array containing the read bytes.
   * @throws ReadBufferError if the requested range is out of bounds.
   */
  public async read(
    offset: number,
    size: number,
  ): Promise<Uint8Array> {
    if (offset < 0 || size < 0) {
      throw new ReadBufferError(
        `Offset and size must be non-negative. Got offset=${offset}, size=${size}`,
        offset,
        size,
        this.#circularBuffer.windowEnd(),
      );
    }

    // Check if requested size exceeds window size - throw error immediately
    if (size > this.#windowSize) {
      throw new ReadBufferError(
        `Requested size ${size} exceeds window size ${this.#windowSize}`,
        offset,
        size,
        this.#windowSize,
      );
    }

    // Check if offset is before the current window start - raise error
    if (offset < this.#circularBuffer.windowStart()) {
      throw new ReadBufferError(
        `Cannot read data before window start. Offset ${offset} is before window start ${this.#circularBuffer.windowStart()}`,
        offset,
        size,
        this.#circularBuffer.windowEnd(),
      );
    }

    // Check if data is already in cache
    if (offset + size <= this.#circularBuffer.windowEnd()) {
      // Data is already cached, extract it directly
      return this.#extractFromBuffer(offset, size);
    }

    // Data is not in cache, wait until it's loaded
    await this.#fillBuffer(offset + size);

    // Check if the requested range is available after filling
    if (offset + size > this.#circularBuffer.windowEnd()) {
      throw new ReadBufferError(
        `Operation exceeds buffer bounds. offset=${offset}, size=${size}, bufferLength=${this.#circularBuffer.windowEnd()}`,
        offset,
        size,
        this.#circularBuffer.windowEnd(),
      );
    }

    // Extract data from buffer
    return this.#extractFromBuffer(offset, size);
  }

  /**
   * Checks if more data can be read starting at the given offset.
   * @param offset The byte offset to check.
   * @returns True if at least one byte can be read from the offset.
   */
  public async canReadMore(offset: number): Promise<boolean> {
    try {
      await this.read(offset, 1);
      return true;
    } catch (err) {
      if (err instanceof ReadBufferError) {
        return false;
      }
      throw err;
    }
  }

  /**
   * Extracts a contiguous range of bytes from the circular buffer.
   */
  #extractFromBuffer(offset: number, size: number): Uint8Array {
    return this.#circularBuffer.get(offset, size);
  }

  /**
   * Fills the buffer with data from the stream until at least targetOffset is reached.
   */
  async #fillBuffer(targetOffset: number): Promise<void> {
    while (!this.#eof && this.#circularBuffer.windowEnd() < targetOffset) {
      const chunk = await this.#streamBuffer.readNext();
      if (chunk === undefined) {
        this.#eof = true;
        break;
      }

      try {
        this.#circularBuffer.push(chunk);
      } catch (error) {
        if (error instanceof ReadBufferError) {
          throw new ReadBufferError(
            `Cannot buffer chunk of size ${chunk.length}: ${error.message}`,
            this.#circularBuffer.windowEnd(),
            chunk.length,
            this.#windowSize,
          );
        }
        throw error;
      }
    }
  }
}
