import { type IReadableBuffer } from "../buffers/buffer.ts";
import { type IStreamReadableBuffer } from "./streams.ts";
import { CircularBuffer } from "../../collections/circular_buffer.ts";

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
   * @returns A Promise that resolves to a new Uint8Array containing the read bytes, or undefined if the read would exceed buffer bounds
   */
  public async read(
    offset: number,
    size: number,
  ): Promise<Uint8Array | undefined> {
    if (offset < 0 || size < 0) {
      return undefined;
    }

    // Check if requested size exceeds window size - throw error immediately
    if (size > this.#windowSize) {
      throw new RangeError(
        `Requested size ${size} exceeds window size ${this.#windowSize}`,
      );
    }

    // Check if offset is before the current window start - raise error
    if (offset < this.#circularBuffer.windowStart()) {
      throw new RangeError(
        `Cannot read data before window start. Offset ${offset} is before window start ${this.#circularBuffer.windowStart()}`,
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
      return undefined;
    }

    // Extract data from buffer
    return this.#extractFromBuffer(offset, size);
  }

  /**
   * Extracts a contiguous range of bytes from the circular buffer.
   */
  #extractFromBuffer(offset: number, size: number): Uint8Array | undefined {
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
        if (error instanceof RangeError) {
          throw new RangeError(
            `Cannot buffer chunk of size ${chunk.length}: ${error.message}`,
          );
        }
        throw error;
      }
    }
  }
}
