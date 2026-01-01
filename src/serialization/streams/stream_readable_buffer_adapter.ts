import type { IReadableBuffer } from "../buffers/buffer.ts";
import { ReadBufferError } from "../buffers/buffer_error.ts";
import type { IStreamReadableBuffer } from "./streams.ts";

/**
 * Adapter that wraps an IStreamReadableBuffer to provide IReadableBuffer interface.
 * This buffers stream content progressively to enable random access reads.
 */
export class StreamReadableBufferAdapter implements IReadableBuffer {
  #bufferedData: Uint8Array | null = null;
  #streamBuffer: IStreamReadableBuffer;
  #eof: boolean = false;

  /**
   * Creates a new adapter from a stream readable buffer.
   * The entire stream will be buffered in memory for random access.
   *
   * @param streamBuffer The stream buffer to adapt.
   */
  public constructor(streamBuffer: IStreamReadableBuffer) {
    this.#streamBuffer = streamBuffer;
  }

  /**
   * Gets the total length of the buffer in bytes.
   * This will buffer the entire stream if not already done.
   *
   * @returns The buffer length in bytes.
   */
  public async length(): Promise<number> {
    await this.#ensureBuffered();
    return this.#bufferedData!.length;
  }

  /**
   * Reads a sequence of bytes from the buffer starting at the specified offset.
   * This will buffer data as needed to satisfy the read request.
   *
   * @param offset The byte offset to start reading from (0-based).
   * @param size The number of bytes to read.
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
        this.#bufferedData?.length ?? 0,
      );
    }

    // Check if the section is already cached
    if (
      this.#bufferedData !== null && offset + size <= this.#bufferedData.length
    ) {
      return this.#bufferedData.slice(offset, offset + size);
    }

    // Load chunks until the necessary data is available
    await this.#ensureBufferedUpTo(offset + size);

    // Check if we have enough data after buffering
    if (
      this.#bufferedData === null || offset + size > this.#bufferedData.length
    ) {
      throw new ReadBufferError(
        `Operation exceeds buffer bounds. offset=${offset}, size=${size}, bufferLength=${
          this.#bufferedData?.length ?? 0
        }`,
        offset,
        size,
        this.#bufferedData?.length ?? 0,
      );
    }

    return this.#bufferedData.slice(offset, offset + size);
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
   * Ensures the entire stream is buffered in memory.
   */
  async #ensureBuffered(): Promise<void> {
    await this.#ensureBufferedUpTo(Infinity);
  }

  /**
   * Ensures data is buffered up to the specified target offset.
   * Loads chunks progressively until the target is reached or EOF.
   */
  async #ensureBufferedUpTo(targetOffset: number): Promise<void> {
    // Initialize buffer if needed
    if (this.#bufferedData === null) {
      this.#bufferedData = new Uint8Array(0);
    }

    // Load chunks until we have enough data or reach EOF
    while (!this.#eof && this.#bufferedData!.length < targetOffset) {
      const chunk = await this.#streamBuffer.readNext();
      if (chunk === undefined) {
        this.#eof = true;
        break;
      }

      // Expand buffer to accommodate new chunk
      const currentLength: number = this.#bufferedData!.length;
      const newBuffer: Uint8Array = new Uint8Array(
        currentLength + chunk.length,
      );
      newBuffer.set(this.#bufferedData!);
      newBuffer.set(chunk, currentLength);
      this.#bufferedData = newBuffer;
    }
  }
}
