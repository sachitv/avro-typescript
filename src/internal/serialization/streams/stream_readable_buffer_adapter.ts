import { type IReadableBuffer } from "../buffers/buffer.ts";
import { type IStreamReadableBuffer } from "./streams.ts";

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
   * @returns A Promise that resolves to a new Uint8Array containing the read bytes, or undefined if the read would exceed buffer bounds.
   */
  public async read(
    offset: number,
    size: number,
  ): Promise<Uint8Array | undefined> {
    if (offset < 0 || size < 0) {
      return undefined;
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
      return undefined;
    }

    return this.#bufferedData.slice(offset, offset + size);
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
