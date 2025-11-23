import type { IStreamReadableBuffer } from "./streams.ts";
import type { IReadableBuffer } from "../buffers/buffer.ts";

/**
 * Adapter that wraps an IStreamReadableBuffer to provide forward-only reading.
 * This adapter only allows forward sequential reads. Attempting to read backwards
 * or seek forward will throw an error. The buffer grows as needed and shrinks
 * by discarding consumed data to save memory.
 */
export class ForwardOnlyStreamReadableBufferAdapter implements IReadableBuffer {
  #bufferedData: Uint8Array | null = null;
  #streamBuffer: IStreamReadableBuffer;
  #eof: boolean = false;
  #currentPosition: number = 0;

  /**
   * Creates a new forward-only adapter from a stream readable buffer.
   * Reads must be sequential from the current position.
   *
   * @param streamBuffer The stream buffer to adapt.
   */
  public constructor(streamBuffer: IStreamReadableBuffer) {
    this.#streamBuffer = streamBuffer;
  }

  /**
   * Reads a sequence of bytes from the buffer starting at the current position.
   * Only sequential reads are allowed; attempting to read backwards or seek
   * forward will throw an error. The read advances the current position.
   *
   * @param offset The byte offset to start reading from (must be current position).
   * @param size The number of bytes to read.
   * @returns A Promise that resolves to a new Uint8Array containing the read bytes, or undefined if the read would exceed buffer bounds.
   * @throws Error if attempting to read backwards or seek forward.
   */
  public async read(
    offset: number,
    size: number,
  ): Promise<Uint8Array | undefined> {
    if (offset < 0 || size < 0) {
      return undefined;
    }

    if (offset < this.#currentPosition) {
      throw new Error("Cannot read backwards from current position");
    }

    if (offset > this.#currentPosition) {
      throw new Error("Cannot seek forward; reads must be sequential");
    }

    // At this point, offset must equal currentPosition due to the checks above
    await this.#ensureBufferedUpTo(this.#currentPosition + size);

    if (this.#bufferedData === null || size > this.#bufferedData.length) {
      return undefined;
    }

    const result = this.#bufferedData.slice(0, size);

    // Advance position
    this.#currentPosition += size;

    // Trim buffer to discard consumed data
    // Split from size position onwards
    this.#bufferedData = this.#bufferedData.slice(size);

    return result;
  }

  public async canReadMore(offset: number): Promise<boolean> {
    if (offset < this.#currentPosition) {
      throw new Error("Cannot read backwards from current position");
    }

    if (offset > this.#currentPosition) {
      throw new Error("Cannot seek forward; reads must be sequential");
    }

    await this.#ensureBufferedUpTo(this.#currentPosition + 1);

    if (this.#bufferedData === null) {
      return false;
    }
    return this.#bufferedData.length > 0;
  }

  /**
   * Ensures data is buffered up to the specified target offset.
   * Loads chunks progressively until the target is reached or EOF.
   */
  async #ensureBufferedUpTo(targetOffset: number): Promise<void> {
    // Load chunks until we have enough data or reach EOF
    while (
      !this.#eof &&
      (this.#bufferedData === null ||
        this.#bufferedData.length + this.#currentPosition < targetOffset)
    ) {
      const chunk = await this.#streamBuffer.readNext();
      if (chunk === undefined) {
        this.#eof = true;
        break;
      }

      // Expand buffer to accommodate new chunk
      if (this.#bufferedData === null) {
        this.#bufferedData = chunk;
      } else {
        const currentLength: number = this.#bufferedData.length;
        const newBuffer: Uint8Array = new Uint8Array(
          currentLength + chunk.length,
        );
        newBuffer.set(this.#bufferedData);
        newBuffer.set(chunk, currentLength);
        this.#bufferedData = newBuffer;
      }
    }
  }
}
