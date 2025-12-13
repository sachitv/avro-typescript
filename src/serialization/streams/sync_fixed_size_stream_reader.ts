import type { ISyncStreamReadableBuffer } from "./sync_streams.ts";

/**
 * Simple synchronous stream reader backed by a fixed-size Uint8Array.
 * Data is delivered sequentially in fixed-size chunks until the buffer is exhausted.
 */
export class SyncFixedSizeStreamReader implements ISyncStreamReadableBuffer {
  #source: Uint8Array;
  #chunkSize: number;
  #offset = 0;
  #isClosed = false;

  /**
   * Creates a new fixed-size reader.
   *
   * @param source The source data to read from.
   * @param chunkSize The maximum chunk size to return per readNext call.
   */
  public constructor(source: Uint8Array, chunkSize: number) {
    if (chunkSize <= 0) {
      throw new RangeError("chunkSize must be positive");
    }
    this.#source = source;
    this.#chunkSize = chunkSize;
  }

  /**
   * Reads the next chunk of data from the underlying source.
   *
   * @returns The next Uint8Array chunk, or undefined if the stream is exhausted or closed.
   */
  public readNext(): Uint8Array | undefined {
    if (this.#isClosed || this.#offset >= this.#source.length) {
      return undefined;
    }

    const remaining = this.#source.length - this.#offset;
    const size = Math.min(this.#chunkSize, remaining);
    const chunk = this.#source.slice(this.#offset, this.#offset + size);
    this.#offset += size;
    return chunk;
  }

  /**
   * Closes the reader and prevents further reads.
   */
  public close(): void {
    this.#isClosed = true;
  }

  /**
   * @internal test helper exposing the current read offset.
   */
  public _testOnlyOffset(): number {
    return this.#offset;
  }
}
