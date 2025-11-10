import { type IStreamReadableBuffer } from "./streams.ts";

/**
 * A readable buffer implementation that reads data sequentially from a ReadableStream.
 * This provides streaming read capabilities for large data sources.
 *
 * Key features:
 * - Stream-backed: Reads data sequentially from a ReadableStream.
 * - Memory efficient: Processes data in chunks without loading everything into memory.
 * - Web standard: Uses the Web Streams API for broad compatibility.
 *
 * @example
 * ```typescript
 * const response = await fetch('large-file.avro');
 * const stream = response.body!;
 * const buffer = new StreamReadableBuffer(stream);
 *
 * let chunk;
 * while ((chunk = await buffer.readNext()) !== undefined) {
 *   // Process chunk
 * }
 * ```
 */
export class StreamReadableBuffer implements IStreamReadableBuffer {
  #reader: ReadableStreamDefaultReader<Uint8Array>;

  /**
   * Creates a new StreamReadableBuffer from the provided ReadableStream.
   *
   * @param stream The ReadableStream to read data from.
   */
  public constructor(stream: ReadableStream<Uint8Array>) {
    this.#reader = stream.getReader();
  }

  /**
   * Reads the next chunk of data from the stream.
   *
   * @returns A Promise that resolves to the next Uint8Array chunk, or undefined if the stream is exhausted.
   */
  public async readNext(): Promise<Uint8Array | undefined> {
    const { done, value } = await this.#reader.read();
    if (done) {
      return undefined;
    }
    return value;
  }

  /**
   * Closes the stream and releases the reader lock.
   */
  public async close(): Promise<void> {
    await this.#reader.releaseLock();
  }
}
