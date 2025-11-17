import type { IStreamWritableBuffer } from "./streams.ts";

/**
 * A writable buffer implementation that writes data sequentially to a WritableStream.
 * This provides streaming write capabilities for large data outputs.
 *
 * Key features:
 * - Stream-backed: Writes data sequentially to a WritableStream.
 * - Memory efficient: Processes data in chunks without buffering everything in memory.
 * - Web standard: Uses the Web Streams API for broad compatibility.
 *
 * @example
 * ```typescript
 * const stream = new WritableStream({
 *   write(chunk) {
 *     // Handle chunk
 *   }
 * });
 * const buffer = new StreamWritableBuffer(stream);
 *
 * await buffer.writeBytes(new Uint8Array([1, 2, 3, 4]));
 * await buffer.close();
 * ```
 */
export class StreamWritableBuffer implements IStreamWritableBuffer {
  #writer: WritableStreamDefaultWriter<Uint8Array>;

  /**
   * Creates a new StreamWritableBuffer from the provided WritableStream.
   *
   * @param stream The WritableStream to write data to.
   */
  public constructor(stream: WritableStream<Uint8Array>) {
    this.#writer = stream.getWriter();
  }

  /**
   * Writes bytes to the stream.
   *
   * @param data The bytes to write.
   */
  public async writeBytes(data: Uint8Array): Promise<void> {
    if (data.length === 0) {
      return;
    }
    await this.#writer.write(data);
  }

  /**
   * Closes the stream and releases the writer lock.
   */
  public async close(): Promise<void> {
    await this.#writer.close();
    this.#writer.releaseLock();
  }
}
