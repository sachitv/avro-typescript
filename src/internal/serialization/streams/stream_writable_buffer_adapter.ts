import { type IWritableBuffer } from "../buffers/buffer.ts";
import { type IStreamWritableBuffer } from "./streams.ts";

/**
 * Adapter that wraps an IStreamWritableBuffer to provide IWritableBuffer interface.
 * This collects writes and forwards them to the stream buffer.
 */
export class StreamWritableBufferAdapter implements IWritableBuffer {
  #streamBuffer: IStreamWritableBuffer;
  #isClosed = false;

  /**
   * Creates a new adapter from a stream writable buffer.
   *
   * @param streamBuffer The stream buffer to adapt.
   */
  public constructor(streamBuffer: IStreamWritableBuffer) {
    this.#streamBuffer = streamBuffer;
  }

  /**
   * Appends bytes to the buffer, advancing its internal write cursor when the
   * operation succeeds.
   */
  public async appendBytes(data: Uint8Array): Promise<void> {
    if (this.#isClosed) {
      return;
    }
    await this.#streamBuffer.writeBytes(data);
  }

  /**
   * Returns whether the buffer can continue accepting writes.
   * Stream buffers can always accept writes until explicitly closed.
   */
  // deno-lint-ignore require-await
  public async isValid(): Promise<boolean> {
    return !this.#isClosed;
  }

  /**
   * Closes the underlying stream buffer.
   * After calling this, further writes will be ignored.
   */
  public async close(): Promise<void> {
    if (!this.#isClosed) {
      this.#isClosed = true;
      await this.#streamBuffer.close();
    }
  }
}
