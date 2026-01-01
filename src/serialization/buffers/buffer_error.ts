/**
 * Shared error types for buffer implementations.
 *
 * These errors are shared between the async and sync buffer implementations so
 * that callers can rely on consistent error shapes.
 */

/** Error thrown when a readable buffer operation fails. */
export class ReadBufferError extends RangeError {
  /** The offset where the read operation failed. */
  public readonly offset: number;
  /** The size requested for the read operation. */
  public readonly size: number;
  /** The total buffer length (or best-effort upper bound). */
  public readonly bufferLength: number;

  constructor(
    message: string,
    offset: number,
    size: number,
    bufferLength: number,
  ) {
    super(message);
    this.name = "ReadBufferError";
    this.offset = offset;
    this.size = size;
    this.bufferLength = bufferLength;
  }
}

/** Error thrown when a writable buffer operation fails. */
export class WriteBufferError extends RangeError {
  /** The offset where the write operation failed. */
  public readonly offset: number;
  /** The size of data being written. */
  public readonly dataSize: number;
  /** The total buffer length (or best-effort upper bound). */
  public readonly bufferLength: number;

  constructor(
    message: string,
    offset: number,
    dataSize: number,
    bufferLength: number,
  ) {
    super(message);
    this.name = "WriteBufferError";
    this.offset = offset;
    this.dataSize = dataSize;
    this.bufferLength = bufferLength;
  }
}
