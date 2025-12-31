/**
 * Error classes for tap read and write operations.
 * These errors are used by both synchronous and asynchronous taps to provide
 * consistent error reporting across the serialization layer.
 */

/**
 * Error thrown when a tap read operation fails.
 */
export class ReadBufferError extends Error {
  /** The offset where the read operation failed. */
  public readonly offset: number;
  /** The size requested for the read operation. */
  public readonly size: number;
  /** The total buffer length. */
  public readonly bufferLength: number;

  /**
   * Creates a new ReadBufferError.
   * @param message The error message.
   * @param offset The offset where the read failed.
   * @param size The size requested.
   * @param bufferLength The total buffer length.
   */
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

/**
 * Error thrown when a tap write operation fails.
 */
export class WriteBufferError extends Error {
  /** The offset where the write operation failed. */
  public readonly offset: number;
  /** The size of data being written. */
  public readonly dataSize: number;
  /** The total buffer length. */
  public readonly bufferLength: number;

  /**
   * Creates a new WriteBufferError.
   * @param message The error message.
   * @param offset The offset where the write failed.
   * @param dataSize The size of data being written.
   * @param bufferLength The total buffer length.
   */
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
