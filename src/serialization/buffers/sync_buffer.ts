/**
 * Synchronous interfaces for readable and writable buffers.
 */

/**
 * Error thrown when a synchronous readable buffer operation fails.
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
 * Error thrown when a synchronous writable buffer operation fails.
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

/**
 * Interface describing a random-access readable buffer (synchronous).
 */
export interface ISyncReadable {
  /**
   * Reads a portion of the buffer starting at offset with the given size.
   * Throws a ReadBufferError when the requested range exceeds the bounds of
   * the buffer.
   */
  read(offset: number, size: number): Uint8Array;

  /**
   * Checks if more data can be read starting at the given offset.
   */
  canReadMore(offset: number): boolean;
}

/**
 * Interface describing an append-only writable buffer (synchronous).
 */
export interface ISyncWritable {
  /**
   * Appends bytes to the buffer, advancing its internal write cursor when the
   * operation succeeds.
   */
  appendBytes(data: Uint8Array): void;

  /**
   * Returns whether the buffer can continue accepting writes. Implementations
   * should flip this to `false` after a write would exceed capacity so callers
   * can detect the overflow condition.
   */
  isValid(): boolean;

  /**
   * Checks if the buffer can accept appending the given number of bytes.
   */
  canAppendMore(size: number): boolean;
}

/**
 * Convenience type for buffers capable of both read and write operations.
 */
export type ISyncReadableAndWritable = ISyncReadable & ISyncWritable;
