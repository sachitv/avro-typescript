/**
 * A circular buffer implementation that maintains a rolling window of Uint8Array data.
 * Uses copyWithin for efficient window sliding and provides memory-efficient random access.
 */
export class CircularBuffer {
  #buffer: Uint8Array;
  #capacity: number;
  #windowStart: number;
  #windowEnd: number;

  /**
   * Creates a new circular buffer with the specified capacity.
   * @param capacity Maximum number of bytes the buffer can hold
   * @throws RangeError if capacity is not positive
   */
  public constructor(capacity: number) {
    if (capacity <= 0) {
      throw new RangeError("Capacity must be positive");
    }
    this.#capacity = capacity;
    this.#buffer = new Uint8Array(capacity);
    this.#windowStart = 0;
    this.#windowEnd = 0;
  }

  /**
   * Maximum capacity of the buffer in bytes.
   */
  public capacity(): number {
    return this.#capacity;
  }

  /**
   * Current number of bytes in the buffer.
   */
  public length(): number {
    return this.#windowEnd - this.#windowStart;
  }

  /**
   * Position in the overall data stream where the current window starts.
   */
  public windowStart(): number {
    return this.#windowStart;
  }

  /**
   * Position in the overall data stream where the current window ends (exclusive).
   */
  public windowEnd(): number {
    return this.#windowEnd;
  }

  /**
   * Clears the buffer, resetting the window to empty.
   */
  public clear(): void {
    this.#windowStart = 0;
    this.#windowEnd = 0;
  }

  /**
   * Adds data to the buffer, sliding the window if necessary.
   * @param data Uint8Array to add
   * @throws RangeError if data size exceeds buffer capacity
   */
  public push(data: Uint8Array): void {
    if (data.length === 0) {
      return;
    }

    this.#validateDataSize(data.length);
    this.#slideWindowIfNeeded(data.length);
    this.#addData(data);
  }

  /**
   * Validates that the data size doesn't exceed buffer capacity.
   * @param dataSize Size of data to validate
   * @throws RangeError if data size exceeds capacity
   */
  #validateDataSize(dataSize: number): void {
    if (dataSize > this.#capacity) {
      throw new RangeError(
        `Data size ${dataSize} exceeds buffer capacity ${this.#capacity}`,
      );
    }
  }

  /**
   * Slides the window if adding data would exceed capacity.
   * @param dataSize Size of data being added
   */
  #slideWindowIfNeeded(dataSize: number): void {
    const currentLength = this.length();
    if (currentLength + dataSize > this.#capacity) {
      const slideAmount = (currentLength + dataSize) - this.#capacity;
      const newWindowStart = this.#windowStart + slideAmount;

      // Slide the window using copyWithin
      const shift = newWindowStart - this.#windowStart;
      if (shift > 0 && shift <= currentLength) {
        this.#buffer.copyWithin(0, shift, currentLength);
        this.#windowStart = newWindowStart;
        this.#windowEnd = this.#windowStart + (currentLength - shift);
      }
    }
  }

  /**
   * Adds data to the buffer at the current write position.
   * @param data Uint8Array to add
   */
  #addData(data: Uint8Array): void {
    const bufferOffset = this.#windowEnd - this.#windowStart;
    this.#buffer.set(data, bufferOffset);
    this.#windowEnd += data.length;
  }

  /**
   * Retrieves a contiguous sequence of bytes from the buffer.
   * @param start Start position relative to the overall data stream
   * @param size Number of bytes to retrieve
   * @returns Readonly view of the requested bytes (shallow copy, no allocation)
   * @throws RangeError if start is before window start or size is negative
   * @throws RangeError if requested range extends beyond window end
   */
  public get(start: number, size: number): Readonly<Uint8Array> {
    this.#validateGetParameters(start, size);
    this.#validateGetRange(start, size);

    const bufferStart = start - this.#windowStart;
    return this.#buffer.subarray(bufferStart, bufferStart + size);
  }

  /**
   * Validates get() method parameters.
   * @param start Start position to validate
   * @param size Size to validate
   * @throws RangeError if parameters are invalid
   */
  #validateGetParameters(start: number, size: number): void {
    if (start < this.#windowStart) {
      throw new RangeError(
        `Start position ${start} is before window start ${this.#windowStart}`,
      );
    }
    if (size < 0) {
      throw new RangeError(`Size ${size} cannot be negative`);
    }
  }

  /**
   * Validates that the requested range is within the window.
   * @param start Start position to validate
   * @param size Size to validate
   * @throws RangeError if range extends beyond window
   */
  #validateGetRange(start: number, size: number): void {
    const end = start + size;
    if (end > this.#windowEnd) {
      throw new RangeError(
        `Requested range [${start}, ${end}) extends beyond window end ${this.#windowEnd}`,
      );
    }
  }
}
