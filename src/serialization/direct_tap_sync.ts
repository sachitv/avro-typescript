import { decode } from "./text_encoding.ts";
import type { SyncReadableTapLike } from "./tap_sync.ts";
import { compareUint8Arrays } from "./compare_bytes.ts";

/**
 * High-performance synchronous readable tap that works directly on a Uint8Array.
 *
 * This tap eliminates the buffer abstraction overhead present in SyncReadableTap:
 * - No virtual dispatch through ISyncReadable interface
 * - No checkBounds() call on every read operation
 * - No subarray() allocation for fixed-size reads
 * - Direct array access with a single position cursor
 *
 * Safety model: Unlike SyncReadableTap which validates bounds on every read,
 * this tap defers validation. If you read past the end of the buffer, you'll
 * get undefined values. Use isValid() or canReadMore() to check bounds when needed.
 *
 * This is designed for hot paths where the caller knows the data is well-formed
 * (e.g., reading from a pre-validated Avro container).
 */
export class DirectSyncReadableTap implements SyncReadableTapLike {
  readonly #buf: Uint8Array;
  #pos: number;
  readonly #dataView: DataView;

  constructor(buf: Uint8Array, pos = 0) {
    this.#buf = buf;
    this.#pos = pos;
    this.#dataView = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }

  /** Returns the current position in the buffer. */
  get pos(): number {
    return this.#pos;
  }

  /** Sets the current position in the buffer. */
  set pos(value: number) {
    this.#pos = value;
  }

  /** Returns whether the cursor is positioned within the buffer bounds. */
  isValid(): boolean {
    return this.#pos >= 0 && this.#pos <= this.#buf.length;
  }

  /** Returns whether more data can be read from the current position. */
  canReadMore(): boolean {
    return this.#pos < this.#buf.length;
  }

  /** Returns the buffer contents from the start up to the current cursor. */
  getValue(): Uint8Array {
    return this.#buf.subarray(0, this.#pos);
  }

  /** Reads the next byte as a boolean value and advances the cursor. */
  readBoolean(): boolean {
    return !!this.#buf[this.#pos++];
  }

  /** Skips a boolean value by advancing the cursor by one byte. */
  skipBoolean(): void {
    this.#pos++;
  }

  /**
   * Reads a variable-length zig-zag encoded 32-bit signed integer.
   * Optimized: direct array access without bounds checking.
   */
  readInt(): number {
    const buf = this.#buf;
    let pos = this.#pos;
    let result = 0;
    let shift = 0;
    let byte: number;

    do {
      byte = buf[pos++]!;
      if (shift >= 28) {
        // 5th byte: only 4 bits allowed for int32
        if ((byte & 0x70) !== 0) {
          throw new RangeError(
            "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
          );
        }
      }
      result |= (byte & 0x7f) << shift;
      shift += 7;
    } while ((byte & 0x80) !== 0);

    this.#pos = pos;
    return (result >>> 1) ^ -(result & 1);
  }

  /**
   * Reads a variable-length zig-zag encoded 64-bit signed integer as bigint.
   * Optimized: direct array access without bounds checking.
   */
  readLong(): bigint {
    const buf = this.#buf;
    let pos = this.#pos;
    let shift = 0n;
    let result = 0n;
    let byte: number;

    do {
      byte = buf[pos++]!;
      if (shift >= 64n) {
        throw new RangeError(
          "Varint requires more than 10 bytes (int64 range exceeded)",
        );
      }
      const chunk = BigInt.asUintN(64, BigInt(byte & 0x7f) << shift);
      result = BigInt.asUintN(64, result | chunk);
      shift += 7n;
    } while ((byte & 0x80) !== 0);

    this.#pos = pos;

    const shifted = BigInt.asUintN(64, result >> 1n);
    const sign = BigInt.asUintN(64, -(result & 1n));
    return BigInt.asIntN(64, shifted ^ sign);
  }

  /** Skips a zig-zag encoded 32-bit integer. */
  skipInt(): void {
    this.skipLong();
  }

  /** Skips a zig-zag encoded 64-bit integer by advancing past continuation bytes. */
  skipLong(): void {
    const buf = this.#buf;
    let pos = this.#pos;
    while (buf[pos++]! & 0x80) {
      /* continue until we find a byte without continuation bit */
    }
    this.#pos = pos;
  }

  readFloat(): number {
    const pos = this.#pos;
    this.#pos = pos + 4;
    return this.#dataView.getFloat32(pos, true);
  }

  /** Skips a 32-bit floating point value by advancing four bytes. */
  skipFloat(): void {
    this.#pos += 4;
  }

  readDouble(): number {
    const pos = this.#pos;
    this.#pos = pos + 8;
    return this.#dataView.getFloat64(pos, true);
  }

  /** Skips a 64-bit floating point value by advancing eight bytes. */
  skipDouble(): void {
    this.#pos += 8;
  }

  /** Reads a fixed-length byte sequence. */
  readFixed(len: number): Readonly<Uint8Array> {
    const pos = this.#pos;
    this.#pos += len;
    return this.#buf.subarray(pos, pos + len);
  }

  /** Skips a fixed-length byte sequence. */
  skipFixed(len: number): void {
    this.#pos += len;
  }

  /** Reads a length-prefixed byte sequence. */
  readBytes(): Readonly<Uint8Array> {
    const length = this.readInt();
    if (length < 0) {
      throw new RangeError(`Invalid negative bytes length: ${length}`);
    }
    return this.readFixed(length);
  }

  /** Skips a length-prefixed byte sequence. */
  skipBytes(): void {
    const len = this.readInt();
    if (len < 0) {
      throw new RangeError(`Invalid negative bytes length: ${len}`);
    }
    this.#pos += len;
  }

  /** Reads a length-prefixed UTF-8 string. */
  readString(): string {
    const len = this.readInt();
    if (len < 0) {
      throw new RangeError(`Invalid negative string length: ${len}`);
    }
    const pos = this.#pos;
    this.#pos += len;
    return decode(this.#buf.subarray(pos, pos + len));
  }

  /** Skips a length-prefixed UTF-8 string. */
  skipString(): void {
    const len = this.readInt();
    if (len < 0) {
      throw new RangeError(`Invalid negative string length: ${len}`);
    }
    this.#pos += len;
  }

  /** Compares boolean values from this tap and another. */
  matchBoolean(tap: SyncReadableTapLike): number {
    const v1 = this.readBoolean();
    const v2 = tap.readBoolean();
    return Number(v1) - Number(v2);
  }

  /** Compares 32-bit integer values from this tap and another. */
  matchInt(tap: SyncReadableTapLike): number {
    return this.matchLong(tap);
  }

  /** Compares 64-bit integer values from this tap and another. */
  matchLong(tap: SyncReadableTapLike): number {
    const n1 = this.readLong();
    const n2 = tap.readLong();
    if (n1 === n2) {
      return 0;
    } else if (n1 < n2) {
      return -1;
    } else {
      return 1;
    }
  }

  /** Compares 32-bit float values from this tap and another. */
  matchFloat(tap: SyncReadableTapLike): number {
    const n1 = this.readFloat();
    const n2 = tap.readFloat();
    if (n1 === n2) {
      return 0;
    } else if (n1 < n2) {
      return -1;
    } else {
      return 1;
    }
  }

  /** Compares 64-bit double values from this tap and another. */
  matchDouble(tap: SyncReadableTapLike): number {
    const n1 = this.readDouble();
    const n2 = tap.readDouble();
    if (n1 === n2) {
      return 0;
    } else if (n1 < n2) {
      return -1;
    } else {
      return 1;
    }
  }

  /** Compares fixed-length byte sequences from this tap and another. */
  matchFixed(tap: SyncReadableTapLike, len: number): number {
    const f1 = this.readFixed(len);
    const f2 = tap.readFixed(len);
    return compareUint8Arrays(f1, f2);
  }

  /** Compares length-prefixed byte sequences from this tap and another. */
  matchBytes(tap: SyncReadableTapLike): number {
    return this.matchString(tap);
  }

  /** Compares length-prefixed strings from this tap and another. */
  matchString(tap: SyncReadableTapLike): number {
    const l1 = this.readInt();
    const b1 = this.readFixed(l1);
    const l2 = tap.readInt();
    const b2 = tap.readFixed(l2);
    return compareUint8Arrays(b1, b2);
  }

  // ==========================================================================
  // BULK READ METHODS (for array optimization)
  // ==========================================================================
  // These methods read multiple values in a tight loop, avoiding the virtual
  // dispatch overhead of calling itemsType.readSync(tap) for each element.

  /**
   * Reads an array of varint-encoded zig-zag 32-bit integers directly into an array.
   * @param result Pre-allocated array to fill starting at startIdx.
   * @param startIdx Index in result to start writing.
   * @param count Number of integers to read.
   */
  readIntArrayInto(result: number[], startIdx: number, count: number): void {
    const buf = this.#buf;
    let pos = this.#pos;

    for (let i = 0; i < count; i++) {
      // Inline varint decode + zig-zag
      let value = 0;
      let shift = 0;
      let byte: number;
      do {
        byte = buf[pos++]!;
        if (shift >= 28) {
          // 5th byte: only 4 bits allowed for int32
          if ((byte & 0x70) !== 0) {
            throw new RangeError(
              "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
            );
          }
        }
        value |= (byte & 0x7f) << shift;
        shift += 7;
      } while ((byte & 0x80) !== 0);
      result[startIdx + i] = (value >>> 1) ^ -(value & 1);
    }

    this.#pos = pos;
  }

  /**
   * Reads an array of varint-encoded zig-zag 64-bit integers (bigint) directly into an array.
   * @param result Pre-allocated array to fill starting at startIdx.
   * @param startIdx Index in result to start writing.
   * @param count Number of longs to read.
   */
  readLongArrayInto(result: bigint[], startIdx: number, count: number): void {
    const buf = this.#buf;
    let pos = this.#pos;

    for (let i = 0; i < count; i++) {
      // Inline varint decode + zig-zag for bigint
      let shift = 0n;
      let value = 0n;
      let byte: number;
      do {
        byte = buf[pos++]!;
        if (shift >= 64n) {
          throw new RangeError(
            "Varint requires more than 10 bytes (int64 range exceeded)",
          );
        }
        const chunk = BigInt.asUintN(64, BigInt(byte & 0x7f) << shift);
        value = BigInt.asUintN(64, value | chunk);
        shift += 7n;
      } while ((byte & 0x80) !== 0);

      const shifted = BigInt.asUintN(64, value >> 1n);
      const sign = BigInt.asUintN(64, -(value & 1n));
      result[startIdx + i] = BigInt.asIntN(64, shifted ^ sign);
    }

    this.#pos = pos;
  }

  readFloatArrayInto(result: number[], startIdx: number, count: number): void {
    const dv = this.#dataView;
    let pos = this.#pos;

    for (let i = 0; i < count; i++) {
      result[startIdx + i] = dv.getFloat32(pos, true);
      pos += 4;
    }

    this.#pos = pos;
  }

  readDoubleArrayInto(result: number[], startIdx: number, count: number): void {
    const dv = this.#dataView;
    let pos = this.#pos;

    for (let i = 0; i < count; i++) {
      result[startIdx + i] = dv.getFloat64(pos, true);
      pos += 8;
    }

    this.#pos = pos;
  }

  /**
   * Reads an array of booleans directly into an array.
   * @param result Pre-allocated array to fill starting at startIdx.
   * @param startIdx Index in result to start writing.
   * @param count Number of booleans to read.
   */
  readBooleanArrayInto(
    result: boolean[],
    startIdx: number,
    count: number,
  ): void {
    const buf = this.#buf;
    let pos = this.#pos;

    for (let i = 0; i < count; i++) {
      result[startIdx + i] = !!buf[pos++];
    }

    this.#pos = pos;
  }

  /**
   * Reads an array of strings directly into an array.
   * @param result Pre-allocated array to fill starting at startIdx.
   * @param startIdx Index in result to start writing.
   * @param count Number of strings to read.
   */
  readStringArrayInto(result: string[], startIdx: number, count: number): void {
    const buf = this.#buf;
    let pos = this.#pos;

    for (let i = 0; i < count; i++) {
      // Inline varint decode for length
      let len = 0;
      let shift = 0;
      let byte: number;
      do {
        byte = buf[pos++]!;
        if (shift >= 28) {
          // 5th byte: only 4 bits allowed for int32
          if ((byte & 0x70) !== 0) {
            throw new RangeError(
              "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
            );
          }
        }
        len |= (byte & 0x7f) << shift;
        shift += 7;
      } while ((byte & 0x80) !== 0);
      len = (len >>> 1) ^ -(len & 1);

      // Decode UTF-8 string
      result[startIdx + i] = decode(buf.subarray(pos, pos + len));
      pos += len;
    }

    this.#pos = pos;
  }

  // ==========================================================================
  // BULK MAP READ METHODS (for map optimization)
  // ==========================================================================
  // These methods read map blocks with primitive values directly, avoiding
  // virtual dispatch through valuesType.readSync() for each entry.

  /**
   * Reads a map block with int values directly into a Map.
   * @param result Map to populate.
   * @param count Number of entries to read.
   */
  readMapIntBlockInto(result: Map<string, number>, count: number): void {
    const buf = this.#buf;
    let pos = this.#pos;

    for (let i = 0; i < count; i++) {
      // Read key (inline varint decode for length + string decode)
      let keyLen = 0;
      let keyShift = 0;
      let keyByte: number;
      do {
        keyByte = buf[pos++]!;
        if (keyShift >= 28) {
          // 5th byte: only 4 bits allowed for int32
          if ((keyByte & 0x70) !== 0) {
            throw new RangeError(
              "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
            );
          }
        }
        keyLen |= (keyByte & 0x7f) << keyShift;
        keyShift += 7;
      } while ((keyByte & 0x80) !== 0);
      keyLen = (keyLen >>> 1) ^ -(keyLen & 1);
      const key = decode(buf.subarray(pos, pos + keyLen));
      pos += keyLen;

      // Read value (inline varint decode + zig-zag)
      let value = 0;
      let valShift = 0;
      let valByte: number;
      do {
        valByte = buf[pos++]!;
        if (valShift >= 28) {
          // 5th byte: only 4 bits allowed for int32
          if ((valByte & 0x70) !== 0) {
            throw new RangeError(
              "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
            );
          }
        }
        value |= (valByte & 0x7f) << valShift;
        valShift += 7;
      } while ((valByte & 0x80) !== 0);

      result.set(key, (value >>> 1) ^ -(value & 1));
    }

    this.#pos = pos;
  }

  /**
   * Reads a map block with string values directly into a Map.
   * @param result Map to populate.
   * @param count Number of entries to read.
   */
  readMapStringBlockInto(result: Map<string, string>, count: number): void {
    const buf = this.#buf;
    let pos = this.#pos;

    for (let i = 0; i < count; i++) {
      // Read key (inline varint decode for length + string decode)
      let keyLen = 0;
      let keyShift = 0;
      let keyByte: number;
      do {
        keyByte = buf[pos++]!;
        if (keyShift >= 28) {
          // 5th byte: only 4 bits allowed for int32
          if ((keyByte & 0x70) !== 0) {
            throw new RangeError(
              "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
            );
          }
        }
        keyLen |= (keyByte & 0x7f) << keyShift;
        keyShift += 7;
      } while ((keyByte & 0x80) !== 0);
      keyLen = (keyLen >>> 1) ^ -(keyLen & 1);
      const key = decode(buf.subarray(pos, pos + keyLen));
      pos += keyLen;

      // Read value (inline varint decode for length + string decode)
      let valLen = 0;
      let valShift = 0;
      let valByte: number;
      do {
        valByte = buf[pos++]!;
        if (valShift >= 28) {
          // 5th byte: only 4 bits allowed for int32
          if ((valByte & 0x70) !== 0) {
            throw new RangeError(
              "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
            );
          }
        }
        valLen |= (valByte & 0x7f) << valShift;
        valShift += 7;
      } while ((valByte & 0x80) !== 0);
      valLen = (valLen >>> 1) ^ -(valLen & 1);
      const value = decode(buf.subarray(pos, pos + valLen));
      pos += valLen;

      result.set(key, value);
    }

    this.#pos = pos;
  }
}
