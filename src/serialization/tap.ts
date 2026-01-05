import { bigIntToSafeNumber } from "./conversion.ts";
import { compareUint8Arrays } from "./compare_bytes.ts";
import { decode, encode } from "./text_encoding.ts";
import type { IReadableBuffer, IWritableBuffer } from "./buffers/buffer.ts";
import { ReadBufferError, WriteBufferError } from "./buffers/buffer_error.ts";
import {
  InMemoryReadableBuffer,
  InMemoryWritableBuffer,
} from "./buffers/in_memory_buffer.ts";

/**
 * Interface for readable tap operations in Avro serialization.
 */
export interface ReadableTapLike {
  /**
   * Returns whether the cursor is positioned within the buffer bounds.
   */
  isValid(): Promise<boolean>;
  /**
   * Returns whether more data can be read from the current position without advancing the cursor.
   */
  canReadMore(): Promise<boolean>;
  /**
   * Returns the buffer contents from the start up to the current cursor.
   * @throws ReadBufferError if the cursor has advanced past the buffer length.
   */
  getValue(): Promise<Uint8Array>;
  /**
   * Reads the next byte as a boolean value and advances the cursor by one byte.
   */
  readBoolean(): Promise<boolean>;
  /**
   * Skips a boolean value by advancing the cursor by one byte.
   */
  skipBoolean(): void;
  /**
   * Reads a variable-length zig-zag encoded 32-bit signed integer.
   */
  readInt(): Promise<number>;
  /**
   * Reads a variable-length zig-zag encoded 64-bit signed integer as bigint.
   */
  readLong(): Promise<bigint>;
  /**
   * Skips a zig-zag encoded 32-bit integer by delegating to `skipLong`.
   */
  skipInt(): Promise<void>;
  /**
   * Skips a zig-zag encoded 64-bit integer, advancing past continuation bytes.
   */
  skipLong(): Promise<void>;
  /**
   * Reads a 32-bit little-endian floating point number.
   * @throws ReadBufferError if the read would exceed the buffer.
   */
  readFloat(): Promise<number>;
  /**
   * Skips a 32-bit floating point value by advancing four bytes.
   */
  skipFloat(): void;
  /**
   * Reads a 64-bit little-endian floating point number.
   * @throws ReadBufferError if the read would exceed the buffer.
   */
  readDouble(): Promise<number>;
  /**
   * Skips a 64-bit floating point value by advancing eight bytes.
   */
  skipDouble(): void;
  /**
   * Reads a fixed-length byte sequence into a new buffer.
   * @param len Number of bytes to read.
   * @throws ReadBufferError if the read exceeds the buffer.
   */
  readFixed(len: number): Promise<Uint8Array>;
  /**
   * Skips a fixed-length byte sequence.
   * @param len Number of bytes to skip.
   */
  skipFixed(len: number): void;
  /**
   * Reads a length-prefixed byte sequence.
   * @throws ReadBufferError if insufficient data remains.
   */
  readBytes(): Promise<Uint8Array>;
  /**
   * Skips a length-prefixed byte sequence.
   */
  skipBytes(): Promise<void>;
  /**
   * Reads a length-prefixed UTF-8 string.
   * @throws ReadBufferError when the buffer is exhausted prematurely.
   */
  readString(): Promise<string>;
  /**
   * Skips a length-prefixed UTF-8 string.
   */
  skipString(): Promise<void>;
  /**
   * Compares the next boolean value with the one from another tap.
   * @param tap Tap to compare against; both cursors advance.
   * @returns 0 when equal, negative when this tap's value is false and the other true, positive otherwise.
   */
  matchBoolean(tap: ReadableTapLike): Promise<number>;
  /**
   * Compares the next zig-zag encoded 32-bit integer with another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  matchInt(tap: ReadableTapLike): Promise<number>;
  /**
   * Compares the next zig-zag encoded 64-bit integer with another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  matchLong(tap: ReadableTapLike): Promise<number>;
  /**
   * Compares the next 32-bit float value with another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  matchFloat(tap: ReadableTapLike): Promise<number>;
  /**
   * Compares the next 64-bit float value with another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  matchDouble(tap: ReadableTapLike): Promise<number>;
  /**
   * Compares fixed-length byte sequences from this tap and another tap.
   * @param tap Tap to compare against; both cursors advance by `len`.
   * @param len Number of bytes to compare.
   * @returns Comparison result using -1/0/1 semantics.
   */
  matchFixed(tap: ReadableTapLike, len: number): Promise<number>;
  /**
   * Compares length-prefixed byte sequences from this tap and another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  matchBytes(tap: ReadableTapLike): Promise<number>;
  /** Matches a string value. */
  matchString(tap: ReadableTapLike): Promise<number>;
}

/**
 * Interface for writable tap operations compatible with Avro binary serialization.
 */
export interface WritableTapLike {
  /**
   * Returns whether the tap is valid for writing.
   */
  isValid(): Promise<boolean>;
  /**
   * Writes a boolean value as a single byte.
   * @param value The boolean value to write.
   */
  writeBoolean(value: boolean): Promise<void>;
  /**
   * Writes a zig-zag encoded 32-bit signed integer.
   * @param value The integer value to write.
   */
  writeInt(value: number): Promise<void>;
  /**
   * Writes a zig-zag encoded 64-bit signed integer.
   * @param value The bigint value to write.
   */
  writeLong(value: bigint): Promise<void>;
  /**
   * Writes a 32-bit little-endian floating point number.
   * @param value The float value to write.
   */
  writeFloat(value: number): Promise<void>;
  /**
   * Writes a 64-bit little-endian floating point number.
   * @param value The double value to write.
   */
  writeDouble(value: number): Promise<void>;
  /**
   * Writes a fixed-length byte sequence from the provided buffer.
   * @param buf Source buffer to copy from.
   */
  writeFixed(buf: Uint8Array): Promise<void>;
  /**
   * Writes a length-prefixed byte sequence backed by the provided buffer.
   * @param buf The bytes to write.
   */
  writeBytes(buf: Uint8Array): Promise<void>;
  /**
   * Writes a length-prefixed UTF-8 string.
   * @param str The string to encode and write.
   */
  writeString(str: string): Promise<void>;
  /**
   * Writes a binary string as raw bytes without a length prefix.
   * @param str Source string containing binary data.
   * @param len Number of bytes from the string to write.
   */
  writeBinary(str: string, len: number): Promise<void>;
}

function assertValidPosition(pos: number, isWritable = false): void {
  if (
    !Number.isFinite(pos) || !Number.isInteger(pos) || pos < 0 ||
    Math.abs(pos) > Number.MAX_SAFE_INTEGER
  ) {
    const message =
      "Tap position must be an integer within the safe number range.";
    if (isWritable) {
      throw new WriteBufferError(message, pos, 0, 0);
    } else {
      throw new ReadBufferError(message, pos, 0, 0);
    }
  }
}

function isIReadableBuffer(value: unknown): value is IReadableBuffer {
  return typeof value === "object" && value !== null &&
    typeof (value as IReadableBuffer).read === "function" &&
    typeof (value as IReadableBuffer).canReadMore === "function";
}

function isIWritable(value: unknown): value is IWritableBuffer {
  return typeof value === "object" && value !== null &&
    typeof (value as IWritableBuffer).appendBytes === "function" &&
    typeof (value as IWritableBuffer).isValid === "function";
}

/** Abstract base class for tap implementations that manage buffer position. */
export abstract class TapBase {
  /** The current position in the buffer. */
  protected pos: number;

  /** Initializes the tap with the given position. */
  protected constructor(pos: number) {
    this.pos = pos;
  }

  /**
   * Returns the current cursor position within the buffer.
   */
  getPos(): number {
    return this.pos;
  }

  /**
   * Resets the cursor to the beginning of the buffer.
   */
  _testOnlyResetPos(): void {
    this.pos = 0;
  }
}

/**
 * Binary tap that exposes Avro-compatible read helpers on top of a readable buffer.
 */
export class ReadableTap extends TapBase implements ReadableTapLike {
  /** The readable buffer backing this tap. */
  protected readonly buffer: IReadableBuffer;
  #lengthHint?: number;

  /** Creates a new ReadableTap with the given buffer and initial position. */
  constructor(buf: ArrayBuffer | IReadableBuffer, pos = 0) {
    assertValidPosition(pos);
    let buffer: IReadableBuffer;
    let lengthHint: number | undefined;
    if (buf instanceof ArrayBuffer) {
      buffer = new InMemoryReadableBuffer(buf);
      lengthHint = buf.byteLength;
    } else if (isIReadableBuffer(buf)) {
      buffer = buf;
    } else {
      throw new TypeError(
        "ReadableTap requires an ArrayBuffer or IReadableBuffer.",
      );
    }
    super(pos);
    this.buffer = buffer;
    this.#lengthHint = lengthHint;
  }

  /**
   * Returns whether the cursor is positioned within the buffer bounds.
   */
  async isValid(): Promise<boolean> {
    try {
      await this.buffer.read(this.pos, 0);
      return true;
    } catch (err) {
      if (err instanceof ReadBufferError) {
        return false;
      }
      throw err;
    }
  }

  /**
   * Returns whether more data can be read from the current position without advancing the cursor.
   */
  async canReadMore(): Promise<boolean> {
    return await this.buffer.canReadMore(this.pos);
  }

  /**
   * Returns a defensive copy of the current buffer for testing purposes.
   */
  async _testOnlyBuf(): Promise<Uint8Array> {
    const readLength = this.#lengthHint ?? this.pos;
    if (readLength <= 0) {
      return new Uint8Array();
    }
    const bytes = await this.buffer.read(0, readLength);
    return bytes.slice();
  }

  /**
   * Returns the buffer contents from the start up to the current cursor.
   * @throws ReadBufferError if the cursor has advanced past the buffer length.
   */
  async getValue(): Promise<Uint8Array> {
    return await this.buffer.read(0, this.pos);
  }

  /** Retrieves the byte at the specified position in the buffer. */
  private async getByteAt(position: number): Promise<number> {
    const bytes = await this.buffer.read(position, 1);
    return bytes[0];
  }

  /**
   * Reads the next byte as a boolean value and advances the cursor by one byte.
   */
  async readBoolean(): Promise<boolean> {
    const value = await this.getByteAt(this.pos);
    this.pos += 1;
    return !!value;
  }

  /**
   * Skips a boolean value by advancing the cursor by one byte.
   */
  skipBoolean(): void {
    this.pos++;
  }

  /**
   * Reads a variable-length zig-zag encoded 32-bit signed integer.
   * Uses 32-bit math to avoid BigInt overhead for performance.
   *
   * In local benchmarks, this approach is approximately 53% faster than using BigInt operations.
   */
  async readInt(): Promise<number> {
    let pos = this.pos;
    let result = 0;
    let shift = 0;
    let byte: number;
    let bytesRead = 0;

    // Read varint-encoded value using 32-bit arithmetic
    do {
      byte = await this.getByteAt(pos++);
      result |= (byte & 0x7f) << shift;
      shift += 7;
      bytesRead++;
      // int32 needs at most 5 bytes (32 bits / 7 bits per byte = 4.57)
      if (bytesRead > 5) {
        // Value too large for int32, use readLong for proper error handling
        this.pos = pos - bytesRead;
        return bigIntToSafeNumber(await this.readLong(), "readInt value");
      }
    } while ((byte & 0x80) !== 0);

    this.pos = pos;

    // Zig-zag decode: (n >>> 1) ^ -(n & 1)
    const decoded = (result >>> 1) ^ -(result & 1);

    // Note: The 32-bit arithmetic above ensures the result is always within int32 range
    return decoded;
  }

  /**
   * Reads a variable-length zig-zag encoded 64-bit signed integer as bigint.
   *
   * Performance optimization: Uses BigInt.asUintN(64, ...) on each accumulation to signal
   * to V8's Turbofan that these are 64-bit operations, enabling int64 lowering optimizations.
   * See https://groups.google.com/g/v8-reviews/c/SF2tmxAUpB8 and
   * https://v8.dev/blog/bigint#optimization-considerations for details.
   *
   * In local benchmarks, this approach is approximately 13% faster than operations without
   * explicit 64-bit truncation.
   *
   * Note: Values encoded with > 64 bits will be truncated to 64 bits during decoding.
   */
  async readLong(): Promise<bigint> {
    let pos = this.pos;
    let shift = 0n;
    let result = 0n;
    let byte: number;

    do {
      byte = await this.getByteAt(pos++);
      // asUintN(64, ...) helps V8 optimize to int64 operations
      result = BigInt.asUintN(64, result | (BigInt(byte & 0x7f) << shift));
      shift += 7n;
    } while ((byte & 0x80) !== 0 && shift < 70n);

    while ((byte & 0x80) !== 0) {
      byte = await this.getByteAt(pos++);
      result = BigInt.asUintN(64, result | (BigInt(byte & 0x7f) << shift));
      shift += 7n;
    }

    this.pos = pos;
    // Zig-zag decode with asIntN to ensure int64 result
    // Note: BigInt.asIntN(64, ...) always returns a value within int64 range by definition
    return BigInt.asIntN(64, (result >> 1n) ^ -(result & 1n));
  }

  /**
   * Skips a zig-zag encoded 32-bit integer by delegating to `skipLong`.
   */
  async skipInt(): Promise<void> {
    await this.skipLong();
  }

  /**
   * Skips a zig-zag encoded 64-bit integer, advancing past continuation bytes.
   */
  async skipLong(): Promise<void> {
    let pos = this.pos;
    while ((await this.getByteAt(pos++)) & 0x80) {
      /* no-op */
    }
    this.pos = pos;
  }

  /**
   * Reads a 32-bit little-endian floating point number.
   * @throws ReadBufferError if the read would exceed the buffer.
   */
  async readFloat(): Promise<number> {
    const pos = this.pos;
    this.pos += 4;
    const bytes = await this.buffer.read(pos, 4);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getFloat32(0, true);
  }

  /**
   * Skips a 32-bit floating point value by advancing four bytes.
   */
  skipFloat(): void {
    this.pos += 4;
  }

  /**
   * Reads a 64-bit little-endian floating point number.
   * @throws ReadBufferError if the read would exceed the buffer.
   */
  async readDouble(): Promise<number> {
    const pos = this.pos;
    this.pos += 8;
    const bytes = await this.buffer.read(pos, 8);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getFloat64(0, true);
  }

  /**
   * Skips a 64-bit floating point value by advancing eight bytes.
   */
  skipDouble(): void {
    this.pos += 8;
  }

  /**
   * Reads a fixed-length byte sequence into a new buffer.
   * @param len Number of bytes to read.
   * @throws ReadBufferError if the read exceeds the buffer.
   */
  async readFixed(len: number): Promise<Uint8Array> {
    const pos = this.pos;
    this.pos += len;
    return await this.buffer.read(pos, len);
  }

  /**
   * Skips a fixed-length byte sequence.
   * @param len Number of bytes to skip.
   */
  skipFixed(len: number): void {
    this.pos += len;
  }

  /**
   * Reads a length-prefixed byte sequence.
   * @throws ReadBufferError if insufficient data remains.
   */
  async readBytes(): Promise<Uint8Array> {
    const length = bigIntToSafeNumber(
      await this.readLong(),
      "readBytes length",
    );
    return await this.readFixed(length);
  }

  /**
   * Skips a length-prefixed byte sequence.
   */
  async skipBytes(): Promise<void> {
    const len = bigIntToSafeNumber(await this.readLong(), "skipBytes length");
    this.pos += len;
  }

  /**
   * Skips a length-prefixed UTF-8 string.
   */
  async skipString(): Promise<void> {
    const len = bigIntToSafeNumber(await this.readLong(), "skipString length");
    this.pos += len;
  }

  /**
   * Reads a length-prefixed UTF-8 string.
   * @throws ReadBufferError when the buffer is exhausted prematurely.
   */
  async readString(): Promise<string> {
    const len = bigIntToSafeNumber(await this.readLong(), "readString length");
    const bytes = await this.readFixed(len);
    return decode(bytes);
  }

  /**
   * Compares the next boolean value with the one from another tap.
   * @param tap Tap to compare against; both cursors advance.
   * @returns 0 when equal, negative when this tap's value is false and the other true, positive otherwise.
   */
  async matchBoolean(tap: ReadableTapLike): Promise<number> {
    const val1 = await this.readBoolean();
    const val2 = await tap.readBoolean();
    return Number(val1) - Number(val2);
  }

  /**
   * Compares the next zig-zag encoded 32-bit integer with another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  async matchInt(tap: ReadableTapLike): Promise<number> {
    return await this.matchLong(tap);
  }

  /**
   * Compares the next zig-zag encoded 64-bit integer with another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  async matchLong(tap: ReadableTapLike): Promise<number> {
    const n1 = await this.readLong();
    const n2 = await tap.readLong();
    return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
  }

  /**
   * Compares the next 32-bit float value with another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  async matchFloat(tap: ReadableTapLike): Promise<number> {
    const n1 = await this.readFloat();
    const n2 = await tap.readFloat();
    return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
  }

  /**
   * Compares the next 64-bit float value with another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  async matchDouble(tap: ReadableTapLike): Promise<number> {
    const n1 = await this.readDouble();
    const n2 = await tap.readDouble();
    return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
  }

  /**
   * Compares fixed-length byte sequences from this tap and another tap.
   * @param tap Tap to compare against; both cursors advance by `len`.
   * @param len Number of bytes to compare.
   * @returns Comparison result using -1/0/1 semantics.
   */
  async matchFixed(tap: ReadableTapLike, len: number): Promise<number> {
    const fixed1 = await this.readFixed(len);
    const fixed2 = await tap.readFixed(len);
    return compareUint8Arrays(fixed1, fixed2);
  }

  /**
   * Compares length-prefixed byte sequences from this tap and another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  async matchBytes(tap: ReadableTapLike): Promise<number> {
    return await this.matchString(tap);
  }

  /**
   * Compares length-prefixed UTF-8 strings read from this tap and another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  async matchString(tap: ReadableTapLike): Promise<number> {
    const l1 = bigIntToSafeNumber(
      await this.readLong(),
      "matchString length this",
    );
    const bytes1 = await this.readFixed(l1);
    const l2 = bigIntToSafeNumber(
      await tap.readLong(),
      "matchString length tap",
    );
    const bytes2 = await tap.readFixed(l2);
    return compareUint8Arrays(bytes1, bytes2);
  }
}

/**
 * Binary tap that exposes Avro-compatible write helpers on top of a writable buffer.
 *
 * **IMPORTANT - Concurrency Constraints:**
 *
 * WritableTap uses static shared buffers for optimal performance. As a result:
 *
 * 1. **Do NOT call write methods concurrently** across different WritableTap instances
 * 2. **Do NOT call write methods concurrently** on the same WritableTap instance
 *
 * Concurrent writes will result in data corruption due to shared buffer state.
 *
 * **Safe usage patterns:**
 * ```typescript
 * // ✅ SAFE: Sequential async writes
 * await tap1.writeInt(42);
 * await tap2.writeInt(43);
 *
 * // ❌ UNSAFE: Concurrent async writes
 * await Promise.all([
 *   tap1.writeInt(42),
 *   tap2.writeInt(43)
 * ]); // Data corruption!
 *
 * // ❌ UNSAFE: Interleaved calls (even if not concurrent)
 * const promise1 = tap1.writeInt(42);  // Starts, may yield
 * const promise2 = tap2.writeInt(43);  // Corrupts tap1's buffer!
 * await Promise.all([promise1, promise2]);
 * ```
 *
 * **Why this matters:**
 * The async write methods may yield control (via `await`) between setting up the
 * shared buffer and actually writing it. If another write starts during this window,
 * it will overwrite the shared buffer, corrupting the first write's data.
 */
export class WritableTap extends TapBase implements WritableTapLike {
  /** The writable buffer backing this tap. */
  private readonly buffer: IWritableBuffer;

  // Buffer pool optimization: Static shared buffers to avoid per-operation allocations
  // Reduces GC pressure and improves performance for hot serialization paths
  // WARNING: These shared buffers make concurrent writes unsafe - see class documentation
  private static floatBuffer = new ArrayBuffer(8);
  private static floatView = new DataView(WritableTap.floatBuffer);
  private static float32Bytes = new Uint8Array(
    WritableTap.floatBuffer,
    0,
    4,
  );
  private static float64Bytes = new Uint8Array(
    WritableTap.floatBuffer,
    0,
    8,
  );
  private static trueByte = new Uint8Array([1]);
  private static falseByte = new Uint8Array([0]);

  private static varintBufferLong = new Uint8Array(11); // Max 11 bytes for 64-bit long (2^70 needs 11 bytes)
  private static varintBufferInt = new Uint8Array(5); // Max 5 bytes for zigzag-encoded int32

  /**
   * Creates a new WritableTap instance.
   * @param buf The buffer to write to, either an ArrayBuffer or IWritableBuffer.
   * @param pos The initial position in the buffer (default 0).
   */
  constructor(buf: ArrayBuffer | IWritableBuffer, pos = 0) {
    assertValidPosition(pos, true);
    let buffer: IWritableBuffer;
    if (buf instanceof ArrayBuffer) {
      buffer = new InMemoryWritableBuffer(buf, pos);
    } else if (isIWritable(buf)) {
      buffer = buf;
    } else {
      throw new TypeError(
        "WritableTap requires an ArrayBuffer or IWritableBuffer.",
      );
    }
    super(pos);
    this.buffer = buffer;
  }

  /**
   * Appends raw bytes to the buffer and advances the cursor.
   * @param bytes The bytes to append.
   */
  private async appendRawBytes(bytes: Uint8Array): Promise<void> {
    if (bytes.length === 0) {
      return;
    }
    this.pos += bytes.length;
    await this.buffer.appendBytes(bytes);
  }

  /** Checks if the buffer is valid. */
  async isValid(): Promise<boolean> {
    return await this.buffer.isValid();
  }

  /**
   * Writes a boolean value as a single byte and advances the cursor.
   * @param value Boolean value to write.
   */
  async writeBoolean(value: boolean): Promise<void> {
    await this.appendRawBytes(
      value ? WritableTap.trueByte : WritableTap.falseByte,
    );
  }

  /**
   * Writes a zig-zag encoded 32-bit signed integer.
   * Uses a 32-bit zig-zag + varint path to avoid BigInt casts for performance.
   * Uses pre-allocated buffer to avoid allocations.
   *
   * In local benchmarks, this approach is approximately 27% faster than using BigInt operations.
   *
   * @param n Integer value to write.
   */
  async writeInt(n: number): Promise<void> {
    if (
      typeof n !== "number" ||
      !Number.isInteger(n) ||
      n < -2147483648 ||
      n > 2147483647
    ) {
      throw new RangeError(
        `Value ${n} out of range for Avro int (-2147483648..2147483647)`,
      );
    }
    // Zigzag encode to an unsigned 32-bit integer:
    // (n << 1) ^ (n >> 31)
    let value = ((n << 1) ^ (n >> 31)) >>> 0;
    const buf = WritableTap.varintBufferInt;
    let i = 0;
    while (value > 0x7f) {
      buf[i++] = (value & 0x7f) | 0x80;
      value >>>= 7;
    }
    buf[i++] = value;
    await this.appendRawBytes(buf.subarray(0, i));
  }

  /**
   * Writes a zig-zag encoded 64-bit signed integer.
   * Uses pre-allocated buffer to avoid allocations.
   *
   * Performance optimization: Uses BigInt.asUintN(64, ...) to signal to V8's Turbofan that
   * all operations are on 64-bit values, enabling int64 lowering optimizations.
   * See https://groups.google.com/g/v8-reviews/c/SF2tmxAUpB8 and
   * https://v8.dev/blog/bigint#optimization-considerations for details.
   *
   * In local benchmarks, this approach is approximately 31% faster than operations without
   * explicit 64-bit truncation.
   *
   * Note: Input values must be within the int64 range (-2^63 to 2^63-1) per Avro spec.
   * Values outside this range will be rejected with a RangeError.
   *
   * @param value BigInt value to write (must be valid int64).
   */
  async writeLong(value: bigint): Promise<void> {
    // Validate range: must be within int64 bounds
    const INT64_MIN = -9223372036854775808n; // -(2^63)
    const INT64_MAX = 9223372036854775807n; // 2^63 - 1

    if (value < INT64_MIN || value > INT64_MAX) {
      throw new RangeError(
        `Value ${value} out of range for Avro long (${INT64_MIN}..${INT64_MAX})`,
      );
    }

    // Zigzag encode: asUintN(64, ...) signals to V8 these are 64-bit operations
    let n: bigint;
    if (value < 0n) {
      n = BigInt.asUintN(64, ((-value) << 1n) - 1n);
    } else {
      n = BigInt.asUintN(64, value << 1n);
    }

    // Fast varint encoding using pre-allocated buffer
    let i = 0;
    const buf = WritableTap.varintBufferLong;
    while (n >= 0x80n) {
      buf[i++] = Number(n & 0x7fn) | 0x80;
      n = BigInt.asUintN(64, n >> 7n);
    }
    buf[i++] = Number(n);
    await this.appendRawBytes(buf.subarray(0, i));
  }

  /**
   * Writes a 32-bit little-endian floating point number.
   * Uses pre-allocated buffer to avoid allocations.
   * @param value Float to write.
   */
  async writeFloat(value: number): Promise<void> {
    WritableTap.floatView.setFloat32(0, value, true);
    await this.appendRawBytes(WritableTap.float32Bytes);
  }

  /**
   * Writes a 64-bit little-endian floating point number.
   * Uses pre-allocated buffer to avoid allocations.
   * @param value Double precision value to write.
   */
  async writeDouble(value: number): Promise<void> {
    WritableTap.floatView.setFloat64(0, value, true);
    await this.appendRawBytes(WritableTap.float64Bytes);
  }

  /**
   * Writes a fixed-length byte sequence from the provided buffer.
   * @param buf Source buffer to copy from.
   */
  async writeFixed(buf: Uint8Array): Promise<void> {
    if (buf.length === 0) {
      return;
    }
    await this.appendRawBytes(buf);
  }

  /**
   * Writes a length-prefixed byte sequence backed by the provided buffer.
   * @param buf Bytes to write.
   */
  async writeBytes(buf: Uint8Array): Promise<void> {
    const len = buf.length;
    // Use writeInt for lengths that fit in 32-bit range (avoids BigInt overhead)
    if (len <= 0x7FFFFFFF) {
      await this.writeInt(len);
    } else {
      await this.writeLong(BigInt(len));
    }
    await this.writeFixed(buf);
  }

  /**
   * Writes a length-prefixed UTF-8 string.
   * @param str String to encode and write.
   */
  async writeString(str: string): Promise<void> {
    const encoded = encode(str);
    const len = encoded.length;
    // Use writeInt for lengths that fit in 32-bit range (avoids BigInt overhead)
    if (len <= 0x7FFFFFFF) {
      await this.writeInt(len);
    } else {
      await this.writeLong(BigInt(len));
    }
    await this.writeFixed(encoded);
  }

  /**
   * Writes a binary string as raw bytes without a length prefix.
   * @param str Source string containing binary data.
   * @param len Number of bytes from the string to write.
   */
  async writeBinary(str: string, len: number): Promise<void> {
    if (len <= 0) {
      return;
    }
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = str.charCodeAt(i) & 0xff;
    }
    await this.appendRawBytes(bytes);
  }
}
