import { bigIntToSafeNumber } from "./conversion.ts";
import { compareUint8Arrays } from "./compare_bytes.ts";
import { readUIntLE } from "./read_uint_le.ts";
import { invert } from "./manipulate_bytes.ts";
import { decode, encode } from "./text_encoding.ts";
import type { IReadableBuffer, IWritableBuffer } from "./buffers/buffer.ts";
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
   * Returns the buffer contents from the start up to the current cursor.
   * @throws RangeError if the cursor has advanced past the buffer length.
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
   * @throws RangeError if the read would exceed the buffer.
   */
  readFloat(): Promise<number>;
  /**
   * Skips a 32-bit floating point value by advancing four bytes.
   */
  skipFloat(): void;
  /**
   * Reads a 64-bit little-endian floating point number.
   * @throws RangeError if the read would exceed the buffer.
   */
  readDouble(): Promise<number>;
  /**
   * Skips a 64-bit floating point value by advancing eight bytes.
   */
  skipDouble(): void;
  /**
   * Reads a fixed-length byte sequence into a new buffer.
   * @param len Number of bytes to read.
   * @throws RangeError if the read exceeds the buffer.
   */
  readFixed(len: number): Promise<Uint8Array>;
  /**
   * Skips a fixed-length byte sequence.
   * @param len Number of bytes to skip.
   */
  skipFixed(len: number): void;
  /**
   * Reads a length-prefixed byte sequence.
   * @throws RangeError if insufficient data remains.
   */
  readBytes(): Promise<Uint8Array>;
  /**
   * Skips a length-prefixed byte sequence.
   */
  skipBytes(): Promise<void>;
  /**
   * Reads a length-prefixed UTF-8 string.
   * @throws RangeError when the buffer is exhausted prematurely.
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
  /** Unpacks a long-encoded byte array. */
  unpackLongBytes(): Promise<Uint8Array>;
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
   * @param len Optional number of bytes to write; defaults to the buffer length.
   */
  writeFixed(buf: Uint8Array, len?: number): Promise<void>;
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
  /**
   * Encodes an 8-byte two's complement integer into zig-zag encoded varint bytes.
   * @param arr Buffer containing the 8-byte value to encode; reused during processing.
   */
  packLongBytes(arr: Uint8Array): Promise<void>;
}

function assertValidPosition(pos: number): void {
  if (
    !Number.isFinite(pos) || !Number.isInteger(pos) || pos < 0 ||
    Math.abs(pos) > Number.MAX_SAFE_INTEGER
  ) {
    throw new RangeError(
      "Tap position must be an integer within the safe number range.",
    );
  }
}

function isIReadableBuffer(value: unknown): value is IReadableBuffer {
  return typeof value === "object" && value !== null &&
    typeof (value as IReadableBuffer).read === "function";
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
      const probe = await this.buffer.read(this.pos, 0);
      return probe !== undefined;
    } catch (err) {
      if (err instanceof RangeError) {
        return false;
      }
      throw err;
    }
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
    if (bytes) {
      return bytes.slice();
    } else {
      return new Uint8Array();
    }
  }

  /**
   * Returns the buffer contents from the start up to the current cursor.
   * @throws RangeError if the cursor has advanced past the buffer length.
   */
  async getValue(): Promise<Uint8Array> {
    const bytes = await this.buffer.read(0, this.pos);
    if (!bytes) {
      throw new RangeError("Tap position exceeds buffer length.");
    }
    return bytes;
  }

  /** Retrieves the byte at the specified position in the buffer. */
  private async getByteAt(position: number): Promise<number> {
    const bytes = await this.buffer.read(position, 1);
    if (!bytes) {
      throw new RangeError("Attempt to read beyond buffer bounds.");
    }
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
   */
  async readInt(): Promise<number> {
    return bigIntToSafeNumber(await this.readLong(), "readInt value");
  }

  /**
   * Reads a variable-length zig-zag encoded 64-bit signed integer as bigint.
   */
  async readLong(): Promise<bigint> {
    let pos = this.pos;
    let shift = 0n;
    let result = 0n;
    let byte: number;

    do {
      byte = await this.getByteAt(pos++);
      result |= BigInt(byte & 0x7f) << shift;
      shift += 7n;
    } while ((byte & 0x80) !== 0 && shift < 70n);

    while ((byte & 0x80) !== 0) {
      byte = await this.getByteAt(pos++);
      result |= BigInt(byte & 0x7f) << shift;
      shift += 7n;
    }

    this.pos = pos;
    return (result >> 1n) ^ -(result & 1n);
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
   * @throws RangeError if the read would exceed the buffer.
   */
  async readFloat(): Promise<number> {
    const pos = this.pos;
    this.pos += 4;
    const bytes = await this.buffer.read(pos, 4);
    if (!bytes) {
      throw new RangeError("Attempt to read beyond buffer bounds.");
    }
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
   * @throws RangeError if the read would exceed the buffer.
   */
  async readDouble(): Promise<number> {
    const pos = this.pos;
    this.pos += 8;
    const bytes = await this.buffer.read(pos, 8);
    if (!bytes) {
      throw new RangeError("Attempt to read beyond buffer bounds.");
    }
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
   * @throws RangeError if the read exceeds the buffer.
   */
  async readFixed(len: number): Promise<Uint8Array> {
    const pos = this.pos;
    this.pos += len;
    const bytes = await this.buffer.read(pos, len);
    if (!bytes) {
      throw new RangeError("Attempt to read beyond buffer bounds.");
    }
    return bytes;
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
   * @throws RangeError if insufficient data remains.
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
   * @throws RangeError when the buffer is exhausted prematurely.
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

  /**
   * Decodes the next zig-zag encoded long into an 8-byte two's complement buffer.
   */
  async unpackLongBytes(): Promise<Uint8Array> {
    const res = new Uint8Array(8);
    let n = 0;
    let i = 0; // Byte index in target buffer.
    let j = 6; // Bit offset in current target buffer byte.
    let pos = this.pos;

    let b = await this.getByteAt(pos++);
    const neg = b & 1;
    res.fill(0);

    // Accumulate the zig-zag decoded bits into the 8-byte buffer, emitting a byte once filled.
    n |= (b & 0x7f) >> 1;
    while (b & 0x80) {
      b = await this.getByteAt(pos++);
      n |= (b & 0x7f) << j;
      j += 7;
      if (j >= 8) {
        j -= 8;
        res[i++] = n;
        n >>= 8;
      }
    }
    res[i] = n;

    if (neg) {
      // The low bit captured the sign; invert to restore the original negative value.
      invert(res, 8);
    }

    this.pos = pos;
    return res;
  }
}

/**
 * Binary tap that exposes Avro-compatible write helpers on top of a writable buffer.
 */
export class WritableTap extends TapBase implements WritableTapLike {
  /** The writable buffer backing this tap. */
  private readonly buffer: IWritableBuffer;

  /**
   * Creates a new WritableTap instance.
   * @param buf The buffer to write to, either an ArrayBuffer or IWritableBuffer.
   * @param pos The initial position in the buffer (default 0).
   */
  constructor(buf: ArrayBuffer | IWritableBuffer, pos = 0) {
    assertValidPosition(pos);
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
    await this.appendRawBytes(Uint8Array.of(value ? 1 : 0));
  }

  /**
   * Writes a zig-zag encoded 32-bit signed integer.
   * @param n Integer value to write.
   */
  async writeInt(n: number): Promise<void> {
    await this.writeLong(BigInt(n));
  }

  /**
   * Writes a zig-zag encoded 64-bit signed integer.
   * @param value BigInt value to write.
   */
  async writeLong(value: bigint): Promise<void> {
    let n = value;
    if (n < 0n) {
      n = ((-n) << 1n) - 1n;
    } else {
      n <<= 1n;
    }

    const bytes: number[] = [];
    while (n >= 0x80n) {
      bytes.push(Number(n & 0x7fn) | 0x80);
      n >>= 7n;
    }
    bytes.push(Number(n));
    await this.appendRawBytes(Uint8Array.from(bytes));
  }

  /**
   * Writes a 32-bit little-endian floating point number.
   * @param value Float to write.
   */
  async writeFloat(value: number): Promise<void> {
    const buffer = new ArrayBuffer(4);
    const view = new DataView(buffer);
    view.setFloat32(0, value, true);
    await this.appendRawBytes(new Uint8Array(buffer));
  }

  /**
   * Writes a 64-bit little-endian floating point number.
   * @param value Double precision value to write.
   */
  async writeDouble(value: number): Promise<void> {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setFloat64(0, value, true);
    await this.appendRawBytes(new Uint8Array(buffer));
  }

  /**
   * Writes a fixed-length byte sequence from the provided buffer.
   * @param buf Source buffer to copy from.
   * @param len Optional number of bytes to write; defaults to the buffer length.
   */
  async writeFixed(buf: Uint8Array, len?: number): Promise<void> {
    const length = len ?? buf.length;
    if (length === 0) {
      return;
    }
    await this.appendRawBytes(buf.slice(0, length));
  }

  /**
   * Writes a length-prefixed byte sequence backed by the provided buffer.
   * @param buf Bytes to write.
   */
  async writeBytes(buf: Uint8Array): Promise<void> {
    const len = buf.length;
    await this.writeLong(BigInt(len));
    await this.writeFixed(buf, len);
  }

  /**
   * Writes a length-prefixed UTF-8 string.
   * @param str String to encode and write.
   */
  async writeString(str: string): Promise<void> {
    const encoded = encode(str);
    const len = encoded.length;
    await this.writeLong(BigInt(len));
    await this.writeFixed(encoded, len);
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

  /**
   * Encodes an 8-byte two's complement integer into zig-zag encoded varint bytes.
   * @param arr Buffer containing the 8-byte value to encode; reused during processing.
   */
  async packLongBytes(arr: Uint8Array): Promise<void> {
    const bufView = new DataView(arr.buffer, arr.byteOffset, arr.byteLength);
    const neg = (bufView.getUint8(7) & 0x80) >> 7;
    let j = 1;
    let k = 0;
    let m = 3;
    let n: number;

    if (neg) {
      // Temporarily convert to magnitude so the zig-zag representation can strip the sign bit.
      invert(arr, 8);
      n = 1;
    } else {
      n = 0;
    }

    const parts = [
      readUIntLE(bufView, 0, 3),
      readUIntLE(bufView, 3, 3),
      readUIntLE(bufView, 6, 2),
    ];

    while (m && !parts[--m]) {
      /* skip trailing zeros */
    }

    const emitted: number[] = [];

    // Pack 24-bit chunks into the zig-zag bucket, flushing continuation bytes as needed.
    while (k < m) {
      n |= parts[k++] << j;
      j += 24;
      while (j > 7) {
        emitted.push((n & 0x7f) | 0x80);
        n >>= 7;
        j -= 7;
      }
    }

    n |= parts[m] << j;
    do {
      const byte = n & 0x7f;
      n >>= 7;
      if (n) {
        emitted.push(byte | 0x80);
      } else {
        emitted.push(byte);
      }
    } while (n);

    if (neg) {
      // Restore the original negative representation in the supplied buffer.
      invert(arr, 8);
    }

    await this.appendRawBytes(Uint8Array.from(emitted));
  }
}
