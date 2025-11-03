import { bigIntToSafeNumber } from "./conversion.ts";
import { getClampedLength } from "./clamp.ts";
import { compareByteRanges, compareUint8Arrays } from "./compare_bytes.ts";
import { readUIntLE } from "./read_uint_le.ts";
import { invert } from "./manipulate_bytes.ts";
import { decode, encode } from "./text_encoding.ts";

/**
 * Binary tap that provides Avro-compatible read/write operations on a backing buffer.
 * The tap tracks an internal cursor and exposes convenience helpers for primitive types.
 */
export class Tap {
  #buf: Uint8Array;
  #pos: number;
  #view: DataView;

  /**
   * Creates a new tap for the given buffer and optional start position.
   * @param buf Buffer the tap will operate on.
   * @param pos Byte offset where the tap cursor should start.
   */
  constructor(buf: ArrayBuffer, pos = 0) {
    if (!(buf instanceof ArrayBuffer)) {
      throw new TypeError("Tap requires an ArrayBuffer.");
    }
    if (
      !Number.isFinite(pos) || !Number.isInteger(pos) || pos < 0 ||
      Math.abs(pos) > Number.MAX_SAFE_INTEGER
    ) {
      throw new RangeError(
        "Tap position must be an integer within the safe number range.",
      );
    }

    this.#buf = new Uint8Array(buf);
    this.#view = new DataView(buf);
    this.#pos = pos;
  }

  /**
   * Returns a defensive copy of the current buffer for testing purposes.
   */
  get _testOnlyBuf(): Uint8Array {
    return this.#buf.slice();
  }

  /**
   * Returns the current cursor position within the buffer for testing.
   */
  get _testOnlyPos(): number {
    return this.#pos;
  }

  /**
   * Resets the cursor to the beginning of the buffer.
   */
  resetPos(): void {
    this.#pos = 0;
  }

  private getByteAt(position: number): number {
    return position < this.#view.byteLength ? this.#view.getUint8(position) : 0;
  }

  private setByteAt(position: number, value: number): void {
    if (position < this.#view.byteLength) {
      this.#view.setUint8(position, value & 0xff);
    }
  }

  /**
   * Returns whether the cursor is positioned within the buffer bounds.
   */
  isValid(): boolean {
    return this.#pos <= this.#view.byteLength;
  }

  /**
   * Returns the buffer contents from the start up to the current cursor.
   * @throws RangeError if the cursor has advanced past the buffer length.
   */
  getValue(): Uint8Array {
    if (this.#pos > this.#view.byteLength) {
      throw new RangeError("Tap position exceeds buffer length.");
    }
    return new Uint8Array(this.#view.buffer, 0, this.#pos);
  }

  /**
   * Reads the next byte as a boolean value and advances the cursor by one byte.
   */
  readBoolean(): boolean {
    const value = this.getByteAt(this.#pos);
    this.#pos += 1;
    return !!value;
  }

  /**
   * Skips a boolean value by advancing the cursor by one byte.
   */
  skipBoolean(): void {
    this.#pos++;
  }

  /**
   * Writes a boolean value as a single byte and advances the cursor.
   * @param value Boolean value to write.
   */
  writeBoolean(value: boolean): void {
    this.setByteAt(this.#pos, value ? 1 : 0);
    this.#pos += 1;
  }

  /**
   * Reads a variable-length zig-zag encoded 32-bit signed integer.
   */
  readInt(): number {
    return bigIntToSafeNumber(this.readLong(), "readInt value");
  }

  /**
   * Reads a variable-length zig-zag encoded 64-bit signed integer as bigint.
   */
  readLong(): bigint {
    let pos = this.#pos;
    let shift = 0n;
    let result = 0n;
    let byte: number;

    do {
      byte = this.getByteAt(pos++);
      result |= BigInt(byte & 0x7f) << shift;
      shift += 7n;
    } while ((byte & 0x80) !== 0 && shift < 70n);

    while ((byte & 0x80) !== 0) {
      byte = this.getByteAt(pos++);
      result |= BigInt(byte & 0x7f) << shift;
      shift += 7n;
    }

    this.#pos = pos;
    return (result >> 1n) ^ -(result & 1n);
  }

  /**
   * Skips a zig-zag encoded 32-bit integer by delegating to `skipLong`.
   */
  skipInt(): void {
    this.skipLong();
  }

  /**
   * Skips a zig-zag encoded 64-bit integer, advancing past continuation bytes.
   */
  skipLong(): void {
    let pos = this.#pos;
    while (this.getByteAt(pos++) & 0x80) {
      /* no-op */
    }
    this.#pos = pos;
  }

  /**
   * Writes a zig-zag encoded 32-bit signed integer.
   * @param n Integer value to write.
   */
  writeInt(n: number): void {
    this.writeLong(BigInt(n));
  }

  /**
   * Writes a zig-zag encoded 64-bit signed integer.
   * @param value BigInt value to write.
   */
  writeLong(value: bigint): void {
    let n = value;
    if (n < 0n) {
      n = ((-n) << 1n) - 1n;
    } else {
      n <<= 1n;
    }

    do {
      const byte = Number(n & 0x7fn);
      n >>= 7n;
      if (n !== 0n) {
        this.setByteAt(this.#pos, byte | 0x80);
      } else {
        this.setByteAt(this.#pos, byte);
      }
      this.#pos += 1;
    } while (n !== 0n);
  }

  /**
   * Reads a 32-bit little-endian floating point number.
   * @returns The float value or `undefined` if the read would exceed the buffer.
   */
  readFloat(): number | undefined {
    const pos = this.#pos;
    this.#pos += 4;
    if (this.#pos > this.#view.byteLength) {
      return undefined;
    }
    return this.#view.getFloat32(pos, true);
  }

  /**
   * Skips a 32-bit floating point value by advancing four bytes.
   */
  skipFloat(): void {
    this.#pos += 4;
  }

  /**
   * Writes a 32-bit little-endian floating point number.
   * @param value Float to write.
   */
  writeFloat(value: number): void {
    const pos = this.#pos;
    this.#pos += 4;
    if (this.#pos > this.#view.byteLength) {
      return;
    }
    this.#view.setFloat32(pos, value, true);
  }

  /**
   * Reads a 64-bit little-endian floating point number.
   * @returns The double value or `undefined` if the read would exceed the buffer.
   */
  readDouble(): number | undefined {
    const pos = this.#pos;
    this.#pos += 8;
    if (this.#pos > this.#view.byteLength) {
      return undefined;
    }
    return this.#view.getFloat64(pos, true);
  }

  /**
   * Skips a 64-bit floating point value by advancing eight bytes.
   */
  skipDouble(): void {
    this.#pos += 8;
  }

  /**
   * Writes a 64-bit little-endian floating point number.
   * @param value Double precision value to write.
   */
  writeDouble(value: number): void {
    const pos = this.#pos;
    this.#pos += 8;
    if (this.#pos > this.#view.byteLength) {
      return;
    }
    this.#view.setFloat64(pos, value, true);
  }

  /**
   * Reads a fixed-length byte sequence into a new buffer.
   * @param len Number of bytes to read.
   * @returns The bytes read or `undefined` if the read exceeds the buffer.
   */
  readFixed(len: number): Uint8Array | undefined {
    const pos = this.#pos;
    this.#pos += len;
    if (this.#pos > this.#view.byteLength) {
      return undefined;
    }
    const fixed = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      fixed[i] = this.#view.getUint8(pos + i);
    }
    return fixed;
  }

  /**
   * Skips a fixed-length byte sequence.
   * @param len Number of bytes to skip.
   */
  skipFixed(len: number): void {
    this.#pos += len;
  }

  /**
   * Writes a fixed-length byte sequence from the provided buffer.
   * @param buf Source buffer to copy from.
   * @param len Optional number of bytes to write; defaults to the buffer length.
   */
  writeFixed(buf: Uint8Array, len?: number): void {
    const length = len ?? buf.length;
    const pos = this.#pos;
    this.#pos += length;
    if (this.#pos > this.#view.byteLength) {
      return;
    }
    for (let i = 0; i < length; i++) {
      this.#view.setUint8(pos + i, buf[i]);
    }
  }

  /**
   * Reads a length-prefixed byte sequence.
   * @returns The bytes read or `undefined` if insufficient data remains.
   */
  readBytes(): Uint8Array | undefined {
    const length = bigIntToSafeNumber(this.readLong(), "readBytes length");
    return this.readFixed(length);
  }

  /**
   * Skips a length-prefixed byte sequence.
   */
  skipBytes(): void {
    const len = bigIntToSafeNumber(this.readLong(), "skipBytes length");
    this.#pos += len;
  }

  /**
   * Writes a length-prefixed byte sequence backed by the provided buffer.
   * @param buf Bytes to write.
   */
  writeBytes(buf: Uint8Array): void {
    const len = buf.length;
    this.writeLong(BigInt(len));
    this.writeFixed(buf, len);
  }

  /**
   * Reads a length-prefixed UTF-8 string.
   * @returns The decoded string or `undefined` when the buffer is exhausted prematurely.
   */
  readString(): string | undefined {
    const len = bigIntToSafeNumber(this.readLong(), "readString length");
    const bytes = this.readFixed(len);
    if (!bytes) {
      return undefined;
    }
    return decode(bytes);
  }

  /**
   * Skips a length-prefixed UTF-8 string.
   */
  skipString(): void {
    const len = bigIntToSafeNumber(this.readLong(), "skipString length");
    this.#pos += len;
  }

  /**
   * Writes a length-prefixed UTF-8 string.
   * @param str String to encode and write.
   */
  writeString(str: string): void {
    const encoded = encode(str);
    const len = encoded.length;
    this.writeLong(BigInt(len));
    this.writeFixed(encoded, len);
  }

  /**
   * Writes a binary string as raw bytes without a length prefix.
   * @param str Source string containing binary data.
   * @param len Number of bytes from the string to write.
   */
  writeBinary(str: string, len: number): void {
    const pos = this.#pos;
    this.#pos += len;
    if (this.#pos > this.#view.byteLength) {
      return;
    }
    for (let i = 0; i < len; i++) {
      this.#view.setUint8(pos + i, str.charCodeAt(i) & 0xff);
    }
  }

  /**
   * Compares the next boolean value with the one from another tap.
   * @param tap Tap to compare against; both cursors advance.
   * @returns 0 when equal, negative when this tap's value is false and the other true, positive otherwise.
   */
  matchBoolean(tap: Tap): number {
    const diff = this.getByteAt(this.#pos) - tap.getByteAt(tap.#pos);
    this.#pos += 1;
    tap.#pos += 1;
    return diff;
  }

  /**
   * Compares the next zig-zag encoded 32-bit integer with another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  matchInt(tap: Tap): number {
    return this.matchLong(tap);
  }

  /**
   * Compares the next zig-zag encoded 64-bit integer with another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  matchLong(tap: Tap): number {
    const n1 = this.readLong();
    const n2 = tap.readLong();
    return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
  }

  /**
   * Compares the next 32-bit float value with another tap.
   * @returns Comparison result using -1/0/1 semantics, or 0 when either side runs out of data.
   */
  matchFloat(tap: Tap): number {
    const n1 = this.readFloat();
    const n2 = tap.readFloat();
    if (n1 === undefined || n2 === undefined) {
      return 0;
    }
    return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
  }

  /**
   * Compares the next 64-bit float value with another tap.
   * @returns Comparison result using -1/0/1 semantics, or 0 when either side runs out of data.
   */
  matchDouble(tap: Tap): number {
    const n1 = this.readDouble();
    const n2 = tap.readDouble();
    if (n1 === undefined || n2 === undefined) {
      return 0;
    }
    return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
  }

  /**
   * Compares fixed-length byte sequences from this tap and another tap.
   * @param tap Tap to compare against; both cursors advance by `len`.
   * @param len Number of bytes to compare.
   * @returns Comparison result using -1/0/1 semantics, or 0 if either sequence is unavailable.
   */
  matchFixed(tap: Tap, len: number): number {
    const fixed1 = this.readFixed(len);
    const fixed2 = tap.readFixed(len);
    if (!fixed1 || !fixed2) {
      return 0;
    }
    return compareUint8Arrays(fixed1, fixed2);
  }

  /**
   * Compares length-prefixed byte sequences from this tap and another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  matchBytes(tap: Tap): number {
    return this.matchString(tap);
  }

  /**
   * Compares length-prefixed UTF-8 strings read from this tap and another tap.
   * @returns Comparison result using -1/0/1 semantics.
   */
  matchString(tap: Tap): number {
    const l1 = bigIntToSafeNumber(this.readLong(), "matchString length this");
    const p1 = this.#pos;
    this.#pos += l1;
    const l2 = bigIntToSafeNumber(tap.readLong(), "matchString length tap");
    const p2 = tap.#pos;
    tap.#pos += l2;
    const len1 = getClampedLength(this.#view.byteLength, p1, l1);
    const len2 = getClampedLength(tap.#view.byteLength, p2, l2);
    return compareByteRanges(this.#view, p1, len1, tap.#view, p2, len2);
  }

  /**
   * Decodes the next zig-zag encoded long into an 8-byte two's complement buffer.
   */
  unpackLongBytes(): Uint8Array {
    const res = new Uint8Array(8);
    let n = 0;
    let i = 0; // Byte index in target buffer.
    let j = 6; // Bit offset in current target buffer byte.
    let pos = this.#pos;

    let b = this.getByteAt(pos++);
    const neg = b & 1;
    res.fill(0);

    // Accumulate the zig-zag decoded bits into the 8-byte buffer, emitting a byte once filled.
    n |= (b & 0x7f) >> 1;
    while (b & 0x80) {
      b = this.getByteAt(pos++);
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

    this.#pos = pos;
    return res;
  }

  /**
   * Encodes an 8-byte two's complement integer into zig-zag encoded varint bytes.
   * @param arr Buffer containing the 8-byte value to encode; reused during processing.
   */
  packLongBytes(arr: Uint8Array): void {
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

    // Pack 24-bit chunks into the zig-zag bucket, flushing continuation bytes as needed.
    while (k < m) {
      n |= parts[k++] << j;
      j += 24;
      while (j > 7) {
        this.setByteAt(this.#pos, (n & 0x7f) | 0x80);
        this.#pos += 1;
        n >>= 7;
        j -= 7;
      }
    }

    n |= parts[m] << j;
    do {
      const byte = n & 0x7f;
      n >>= 7;
      if (n) {
        this.setByteAt(this.#pos, byte | 0x80);
        this.#pos += 1;
      } else {
        this.setByteAt(this.#pos, byte);
        this.#pos += 1;
      }
    } while (n);

    if (neg) {
      // Restore the original negative representation in the supplied buffer.
      invert(arr, 8);
    }
  }
}

export default Tap;
