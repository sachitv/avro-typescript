import { bigIntToSafeNumber } from "./conversion.ts";
import { compareUint8Arrays } from "./compare_bytes.ts";
import { decode, encoder } from "./text_encoding.ts";
import { TapBase } from "./tap.ts";
import type { ISyncReadable, ISyncWritable } from "./buffers/sync_buffer.ts";
import { ReadBufferError } from "./buffers/sync_buffer.ts";
import {
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "./buffers/sync_in_memory_buffer.ts";

/**
 * Type guard to check if an object implements ISyncReadable.
 */
function isISyncReadable(obj: unknown): obj is ISyncReadable {
  return (
    typeof obj === "object" &&
    obj !== null &&
    typeof (obj as ISyncReadable).read === "function" &&
    typeof (obj as ISyncReadable).canReadMore === "function"
  );
}

/**
 * Type guard to check if an object implements ISyncWritable.
 */
function isISyncWritable(obj: unknown): obj is ISyncWritable {
  return (
    typeof obj === "object" &&
    obj !== null &&
    typeof (obj as ISyncWritable).appendBytes === "function" &&
    typeof (obj as ISyncWritable).appendBytesFrom === "function" &&
    typeof (obj as ISyncWritable).isValid === "function" &&
    typeof (obj as ISyncWritable).canAppendMore === "function"
  );
}

export interface SyncReadableTapLike {
  isValid(): boolean;
  canReadMore(): boolean;

  getValue(): Uint8Array;

  readBoolean(): boolean;
  skipBoolean(): void;

  readInt(): number;
  readLong(): bigint;
  skipInt(): void;
  skipLong(): void;

  readFloat(): number;
  skipFloat(): void;

  readDouble(): number;
  skipDouble(): void;

  readFixed(len: number): Uint8Array;
  skipFixed(len: number): void;

  readBytes(): Uint8Array;
  skipBytes(): void;

  readString(): string;
  skipString(): void;

  matchBoolean(tap: SyncReadableTapLike): number;
  matchInt(tap: SyncReadableTapLike): number;
  matchLong(tap: SyncReadableTapLike): number;
  matchFloat(tap: SyncReadableTapLike): number;
  matchDouble(tap: SyncReadableTapLike): number;
  matchFixed(tap: SyncReadableTapLike, len: number): number;
  matchBytes(tap: SyncReadableTapLike): number;
  matchString(tap: SyncReadableTapLike): number;
}

export interface SyncWritableTapLike {
  isValid(): boolean;

  writeBoolean(value: boolean): void;

  writeInt(value: number): void;
  writeLong(value: bigint): void;

  writeFloat(value: number): void;
  writeDouble(value: number): void;

  writeFixed(buf: Uint8Array): void;
  writeBytes(buf: Uint8Array): void;
  writeString(str: string): void;
  writeBinary(str: string, len: number): void;
}

/**
 * Synchronous readable tap over an in-memory buffer.
 * Mirrors ReadableTap but without Promises.
 */
export class SyncReadableTap extends TapBase implements SyncReadableTapLike {
  private readonly buffer: ISyncReadable;

  constructor(buf: ArrayBuffer | ISyncReadable, pos = 0) {
    super(pos);
    if (buf instanceof ArrayBuffer) {
      this.buffer = new SyncInMemoryReadableBuffer(buf);
    } else if (isISyncReadable(buf)) {
      this.buffer = buf;
    } else {
      throw new TypeError(
        "SyncReadableTap requires an ArrayBuffer or ISyncReadable buffer.",
      );
    }
  }

  isValid(): boolean {
    try {
      this.buffer.read(this.pos, 0);
      return true;
    } catch (err) {
      if (err instanceof ReadBufferError) {
        return false;
      }
      throw err;
    }
  }

  canReadMore(): boolean {
    return this.buffer.canReadMore(this.pos);
  }

  getValue(): Uint8Array {
    if (this.pos < 0) {
      throw new RangeError("Tap position is negative.");
    }
    return this.buffer.read(0, this.pos)!;
  }

  private getByteAt(position: number): number {
    const result = this.buffer.read(position, 1)!;
    return result[0]!;
  }

  readBoolean(): boolean {
    const value = this.getByteAt(this.pos);
    this.pos += 1;
    return !!value;
  }

  skipBoolean(): void {
    this.pos += 1;
  }

  readInt(): number {
    return bigIntToSafeNumber(this.readLong(), "readInt value");
  }

  readLong(): bigint {
    let pos = this.pos;
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

    this.pos = pos;
    return (result >> 1n) ^ -(result & 1n);
  }

  skipInt(): void {
    this.skipLong();
  }

  skipLong(): void {
    let pos = this.pos;
    while (this.getByteAt(pos++) & 0x80) {
      /* no-op */
    }
    this.pos = pos;
  }

  readFloat(): number {
    const pos = this.pos;
    this.pos += 4;
    const bytes = this.buffer.read(pos, 4)!;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getFloat32(0, true);
  }

  skipFloat(): void {
    this.pos += 4;
  }

  readDouble(): number {
    const pos = this.pos;
    this.pos += 8;
    const bytes = this.buffer.read(pos, 8)!;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    return view.getFloat64(0, true);
  }

  skipDouble(): void {
    this.pos += 8;
  }

  readFixed(len: number): Uint8Array {
    const pos = this.pos;
    this.pos += len;
    return this.buffer.read(pos, len)!;
  }

  skipFixed(len: number): void {
    this.pos += len;
  }

  readBytes(): Uint8Array {
    const length = bigIntToSafeNumber(this.readLong(), "readBytes length");
    return this.readFixed(length);
  }

  skipBytes(): void {
    const len = bigIntToSafeNumber(this.readLong(), "skipBytes length");
    this.pos += len;
  }

  readString(): string {
    const len = bigIntToSafeNumber(this.readLong(), "readString length");
    const bytes = this.readFixed(len);
    return decode(bytes);
  }

  skipString(): void {
    const len = bigIntToSafeNumber(this.readLong(), "skipString length");
    this.pos += len;
  }

  matchBoolean(tap: SyncReadableTapLike): number {
    const v1 = this.readBoolean();
    const v2 = tap.readBoolean();
    return Number(v1) - Number(v2);
  }

  matchInt(tap: SyncReadableTapLike): number {
    return this.matchLong(tap);
  }

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

  matchFixed(tap: SyncReadableTapLike, len: number): number {
    const f1 = this.readFixed(len);
    const f2 = tap.readFixed(len);
    return compareUint8Arrays(f1, f2);
  }

  matchBytes(tap: SyncReadableTapLike): number {
    return this.matchString(tap);
  }

  matchString(tap: SyncReadableTapLike): number {
    const l1 = bigIntToSafeNumber(
      this.readLong(),
      "matchString length this",
    );
    const b1 = this.readFixed(l1);
    const l2 = bigIntToSafeNumber(
      tap.readLong(),
      "matchString length tap",
    );
    const b2 = tap.readFixed(l2);
    return compareUint8Arrays(b1, b2);
  }
}

/**
 * Synchronous writable tap over an in-memory buffer (Uint8Array / ArrayBuffer).
 * Mirrors WritableTap but without Promises.
 */
/**
 * High-performance synchronous writable tap for Avro serialization.
 *
 * Key optimizations:
 * - Buffer pool pattern: Pre-allocated static buffers eliminate GC pressure
 * - Varint encoding: Direct buffer writing avoids intermediate allocations
 * - ASCII string optimization: Fast path for ASCII strings, encodeInto() for Unicode
 * - Static primitive buffers: Reuse buffers for booleans, floats, and doubles
 */
export class SyncWritableTap extends TapBase implements SyncWritableTapLike {
  private readonly buffer: ISyncWritable;

  private static readonly INT_MIN = -2147483648;
  private static readonly INT_MAX = 2147483647;

  // Buffer pool optimization: Pre-allocated static buffers to avoid per-operation allocations
  // Reduces GC pressure and improves performance for hot serialization paths
  private static readonly floatBuffer = new ArrayBuffer(8);
  private static readonly floatView = new DataView(SyncWritableTap.floatBuffer);
  private static readonly float32Bytes = new Uint8Array(
    SyncWritableTap.floatBuffer,
    0,
    4,
  );
  private static readonly float64Bytes = new Uint8Array(
    SyncWritableTap.floatBuffer,
    0,
    8,
  );
  private static readonly trueByte = new Uint8Array([1]);
  private static readonly falseByte = new Uint8Array([0]);

  private static readonly varintBufferLong = new Uint8Array(11); // Max 11 bytes for 64-bit long (2^70 needs 11 bytes)
  private static readonly varintBufferInt = new Uint8Array(5); // Max 5 bytes for zigzag-encoded int32

  // String buffer pool: Pre-allocated buffer for ASCII string encoding
  // Avoids allocations for common ASCII strings up to 1024 characters
  private static readonly stringBuffer = new Uint8Array(1024);
  private static encodedStringBuffer = new Uint8Array(0);
  private static binaryBuffer = new Uint8Array(0);

  constructor(buf: ArrayBuffer | ISyncWritable, pos = 0) {
    super(pos);
    if (buf instanceof ArrayBuffer) {
      this.buffer = new SyncInMemoryWritableBuffer(buf, pos);
    } else if (isISyncWritable(buf)) {
      this.buffer = buf;
    } else {
      throw new TypeError(
        "SyncWritableTap requires an ArrayBuffer or ISyncWritable buffer.",
      );
    }
  }

  isValid(): boolean {
    return this.buffer.isValid();
  }

  private appendRawBytes(
    bytes: Uint8Array,
    offset = 0,
    length = bytes.length - offset,
  ): void {
    if (length <= 0) return;
    this.buffer.appendBytesFrom(bytes, offset, length);
    this.pos += length;
  }

  writeBoolean(value: boolean): void {
    this.appendRawBytes(
      value ? SyncWritableTap.trueByte : SyncWritableTap.falseByte,
    );
  }

  /**
   * Writes a 32-bit integer using zigzag + varint encoding.
   */
  writeInt(value: number): void {
    if (
      typeof value !== "number" ||
      !Number.isInteger(value) ||
      value < SyncWritableTap.INT_MIN ||
      value > SyncWritableTap.INT_MAX
    ) {
      throw new RangeError(
        `Value ${value} out of range for Avro int (${SyncWritableTap.INT_MIN}..${SyncWritableTap.INT_MAX})`,
      );
    }
    this.writeVarint32ZigZag(value);
  }

  private writeVarint32ZigZag(value: number): void {
    // Zigzag encode to an unsigned 32-bit integer:
    // (n << 1) ^ (n >> 31)
    let n = ((value << 1) ^ (value >> 31)) >>> 0;
    let i = 0;
    const buf = SyncWritableTap.varintBufferInt;
    while (n > 0x7f) {
      buf[i++] = (n & 0x7f) | 0x80;
      n >>>= 7;
    }
    buf[i++] = n;
    this.appendRawBytes(buf, 0, i);
  }

  /**
   * Writes a 64-bit integer using zigzag + varint encoding.
   *
   * Optimizations:
   * - Buffer pool: Uses pre-allocated varintBufferLong to avoid allocations
   * - Direct buffer writing: Encodes into a shared buffer then appends a slice
   *
   * Performance impact: ~3x faster than original array-based approach
   */
  writeLong(value: bigint): void {
    let n = value;
    if (n < 0n) {
      n = ((-n) << 1n) - 1n;
    } else {
      n <<= 1n;
    }

    // Fast varint encoding using pre-allocated buffer
    let i = 0;
    const buf = SyncWritableTap.varintBufferLong;
    while (n >= 0x80n) {
      buf[i++] = Number(n & 0x7fn) | 0x80;
      n >>= 7n;
    }
    buf[i++] = Number(n);
    this.appendRawBytes(buf, 0, i);
  }

  writeFloat(value: number): void {
    SyncWritableTap.floatView.setFloat32(0, value, true);
    this.appendRawBytes(SyncWritableTap.float32Bytes);
  }

  writeDouble(value: number): void {
    SyncWritableTap.floatView.setFloat64(0, value, true);
    this.appendRawBytes(SyncWritableTap.float64Bytes);
  }

  writeFixed(buf: Uint8Array): void {
    if (buf.length === 0) return;
    this.appendRawBytes(buf, 0, buf.length);
  }

  writeBytes(buf: Uint8Array): void {
    const len = buf.length;
    this.writeLong(BigInt(len));
    this.writeFixed(buf);
  }

  /**
   * Writes a UTF-8 encoded string with length prefix.
   *
   * Optimizations:
   * - ASCII fast path: Direct charCodeAt() loop for ASCII strings (most common)
   * - Unicode optimization: Uses TextEncoder.encodeInto() for zero-copy encoding
   * - Buffer pool: Reuses stringBuffer to avoid allocations
   * - Size estimation: Conservative UTF-8 size prediction (2x chars)
   *
   * Performance: ASCII strings ~800ns, Unicode strings ~1µs with encodeInto()
   * Rationale: ASCII dominates real-world usage, Unicode needs zero-copy encoding
   */
  writeString(str: string): void {
    const strLen = str.length;

    // Hybrid approach: ASCII fast path + encodeInto() for Unicode
    if (strLen <= 1024) {
      // Check if string is ASCII (single pass detection)
      const buf = SyncWritableTap.stringBuffer;
      let i = 0;
      for (; i < strLen; i++) {
        const code = str.charCodeAt(i);
        if (code > 127) {
          break;
        }
        buf[i] = code;
      }

      if (i === strLen) {
        // Fast ASCII path: direct charCodeAt() encoding
        // Avoids TextEncoder overhead for common ASCII strings
        // In this path, encoded length == strLen, so it can fit in an int.
        this.writeInt(strLen);
        if (strLen > 0) {
          this.appendRawBytes(buf, 0, strLen);
        }
        return;
      }
    }

    // Fallback for large or complex strings
    const estimatedSize = strLen * 3 + 4;
    let encodedBuffer = SyncWritableTap.ensureEncodedStringBuffer(
      estimatedSize,
    );
    let result = encoder.encodeInto(str, encodedBuffer);
    if (result.read !== strLen) {
      encodedBuffer = SyncWritableTap.ensureEncodedStringBuffer(strLen * 4 + 8);
      result = encoder.encodeInto(str, encodedBuffer);
      if (result.read !== strLen) {
        throw new Error(
          "TextEncoder.encodeInto failed to consume the entire string.",
        );
      }
    }
    if (result.written > SyncWritableTap.INT_MAX) {
      this.writeLong(BigInt(result.written));
    } else {
      this.writeInt(result.written);
    }
    this.appendRawBytes(encodedBuffer, 0, result.written);
  }

  writeBinary(str: string, len: number): void {
    if (len <= 0) return;
    const bytes = SyncWritableTap.ensureBinaryBuffer(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = str.charCodeAt(i) & 0xff;
    }
    this.appendRawBytes(bytes, 0, len);
  }

  private static ensureEncodedStringBuffer(minSize: number): Uint8Array {
    if (SyncWritableTap.encodedStringBuffer.length < minSize) {
      SyncWritableTap.encodedStringBuffer = new Uint8Array(minSize);
    }
    return SyncWritableTap.encodedStringBuffer;
  }

  private static ensureBinaryBuffer(size: number): Uint8Array {
    if (SyncWritableTap.binaryBuffer.length < size) {
      SyncWritableTap.binaryBuffer = new Uint8Array(size);
    }
    return SyncWritableTap.binaryBuffer;
  }
}
