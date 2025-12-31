import { TapBase } from "./tap.ts";
import type { SyncWritableTapLike } from "./tap_sync.ts";
import { utf8ByteLength } from "./text_encoding.ts";

/**
 * A synchronous writable tap that counts encoded bytes without allocating buffers.
 *
 * Used for two-pass serialization where the first pass calculates the
 * exact buffer size needed, then the second pass writes to a pre-allocated
 * buffer. This avoids buffer reallocations and copies.
 *
 * @example
 * ```ts
 * // Pass 1: Calculate size
 * const countingTap = new SyncCountingWritableTap();
 * type.writeSync(countingTap, value);
 * const size = countingTap.getPos();
 *
 * // Pass 2: Write to exact-size buffer
 * const buffer = new ArrayBuffer(size);
 * const tap = new SyncWritableTap(buffer);
 * type.writeSync(tap, value);
 * ```
 */
export class SyncCountingWritableTap extends TapBase
  implements SyncWritableTapLike {
  /** Creates a new counting tap starting at position 0. */
  constructor() {
    super(0);
  }

  /** Returns whether the tap is valid (always true for counting tap). */
  isValid(): boolean {
    return true;
  }

  /** Counts a boolean value (1 byte). */
  writeBoolean(_value: boolean): void {
    this.pos += 1;
  }

  /** Counts an integer value using varint encoding. */
  writeInt(value: number): void {
    // Calculate varint size for zigzag-encoded 32-bit int
    let n = ((value << 1) ^ (value >> 31)) >>> 0;
    let size = 1;
    while (n > 0x7f) {
      size++;
      n >>>= 7;
    }
    this.pos += size;
  }

  /** Counts a long value using zigzag + varint encoding. */
  writeLong(value: bigint): void {
    // Calculate varint size for zigzag-encoded value
    let n = value < 0n ? ((-value) << 1n) - 1n : value << 1n;
    let size = 1;
    while (n >= 0x80n) {
      size++;
      n >>= 7n;
    }
    this.pos += size;
  }

  /** Counts a float value (4 bytes). */
  writeFloat(_value: number): void {
    this.pos += 4;
  }

  /** Counts a double value (8 bytes). */
  writeDouble(_value: number): void {
    this.pos += 8;
  }

  /** Counts a fixed-length byte sequence. */
  writeFixed(buf: Uint8Array): void {
    this.pos += buf.length;
  }

  /** Counts a length-prefixed byte sequence. */
  writeBytes(buf: Uint8Array): void {
    const len = buf.length;
    this.writeLong(BigInt(len));
    this.pos += len;
  }

  /** Counts a length-prefixed UTF-8 string. */
  writeString(str: string): void {
    const len = utf8ByteLength(str);
    this.writeLong(BigInt(len));
    this.pos += len;
  }

  /** Counts raw binary bytes without length prefix. */
  writeBinary(_str: string, len: number): void {
    if (len > 0) this.pos += len;
  }
}
