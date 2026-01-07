import { TapBase, type WritableTapLike } from "./tap.ts";
import { utf8ByteLength } from "./text_encoding.ts";

/**
 * A writable tap that counts encoded bytes without allocating buffers.
 *
 * Used for two-pass serialization where the first pass calculates the
 * exact buffer size needed, then the second pass writes to a pre-allocated
 * buffer. This avoids buffer reallocations and copies.
 *
 * @example
 * ```ts
 * // Pass 1: Calculate size
 * const countingTap = new CountingWritableTap();
 * await type.write(countingTap, value);
 * const size = countingTap.getPos();
 *
 * // Pass 2: Write to exact-size buffer
 * const buffer = new ArrayBuffer(size);
 * const tap = new WritableTap(buffer);
 * await type.write(tap, value);
 * ```
 */
export class CountingWritableTap extends TapBase implements WritableTapLike {
  /** Creates a new counting tap starting at position 0. */
  constructor() {
    super(0);
  }

  /** Returns whether the tap is valid (always true for counting tap). */
  isValid(): Promise<boolean> {
    return Promise.resolve(true);
  }

  /** Counts a boolean value (1 byte). */
  writeBoolean(_value: boolean): Promise<void> {
    this.pos += 1;
    return Promise.resolve();
  }

  /** Counts an integer value using varint encoding. */
  writeInt(value: number): Promise<void> {
    return this.writeLong(BigInt(value));
  }

  /** Counts a long value using zigzag + varint encoding. */
  writeLong(value: bigint): Promise<void> {
    let n: bigint;
    if (value < 0n) {
      const absVal = BigInt.asUintN(64, -value);
      const shifted = BigInt.asUintN(64, absVal << 1n);
      n = BigInt.asUintN(64, shifted - 1n);
    } else {
      n = BigInt.asUintN(64, value << 1n);
    }
    let size = 1;
    while (n >= 0x80n) {
      size++;
      n = BigInt.asUintN(64, n >> 7n);
    }
    this.pos += size;
    return Promise.resolve();
  }

  /** Counts a float value (4 bytes). */
  writeFloat(_value: number): Promise<void> {
    this.pos += 4;
    return Promise.resolve();
  }

  /** Counts a double value (8 bytes). */
  writeDouble(_value: number): Promise<void> {
    this.pos += 8;
    return Promise.resolve();
  }

  /** Counts a fixed-length byte sequence. */
  writeFixed(buf: Uint8Array): Promise<void> {
    this.pos += buf.length;
    return Promise.resolve();
  }

  /** Counts a length-prefixed byte sequence. */
  writeBytes(buf: Uint8Array): Promise<void> {
    const len = buf.length;
    // Length prefix + data
    return this.writeLong(BigInt(len)).then(() => {
      this.pos += len;
    });
  }

  /** Counts a length-prefixed UTF-8 string. */
  writeString(str: string): Promise<void> {
    // Calculate UTF-8 byte length
    const len = utf8ByteLength(str);
    return this.writeLong(BigInt(len)).then(() => {
      this.pos += len;
    });
  }

  /** Counts raw binary bytes without length prefix. */
  writeBinary(_str: string, len: number): Promise<void> {
    if (len > 0) this.pos += len;
    return Promise.resolve();
  }
}
