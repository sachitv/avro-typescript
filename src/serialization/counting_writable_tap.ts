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
  constructor() {
    super(0);
  }

  isValid(): Promise<boolean> {
    return Promise.resolve(true);
  }

  writeBoolean(_value: boolean): Promise<void> {
    this.pos += 1;
    return Promise.resolve();
  }

  writeInt(value: number): Promise<void> {
    return this.writeLong(BigInt(value));
  }

  writeLong(value: bigint): Promise<void> {
    // Calculate varint size for zigzag-encoded value
    let n = value < 0n ? ((-value) << 1n) - 1n : value << 1n;
    let size = 1;
    while (n >= 0x80n) {
      size++;
      n >>= 7n;
    }
    this.pos += size;
    return Promise.resolve();
  }

  writeFloat(_value: number): Promise<void> {
    this.pos += 4;
    return Promise.resolve();
  }

  writeDouble(_value: number): Promise<void> {
    this.pos += 8;
    return Promise.resolve();
  }

  writeFixed(buf: Uint8Array): Promise<void> {
    this.pos += buf.length;
    return Promise.resolve();
  }

  writeBytes(buf: Uint8Array): Promise<void> {
    const len = buf.length;
    // Length prefix + data
    return this.writeLong(BigInt(len)).then(() => {
      this.pos += len;
    });
  }

  writeString(str: string): Promise<void> {
    // Calculate UTF-8 byte length
    const len = utf8ByteLength(str);
    return this.writeLong(BigInt(len)).then(() => {
      this.pos += len;
    });
  }

  writeBinary(_str: string, len: number): Promise<void> {
    if (len > 0) this.pos += len;
    return Promise.resolve();
  }
}
