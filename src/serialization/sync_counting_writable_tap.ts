import { TapBase } from "./tap.ts";
import type { SyncWritableTapLike } from "./sync_tap.ts";
import { encode } from "./text_encoding.ts";

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
  constructor() {
    super(0);
  }

  isValid(): boolean {
    return true;
  }

  writeBoolean(_value: boolean): void {
    this.pos += 1;
  }

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

  writeFloat(_value: number): void {
    this.pos += 4;
  }

  writeDouble(_value: number): void {
    this.pos += 8;
  }

  writeFixed(buf: Uint8Array): void {
    this.pos += buf.length;
  }

  writeBytes(buf: Uint8Array): void {
    const len = buf.length;
    this.writeLong(BigInt(len));
    this.pos += len;
  }

  writeString(str: string): void {
    const len = encode(str).length;
    this.writeLong(BigInt(len));
    this.pos += len;
  }

  writeBinary(_str: string, len: number): void {
    if (len > 0) this.pos += len;
  }
}
