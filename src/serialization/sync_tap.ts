import { bigIntToSafeNumber } from "./conversion.ts";
import { compareUint8Arrays } from "./compare_bytes.ts";
import { decode, encode } from "./text_encoding.ts";
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
export class SyncWritableTap extends TapBase implements SyncWritableTapLike {
  private readonly buffer: ISyncWritable;

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

  private appendRawBytes(bytes: Uint8Array): void {
    if (bytes.length === 0) return;
    this.buffer.appendBytes(bytes);
    this.pos += bytes.length;
  }

  writeBoolean(value: boolean): void {
    if (value) {
      this.appendRawBytes(Uint8Array.of(1));
    } else {
      this.appendRawBytes(Uint8Array.of(0));
    }
  }

  writeInt(value: number): void {
    this.writeLong(BigInt(value));
  }

  writeLong(value: bigint): void {
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
    this.appendRawBytes(Uint8Array.from(bytes));
  }

  writeFloat(value: number): void {
    const buffer = new ArrayBuffer(4);
    const view = new DataView(buffer);
    view.setFloat32(0, value, true);
    this.appendRawBytes(new Uint8Array(buffer));
  }

  writeDouble(value: number): void {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setFloat64(0, value, true);
    this.appendRawBytes(new Uint8Array(buffer));
  }

  writeFixed(buf: Uint8Array): void {
    if (buf.length === 0) return;
    this.appendRawBytes(buf);
  }

  writeBytes(buf: Uint8Array): void {
    const len = buf.length;
    this.writeLong(BigInt(len));
    this.writeFixed(buf);
  }

  writeString(str: string): void {
    const encoded = encode(str);
    const len = encoded.length;
    this.writeLong(BigInt(len));
    this.writeFixed(encoded);
  }

  writeBinary(str: string, len: number): void {
    if (len <= 0) return;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = str.charCodeAt(i) & 0xff;
    }
    this.appendRawBytes(bytes);
  }
}
