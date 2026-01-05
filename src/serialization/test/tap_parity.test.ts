import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { assertRejects, assertThrows } from "@std/assert";

import { ReadableTap, WritableTap } from "../tap.ts";
import { SyncReadableTap, SyncWritableTap } from "../tap_sync.ts";
import {
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "../buffers/in_memory_buffer_sync.ts";
import { ReadBufferError, WriteBufferError } from "../buffers/buffer_sync.ts";
import { encoder } from "../text_encoding.ts";

const toUint8Array = (values: number[]): Uint8Array =>
  Uint8Array.from(values.map((value) => ((value % 256) + 256) % 256));

const bytesOf = (buffer: ArrayBuffer): Uint8Array => new Uint8Array(buffer);

class LenientWritableBuffer implements ISyncWritable {
  #pos = 0;

  appendBytes(data: Uint8Array): void {
    this.#pos += data.length;
  }

  appendBytesFrom(_data: Uint8Array, _offset: number, length: number): void {
    this.#pos += length;
  }

  isValid(): boolean {
    return true;
  }

  canAppendMore(_size: number): boolean {
    return true;
  }

  getPos(): number {
    return this.#pos;
  }
}

import type { ISyncWritable } from "../buffers/buffer_sync.ts";

describe("SyncWritableTap vs WritableTap parity", () => {
  it("writeLong encodes identical bytes as WritableTap", async () => {
    const size = 64;
    const values = [
      0n,
      -1n,
      109213n,
      -1211n,
      -1312411211n,
      900719925474090n,
    ];

    for (const v of values) {
      const asyncBuf = new ArrayBuffer(size);
      const syncBuf = new ArrayBuffer(size);
      const asyncTap = new WritableTap(asyncBuf);
      const syncTap = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(syncBuf),
      );

      await asyncTap.writeLong(v);
      syncTap.writeLong(v);

      expect(bytesOf(asyncBuf)).toEqual(bytesOf(syncBuf));
      expect(asyncTap.getPos()).toBe(syncTap.getPos());
    }
  });

  it("writeInt encodes identical bytes", async () => {
    const size = 64;
    const values = [0, -1, 42, -1234567, 2147483647, -2147483648];

    for (const v of values) {
      const asyncBuf = new ArrayBuffer(size);
      const syncBuf = new ArrayBuffer(size);
      const asyncTap = new WritableTap(asyncBuf);
      const syncTap = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(syncBuf),
      );

      await asyncTap.writeInt(v);
      syncTap.writeInt(v);

      expect(bytesOf(asyncBuf)).toEqual(bytesOf(syncBuf));
      expect(asyncTap.getPos()).toBe(syncTap.getPos());
    }
  });

  it("writeBoolean encodes identical bytes", async () => {
    const size = 8;
    const values = [true, false];

    for (const v of values) {
      const asyncBuf = new ArrayBuffer(size);
      const syncBuf = new ArrayBuffer(size);
      const asyncTap = new WritableTap(asyncBuf);
      const syncTap = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(syncBuf),
      );

      await asyncTap.writeBoolean(v);
      syncTap.writeBoolean(v);

      expect(bytesOf(asyncBuf)).toEqual(bytesOf(syncBuf));
      expect(asyncTap.getPos()).toBe(syncTap.getPos());
    }
  });

  it("writeFloat and writeDouble encode identical bytes", async () => {
    const floats = [1, 3.1, -5, 1e9];
    const doubles = [1, 3.1, -5, 1e12];

    for (const v of floats) {
      const asyncBuf = new ArrayBuffer(32);
      const syncBuf = new ArrayBuffer(32);
      const asyncTap = new WritableTap(asyncBuf);
      const syncTap = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(syncBuf),
      );

      await asyncTap.writeFloat(v);
      syncTap.writeFloat(v);

      expect(bytesOf(asyncBuf)).toEqual(bytesOf(syncBuf));
      expect(asyncTap.getPos()).toBe(syncTap.getPos());
    }

    for (const v of doubles) {
      const asyncBuf = new ArrayBuffer(32);
      const syncBuf = new ArrayBuffer(32);
      const asyncTap = new WritableTap(asyncBuf);
      const syncTap = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(syncBuf),
      );

      await asyncTap.writeDouble(v);
      syncTap.writeDouble(v);

      expect(bytesOf(asyncBuf)).toEqual(bytesOf(syncBuf));
      expect(asyncTap.getPos()).toBe(syncTap.getPos());
    }
  });

  it("writeBytes and writeString encode identical bytes", async () => {
    const byteCases = [
      toUint8Array([0x61, 0x62, 0x63]),
      new Uint8Array(0),
      toUint8Array([1, 5, 255]),
    ];
    const stringCases = ["ahierw", "", "alh hewlii! rew"];

    for (const buf of byteCases) {
      const asyncBuf = new ArrayBuffer(128);
      const syncBuf = new ArrayBuffer(128);
      const asyncTap = new WritableTap(asyncBuf);
      const syncTap = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(syncBuf),
      );

      await asyncTap.writeBytes(buf);
      syncTap.writeBytes(buf);

      expect(bytesOf(asyncBuf)).toEqual(bytesOf(syncBuf));
      expect(asyncTap.getPos()).toBe(syncTap.getPos());
    }

    for (const str of stringCases) {
      const asyncBuf = new ArrayBuffer(128);
      const syncBuf = new ArrayBuffer(128);
      const asyncTap = new WritableTap(asyncBuf);
      const syncTap = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(syncBuf),
      );

      await asyncTap.writeString(str);
      syncTap.writeString(str);

      expect(bytesOf(asyncBuf)).toEqual(bytesOf(syncBuf));
      expect(asyncTap.getPos()).toBe(syncTap.getPos());
    }

    // Test Unicode string to cover encodeInto() path in writeString()
    // This ensures the Unicode branch of the ASCII/Unicode hybrid optimization is tested
    // Uses Japanese proverb "七転び八起き" (Fall seven times, stand up eight) for cultural relevance
    const unicodeStr = "七転び八起き";
    const asyncBufUnicode = new ArrayBuffer(128);
    const syncBufUnicode = new ArrayBuffer(128);
    const asyncTapUnicode = new WritableTap(asyncBufUnicode);
    const syncTapUnicode = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(syncBufUnicode),
    );

    await asyncTapUnicode.writeString(unicodeStr);
    syncTapUnicode.writeString(unicodeStr);

    expect(bytesOf(asyncBufUnicode)).toEqual(bytesOf(syncBufUnicode));
    expect(asyncTapUnicode.getPos()).toBe(syncTapUnicode.getPos());
  });

  describe("SyncWritableTap writeString fallback handling", () => {
    const fallbackString = "😀".repeat(8);

    it("retries encodeInto when the first buffer is too small", () => {
      const buffer = new ArrayBuffer(512);
      const writer = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(buffer),
      );
      // @ts-ignore private method access needed for coverage
      const originalEnsure = SyncWritableTap.ensureEncodedStringBuffer;
      let ensureCallCount = 0;

      // @ts-ignore private method access needed for the controlled scenario
      SyncWritableTap.ensureEncodedStringBuffer = (minSize: number) => {
        ensureCallCount++;
        if (ensureCallCount === 1) {
          const smallBuffer = new Uint8Array(1);
          // @ts-ignore private field access for coverage
          SyncWritableTap.encodedStringBuffer = smallBuffer;
          return smallBuffer;
        }
        return originalEnsure.call(SyncWritableTap, minSize);
      };

      try {
        writer.writeString(fallbackString);
      } finally {
        // @ts-ignore private field reset for subsequent tests
        SyncWritableTap.encodedStringBuffer = new Uint8Array(0);
        // @ts-ignore restore original method
        SyncWritableTap.ensureEncodedStringBuffer = originalEnsure;
      }

      const reader = new SyncReadableTap(
        new SyncInMemoryReadableBuffer(buffer),
      );
      expect(reader.readString()).toBe(fallbackString);
    });

    it("throws when encodeInto still cannot consume the entire string", () => {
      const buffer = new ArrayBuffer(512);
      const writer = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(buffer),
      );
      // @ts-ignore private method access needed for coverage
      const originalEnsure = SyncWritableTap.ensureEncodedStringBuffer;

      // @ts-ignore private method access needed for the controlled scenario
      SyncWritableTap.ensureEncodedStringBuffer = () => {
        const smallBuffer = new Uint8Array(1);
        // @ts-ignore private field access for coverage
        SyncWritableTap.encodedStringBuffer = smallBuffer;
        return smallBuffer;
      };

      try {
        assertThrows(
          () => writer.writeString(fallbackString),
          Error,
          "TextEncoder.encodeInto failed to consume the entire string.",
        );
      } finally {
        // @ts-ignore private field reset for subsequent tests
        SyncWritableTap.encodedStringBuffer = new Uint8Array(0);
        // @ts-ignore restore original method
        SyncWritableTap.ensureEncodedStringBuffer = originalEnsure;
      }
    });

    it("uses writeLong when encoded length exceeds INT_MAX", () => {
      const writer = new SyncWritableTap(new LenientWritableBuffer());
      const originalEncodeInto = encoder.encodeInto;
      const oversized = "😀";

      try {
        encoder.encodeInto = (_str, _buf) => ({
          read: oversized.length,
          written: 2147483648,
        });
        expect(() => writer.writeString(oversized)).not.toThrow();
      } finally {
        encoder.encodeInto = originalEncodeInto;
      }

      expect(writer.getPos()).toBeGreaterThan(0);
    });
  });

  it("writeFixed encodes identical bytes when sizes match", async () => {
    const payload = toUint8Array([1, 5, 255, 9]);

    const asyncBuf = new ArrayBuffer(8);
    const syncBuf = new ArrayBuffer(8);
    const asyncTap = new WritableTap(asyncBuf);
    const syncTap = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(syncBuf),
    );

    await asyncTap.writeFixed(payload);
    syncTap.writeFixed(payload);

    expect(bytesOf(asyncBuf)).toEqual(bytesOf(syncBuf));
    expect(asyncTap.getPos()).toBe(syncTap.getPos());
  });

  it("writeBinary encodes identical bytes", async () => {
    const asyncBuf = new ArrayBuffer(8);
    const syncBuf = new ArrayBuffer(8);
    const asyncTap = new WritableTap(asyncBuf);
    const syncTap = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(syncBuf),
    );

    await asyncTap.writeBinary("\x01\x02", 2);
    syncTap.writeBinary("\x01\x02", 2);

    expect(bytesOf(asyncBuf)).toEqual(bytesOf(syncBuf));
    expect(asyncTap.getPos()).toBe(syncTap.getPos());
  });

  it("writeBinary with len 0 behaves identically", async () => {
    const asyncBuf = new ArrayBuffer(1);
    const syncBuf = new ArrayBuffer(1);
    const asyncTap = new WritableTap(asyncBuf);
    const syncTap = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(syncBuf),
    );

    await asyncTap.writeBinary("", 0);
    syncTap.writeBinary("", 0);

    expect(asyncTap.getPos()).toBe(0);
    expect(syncTap.getPos()).toBe(0);
    expect(bytesOf(asyncBuf)).toEqual(bytesOf(syncBuf));
  });

  it("writeBinary throws same error type on overflow", async () => {
    const asyncBuf = new ArrayBuffer(1);
    const syncBuf = new ArrayBuffer(1);
    const asyncTap = new WritableTap(asyncBuf);
    const syncTap = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(syncBuf),
    );

    await assertRejects(
      async () => await asyncTap.writeBinary("\x01\x02", 2),
      WriteBufferError,
    );
    assertThrows(() => syncTap.writeBinary("\x01\x02", 2), WriteBufferError);
  });
});

describe("SyncReadableTap vs ReadableTap parity", () => {
  it("readLong decodes the same value and cursor position", async () => {
    const values = [
      0n,
      -1n,
      109213n,
      -1211n,
      -1312411211n,
      900719925474090n,
    ];

    for (const v of values) {
      const buf = new ArrayBuffer(32);
      const writer = new WritableTap(buf);
      await writer.writeLong(v);
      const writtenPos = writer.getPos();

      const asyncReader = new ReadableTap(buf, 0);
      const syncReader = new SyncReadableTap(
        new SyncInMemoryReadableBuffer(buf),
        0,
      );

      const asyncVal = await asyncReader.readLong();
      const syncVal = syncReader.readLong();

      expect(syncVal).toBe(asyncVal);
      expect(asyncReader.getPos()).toBe(syncReader.getPos());
      expect(asyncReader.getPos()).toBe(writtenPos);
    }
  });

  it("readLong handles varint with additional continuation bytes identically", async () => {
    // Test edge case: values encoded with >64 bits (exercises second while loop)
    // This manually constructs a varint that has continuation bytes beyond shift >= 70
    // to ensure both async and sync versions handle the overflow logic identically.
    //
    // Note: Valid int64 values need at most 11 bytes (64 bits / 7 bits per byte ≈ 9.14,
    // plus up to 2 more bytes for values requiring 70 bits in varint encoding).
    // We use 20 bytes to thoroughly test the second while loop that discards excess bytes.
    const oversizedVarint = toUint8Array([
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF, // 5 bytes with continuation (35 bits consumed)
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF, // 5 more bytes with continuation (70 bits total, triggers second loop)
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF, // 5 more bytes (75 bits total, well beyond 64-bit + varint overhead)
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0x01, // 5 more bytes (20 total, ensures second loop processes many bytes)
    ]);

    const asyncBuffer = new ArrayBuffer(32);
    const syncBuffer = new ArrayBuffer(32);
    new Uint8Array(asyncBuffer).set(oversizedVarint);
    new Uint8Array(syncBuffer).set(oversizedVarint);

    const asyncReader = new ReadableTap(asyncBuffer, 0);
    const syncReader = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(syncBuffer),
      0,
    );

    const asyncVal = await asyncReader.readLong();
    const syncVal = syncReader.readLong();

    // Both should decode to the same truncated 64-bit value
    // The expected value after zig-zag decode and 64-bit truncation of this specific byte pattern
    const expectedValue = -9223372036854775808n; // INT64_MIN
    expect(syncVal).toBe(asyncVal);
    expect(syncVal).toBe(expectedValue);
    expect(asyncReader.getPos()).toBe(syncReader.getPos());
    expect(asyncReader.getPos()).toBe(20); // Should consume all 20 bytes
  });

  it("readInt, readBoolean, readFloat, readDouble match", async () => {
    const buf = new ArrayBuffer(64);
    const writer = new WritableTap(buf);

    await writer.writeInt(42);
    await writer.writeBoolean(true);
    await writer.writeFloat(3.14);
    await writer.writeDouble(-10.5);

    const asyncReader = new ReadableTap(buf, 0);
    const syncReader = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buf),
      0,
    );

    const intAsync = await asyncReader.readInt();
    const intSync = syncReader.readInt();
    expect(intSync).toBe(intAsync);

    const boolAsync = await asyncReader.readBoolean();
    const boolSync = syncReader.readBoolean();
    expect(boolSync).toBe(boolAsync);

    const floatAsync = await asyncReader.readFloat();
    const floatSync = syncReader.readFloat();
    expect(floatSync).toBeCloseTo(floatAsync, 5);

    const doubleAsync = await asyncReader.readDouble();
    const doubleSync = syncReader.readDouble();
    expect(doubleSync).toBeCloseTo(doubleAsync, 10);

    expect(asyncReader.getPos()).toBe(syncReader.getPos());
  });

  it("readBytes and readString match", async () => {
    const buf = new ArrayBuffer(256);
    const writer = new WritableTap(buf);
    const bytes = toUint8Array([1, 2, 3, 255]);
    const str = "hello, world";

    await writer.writeBytes(bytes);
    await writer.writeString(str);

    const asyncReader = new ReadableTap(buf, 0);
    const syncReader = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buf),
      0,
    );

    const bytesAsync = await asyncReader.readBytes();
    const bytesSync = syncReader.readBytes();
    expect(Array.from(bytesSync)).toEqual(Array.from(bytesAsync));

    const strAsync = await asyncReader.readString();
    const strSync = syncReader.readString();
    expect(strSync).toBe(strAsync);

    expect(asyncReader.getPos()).toBe(syncReader.getPos());
  });

  it("skip helpers advance to same position", async () => {
    const buf = new ArrayBuffer(256);
    const writer = new WritableTap(buf);

    await writer.writeInt(1);
    await writer.writeLong(2n);
    await writer.writeBoolean(true);
    await writer.writeFloat(1.5);
    await writer.writeDouble(2.5);
    await writer.writeBytes(toUint8Array([1, 2, 3]));
    await writer.writeString("abc");

    const finalPos = writer.getPos();

    const asyncReader = new ReadableTap(buf, 0);
    const syncReader = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buf),
      0,
    );

    await asyncReader.skipInt();
    syncReader.skipInt();
    await asyncReader.skipLong();
    syncReader.skipLong();
    asyncReader.skipBoolean();
    syncReader.skipBoolean();
    await asyncReader.skipFloat();
    syncReader.skipFloat();
    await asyncReader.skipDouble();
    syncReader.skipDouble();
    await asyncReader.skipBytes();
    syncReader.skipBytes();
    await asyncReader.skipString();
    syncReader.skipString();

    expect(asyncReader.getPos()).toBe(finalPos);
    expect(syncReader.getPos()).toBe(finalPos);
  });

  it("match helpers return same ordering and advance cursors equally", async () => {
    const buf1 = new ArrayBuffer(128);
    const buf2 = new ArrayBuffer(128);
    const w1 = new WritableTap(buf1);
    const w2 = new WritableTap(buf2);

    await w1.writeInt(1);
    await w2.writeInt(2);
    await w1.writeLong(5n);
    await w2.writeLong(2n);
    await w1.writeFloat(1.5);
    await w2.writeFloat(3.2);
    await w1.writeDouble(-10.5);
    await w2.writeDouble(4.0);
    await w1.writeFixed(toUint8Array([1, 2, 3, 4]));
    await w2.writeFixed(toUint8Array([1, 2, 3, 5]));
    await w1.writeString("abc");
    await w2.writeString("abd");
    await w1.writeBytes(toUint8Array([1, 2, 3]));
    await w2.writeBytes(toUint8Array([1, 2, 4]));

    const a1 = new ReadableTap(buf1, 0);
    const a2 = new ReadableTap(buf2, 0);
    const s1 = new SyncReadableTap(new SyncInMemoryReadableBuffer(buf1), 0);
    const s2 = new SyncReadableTap(new SyncInMemoryReadableBuffer(buf2), 0);

    const intAsync = await a1.matchInt(a2);
    const intSync = s1.matchInt(s2);
    expect(intSync).toBe(intAsync);

    const longAsync = await a1.matchLong(a2);
    const longSync = s1.matchLong(s2);
    expect(longSync).toBe(longAsync);

    const floatAsync = await a1.matchFloat(a2);
    const floatSync = s1.matchFloat(s2);
    expect(floatSync).toBe(floatAsync);

    const doubleAsync = await a1.matchDouble(a2);
    const doubleSync = s1.matchDouble(s2);
    expect(doubleSync).toBe(doubleAsync);

    const fixedAsync = await a1.matchFixed(a2, 4);
    const fixedSync = s1.matchFixed(s2, 4);
    expect(fixedSync).toBe(fixedAsync);

    const strAsync = await a1.matchString(a2);
    const strSync = s1.matchString(s2);
    expect(strSync).toBe(strAsync);
  });

  it("verifies both readers throw ReadBufferError in the same situations", async () => {
    const empty = new ArrayBuffer(0);
    const asyncReader = new ReadableTap(empty);
    const syncReader = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(empty),
    );

    await assertRejects(
      async () => await asyncReader.readLong(),
      ReadBufferError,
    );
    assertThrows(() => syncReader.readLong(), ReadBufferError);
  });
});
