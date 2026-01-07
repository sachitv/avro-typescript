import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { assertThrows } from "@std/assert";

import { SyncReadableTap, SyncWritableTap } from "../tap_sync.ts";
import {
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "../buffers/in_memory_buffer_sync.ts";
import type { ISyncReadable, ISyncWritable } from "../buffers/buffer_sync.ts";
import { ReadBufferError, WriteBufferError } from "../buffers/buffer_sync.ts";
import { encoder } from "../text_encoding.ts";

const toUint8Array = (values: number[]): Uint8Array =>
  Uint8Array.from(values.map((value) => ((value % 256) + 256) % 256));

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
}

// Standalone sync-tap helpers mirroring the async TestTap-style tests.
const expectUint8ArrayEqual = (
  actual: Uint8Array,
  expected: Uint8Array,
): void => {
  expect(Array.from(actual)).toEqual(Array.from(expected));
};

type SyncEqualsFn<T> = (actual: T, expected: T) => void;

interface SyncWriterReaderOptions<T> {
  elems: T[];
  reader: (tap: SyncReadableTap) => T;
  skipper: (tap: SyncReadableTap, elem: T) => void;
  writer: (tap: SyncWritableTap, elem: T) => void;
  size?: number;
  equals?: SyncEqualsFn<T>;
}

function registerSyncWriterReaderTests<T>(
  group: string,
  opts: SyncWriterReaderOptions<T>,
): void {
  const size = opts.size ?? 1024;
  const equals: SyncEqualsFn<T> = opts.equals ?? ((actual, expected) => {
    expect(actual).toEqual(expected);
  });

  describe(group, () => {
    it("write/read", () => {
      for (const elem of opts.elems) {
        const buffer = new ArrayBuffer(size);
        const writer = new SyncWritableTap(
          new SyncInMemoryWritableBuffer(buffer),
        );
        opts.writer(writer, elem);
        const writtenPos = writer.getPos();

        const reader = new SyncReadableTap(
          new SyncInMemoryReadableBuffer(buffer),
        );
        const actual = opts.reader(reader);
        equals(actual, elem);
        expect(reader.getPos()).toBe(writtenPos);
      }
    });

    it("read over", () => {
      const reader = new SyncReadableTap(
        new SyncInMemoryReadableBuffer(new ArrayBuffer(0)),
      );
      expect(() => opts.reader(reader)).toThrow();
    });

    it("write over", () => {
      const writer = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(new ArrayBuffer(0)),
      );
      expect(() => opts.writer(writer, opts.elems[0]!)).toThrow();
    });

    it("skip", () => {
      for (const elem of opts.elems) {
        const buffer = new ArrayBuffer(size);
        const writer = new SyncWritableTap(
          new SyncInMemoryWritableBuffer(buffer),
        );
        opts.writer(writer, elem);
        const expectedPos = writer.getPos();

        const reader = new SyncReadableTap(
          new SyncInMemoryReadableBuffer(buffer),
        );
        opts.skipper(reader, elem);
        expect(reader.getPos()).toBe(expectedPos);
      }
    });
  });
}

const createSyncReadableTapWithWrites = (
  writes: (tap: SyncWritableTap) => void,
  size = 64,
): SyncReadableTap => {
  const buffer = new ArrayBuffer(size);
  const writer = new SyncWritableTap(new SyncInMemoryWritableBuffer(buffer));
  writes(writer);
  return new SyncReadableTap(new SyncInMemoryReadableBuffer(buffer));
};

const createBooleanTap = (value: boolean): SyncReadableTap =>
  createSyncReadableTapWithWrites((tap) => tap.writeBoolean(value), 4);

const createLongTap = (value: bigint): SyncReadableTap =>
  createSyncReadableTapWithWrites((tap) => tap.writeLong(value), 32);

const createFloatTap = (value: number): SyncReadableTap =>
  createSyncReadableTapWithWrites((tap) => tap.writeFloat(value), 32);

const createDoubleTap = (value: number): SyncReadableTap =>
  createSyncReadableTapWithWrites((tap) => tap.writeDouble(value), 64);

const createBytesTap = (data: Uint8Array): SyncReadableTap =>
  createSyncReadableTapWithWrites(
    (tap) => tap.writeBytes(data),
    data.length + 16,
  );

describe("SyncTap primitive round-trips", () => {
  registerSyncWriterReaderTests<bigint>("long", {
    elems: [
      0n,
      -1n,
      109213n,
      -1211n,
      -1312411211n,
      900719925474090n,
      (1n << 63n) - 1n, // max int64
      -(1n << 63n), // min int64
    ],
    reader: (tap) => tap.readLong(),
    skipper: (tap) => tap.skipLong(),
    writer: (tap, value) => tap.writeLong(value),
    equals: (actual, expected) => {
      expect(actual).toBe(expected);
    },
  });

  registerSyncWriterReaderTests<number>("int", {
    elems: [
      0,
      -1,
      42,
      -1234567,
      2147483647, // INT32_MAX
      -2147483648, // INT32_MIN
    ],
    reader: (tap) => tap.readInt(),
    skipper: (tap) => tap.skipInt(),
    writer: (tap, value) => tap.writeInt(value),
  });

  registerSyncWriterReaderTests<boolean>("boolean", {
    elems: [true, false],
    reader: (tap) => tap.readBoolean(),
    skipper: (tap) => tap.skipBoolean(),
    writer: (tap, value) => tap.writeBoolean(value),
  });

  registerSyncWriterReaderTests<number>("float", {
    elems: [1, 3.1, -5, 1e9],
    reader: (tap) => tap.readFloat(),
    skipper: (tap) => tap.skipFloat(),
    writer: (tap, value) => tap.writeFloat(value),
    equals: (actual, expected) => {
      expect(actual).toBeCloseTo(expected, 5);
    },
  });

  registerSyncWriterReaderTests<number>("double", {
    elems: [1, 3.1, -5, 1e12],
    reader: (tap) => tap.readDouble(),
    skipper: (tap) => tap.skipDouble(),
    writer: (tap, value) => tap.writeDouble(value),
    equals: (actual, expected) => {
      expect(actual).toBeCloseTo(expected, 10);
    },
  });

  registerSyncWriterReaderTests<string>("string", {
    elems: ["ahierw", "", "alh hewlii! rew"],
    reader: (tap) => tap.readString(),
    skipper: (tap) => tap.skipString(),
    writer: (tap, value) => tap.writeString(value),
  });

  registerSyncWriterReaderTests<Uint8Array>("bytes", {
    elems: [
      toUint8Array([0x61, 0x62, 0x63]),
      new Uint8Array(0),
      toUint8Array([1, 5, 255]),
    ],
    reader: (tap) => tap.readBytes(),
    skipper: (tap) => tap.skipBytes(),
    writer: (tap, value) => tap.writeBytes(value),
    equals: (actual, expected) => {
      expectUint8ArrayEqual(actual, expected);
    },
  });

  registerSyncWriterReaderTests<Uint8Array>("fixed", {
    elems: [toUint8Array([1, 5, 255])],
    reader: (tap) => tap.readFixed(3),
    skipper: (tap) => tap.skipFixed(3),
    writer: (tap, value) => tap.writeFixed(value),
    equals: (actual, expected) => {
      expectUint8ArrayEqual(actual, expected);
    },
    size: 3,
  });
});

describe("SyncTap numeric guard rails", () => {
  it("getValue throws ReadBufferError on read failure", () => {
    const buffer: ISyncReadable = {
      read: () => {
        throw new ReadBufferError("mock read failed", 0, 0, 0);
      },
      canReadMore: () => false,
    };

    const tap = new SyncReadableTap(buffer, 0);
    assertThrows(() => tap.getValue(), ReadBufferError, "mock read failed");
  });

  it("readBoolean throws ReadBufferError on read failure", () => {
    const buffer: ISyncReadable = {
      read: () => {
        throw new ReadBufferError("mock read failed", 0, 1, 0);
      },
      canReadMore: () => false,
    };

    const tap = new SyncReadableTap(buffer, 0);
    assertThrows(() => tap.readBoolean(), ReadBufferError, "mock read failed");
  });
  it("readInt throws when value exceeds safe integer range", () => {
    const buffer = new ArrayBuffer(16);
    const writer = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(buffer),
    );
    const big = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    writer.writeLong(big);
    const reader = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buffer),
    );
    expect(() => reader.readInt()).toThrow(RangeError);
  });

  it("readInt detects overflow before performing invalid bitwise operations", () => {
    const buffer = new ArrayBuffer(16);
    const writer = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(buffer),
    );

    const bigValue = 1n << 34n;
    writer.writeLong(bigValue);

    const reader = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buffer),
    );

    expect(() => reader.readInt()).toThrow(
      "Varint requires more than 5 bytes (int32 range exceeded)",
    );
  });

  it("readInt rejects 5-byte varints with high bits set", () => {
    const bytes = toUint8Array([
      0x80,
      0x80,
      0x80,
      0x80,
      0xF0,
    ]);
    const buf = new ArrayBuffer(bytes.length);
    new Uint8Array(buf).set(bytes);
    const reader = new SyncReadableTap(new SyncInMemoryReadableBuffer(buf));
    expect(() => reader.readInt()).toThrow(
      "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
    );
  });

  it("readInt accepts valid 5-byte varint for INT32_MAX", () => {
    // INT32_MAX = 2147483647
    // Zig-zag encoded = 4294967294 = 0xFFFFFFFE
    // Varint bytes: 0xFE 0xFF 0xFF 0xFF 0x0F
    const bytes = toUint8Array([0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
    const buf = new ArrayBuffer(bytes.length);
    new Uint8Array(buf).set(bytes);
    const reader = new SyncReadableTap(new SyncInMemoryReadableBuffer(buf));
    expect(reader.readInt()).toBe(2147483647);
  });

  it("readInt accepts valid 5-byte varint for INT32_MIN", () => {
    // INT32_MIN = -2147483648
    // Zig-zag encoded = 4294967295 = 0xFFFFFFFF
    // Varint bytes: 0xFF 0xFF 0xFF 0xFF 0x0F
    const bytes = toUint8Array([0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    const buf = new ArrayBuffer(bytes.length);
    new Uint8Array(buf).set(bytes);
    const reader = new SyncReadableTap(new SyncInMemoryReadableBuffer(buf));
    expect(reader.readInt()).toBe(-2147483648);
  });

  it("readInt accepts valid 5-byte varint near INT32_MAX", () => {
    // INT32_MAX - 1 = 2147483646
    // Zig-zag encoded = 4294967292 = 0xFFFFFFFC
    // Varint bytes: 0xFC 0xFF 0xFF 0xFF 0x0F
    const bytes = toUint8Array([0xFC, 0xFF, 0xFF, 0xFF, 0x0F]);
    const buf = new ArrayBuffer(bytes.length);
    new Uint8Array(buf).set(bytes);
    const reader = new SyncReadableTap(new SyncInMemoryReadableBuffer(buf));
    expect(reader.readInt()).toBe(2147483646);
  });

  it("readInt accepts valid 5-byte varint near INT32_MIN", () => {
    // INT32_MIN + 1 = -2147483647
    // Zig-zag encoded = 4294967293 = 0xFFFFFFFD
    // Varint bytes: 0xFD 0xFF 0xFF 0xFF 0x0F
    const bytes = toUint8Array([0xFD, 0xFF, 0xFF, 0xFF, 0x0F]);
    const buf = new ArrayBuffer(bytes.length);
    new Uint8Array(buf).set(bytes);
    const reader = new SyncReadableTap(new SyncInMemoryReadableBuffer(buf));
    expect(reader.readInt()).toBe(-2147483647);
  });

  it("readBytes throws when length exceeds safe integer range", () => {
    const buffer = new ArrayBuffer(16);
    const writer = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(buffer),
    );
    const big = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    writer.writeLong(big);
    const reader = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buffer),
    );
    expect(() => reader.readBytes()).toThrow(RangeError);
  });

  it("readInt, writeInt, and skipInt delegate to long helpers", () => {
    const buffer = new ArrayBuffer(16);
    const writer = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(buffer),
    );
    writer.writeInt(42);
    writer.writeInt(-7);
    const afterWrites = writer.getPos();

    const reader = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buffer),
    );
    expect(reader.readInt()).toBe(42);
    reader.skipInt();
    expect(reader.getPos()).toBe(afterWrites);
  });

  it("writeLong rejects values exceeding int64 range", () => {
    // Values > int64 are out of spec for Avro long and must be rejected
    const large = (1n << 70n) + 123n; // Out of spec value
    const buf = new ArrayBuffer(16);
    const tap = new SyncWritableTap(new SyncInMemoryWritableBuffer(buf));
    assertThrows(
      () => tap.writeLong(large),
      RangeError,
      "out of range for Avro long",
    );
  });

  it("readLong rejects varints exceeding 64 bits", () => {
    const bytes = toUint8Array([
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x00,
    ]);
    const buf = new ArrayBuffer(bytes.length);
    new Uint8Array(buf).set(bytes);
    const reader = new SyncReadableTap(new SyncInMemoryReadableBuffer(buf));
    expect(() => reader.readLong()).toThrow(
      "Varint requires more than 10 bytes (int64 range exceeded)",
    );
  });
});

describe("SyncWritableTap additional coverage", () => {
  it("writeBinary throws WriteBufferError when buffer is too small", () => {
    const buf = new ArrayBuffer(1);
    const tap = new SyncWritableTap(new SyncInMemoryWritableBuffer(buf));
    assertThrows(
      () => tap.writeBinary("\x01\x02", 2),
      WriteBufferError,
    );
  });

  it("writeBinary with len 0", () => {
    const buf = new ArrayBuffer(1);
    const tap = new SyncWritableTap(new SyncInMemoryWritableBuffer(buf));
    tap.writeBinary("", 0);
    expect(tap.getPos()).toBe(0);
  });
});

describe("SyncTap constructor coverage", () => {
  it("SyncReadableTap with Uint8Array", () => {
    const buf = new SyncInMemoryReadableBuffer(new ArrayBuffer(3));
    const tap = new SyncReadableTap(buf);
    expect(tap.isValid()).toBe(true);
    expect(tap.canReadMore()).toBe(true);
  });

  it("SyncWritableTap with Uint8Array", () => {
    const buf = new SyncInMemoryWritableBuffer(new ArrayBuffer(10));
    const tap = new SyncWritableTap(buf);
    expect(tap.isValid()).toBe(true);
    tap.writeInt(42);
  });

  it("SyncReadableTap with empty Uint8Array", () => {
    const buf = new SyncInMemoryReadableBuffer(new ArrayBuffer(0));
    const tap = new SyncReadableTap(buf);
    expect(tap.getValue()).toEqual(new Uint8Array(0));
    expect(tap.canReadMore()).toBe(false);
  });

  it("SyncReadableTap with ArrayBuffer uses lengthHint", () => {
    const buffer = new ArrayBuffer(10);
    const data = new Uint8Array(buffer);
    data[5] = 99;
    const tap = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buffer),
      5,
    );
    expect(tap.readFixed(1)).toEqual(new Uint8Array([99]));
  });

  it("SyncReadableTap with negative pos", () => {
    const buf = new SyncInMemoryReadableBuffer(new ArrayBuffer(3));
    expect(() => new SyncReadableTap(buf, -1)).not.toThrow();
  });

  it("SyncReadableTap with pos > length", () => {
    const buf = new SyncInMemoryReadableBuffer(new ArrayBuffer(3));
    expect(() => new SyncReadableTap(buf, 4)).not.toThrow();
  });

  it("SyncWritableTap with negative pos", () => {
    const buf = new SyncInMemoryWritableBuffer(new ArrayBuffer(10));
    expect(() => new SyncWritableTap(buf, -1)).not.toThrow();
  });

  it("SyncWritableTap with pos > length", () => {
    const buf = new SyncInMemoryWritableBuffer(new ArrayBuffer(10));
    expect(() => new SyncWritableTap(buf, 11)).not.toThrow();
  });

  it("isValid returns false when position exceeds buffer bounds", () => {
    const buf = new SyncInMemoryReadableBuffer(new ArrayBuffer(4));
    const tap = new SyncReadableTap(buf, 8);
    expect(tap.isValid()).toBe(false);
  });

  it("isValid rethrows non-RangeError from buffer", () => {
    const fake: ISyncReadable = {
      read() {
        throw new TypeError("boom");
      },
      canReadMore() {
        return false;
      },
    };
    const tap = new SyncReadableTap(fake);
    expect(() => tap.isValid()).toThrow(TypeError);
  });

  it("SyncReadableTap accepts ArrayBuffer and rejects invalid buffers", () => {
    const tap = new SyncReadableTap(new ArrayBuffer(4));
    expect(tap.getValue()).toEqual(new Uint8Array(0));
    expect(() => new SyncReadableTap({} as unknown as ISyncReadable)).toThrow(
      TypeError,
    );
  });

  it("SyncWritableTap accepts ArrayBuffer and rejects invalid buffers", () => {
    const tap = new SyncWritableTap(new ArrayBuffer(6));
    tap.writeInt(1);
    expect(tap.getPos()).toBeGreaterThan(0);
    expect(() => new SyncWritableTap({} as unknown as ISyncWritable)).toThrow(
      TypeError,
    );
  });

  it("appendRawBytes returns early for empty input", () => {
    const tap = new SyncWritableTap(new ArrayBuffer(4));
    const appendRaw = (tap as unknown as {
      appendRawBytes(bytes: Uint8Array | ArrayBufferLike | number[]): void;
    }).appendRawBytes;
    const initial = tap.getPos();
    appendRaw.call(tap, new Uint8Array(0));
    expect(tap.getPos()).toBe(initial);
  });
});

describe("SyncReadableTap internal helpers", () => {
  it("getValue exposes bytes read so far and rejects negative positions", () => {
    const buffer = new ArrayBuffer(16);
    const writer = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(buffer),
    );
    writer.writeInt(42);
    writer.writeBoolean(true);

    const tap = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buffer),
    );
    tap.readInt();
    const seen = tap.getValue();
    const prefix = new SyncInMemoryReadableBuffer(buffer).read(
      0,
      tap.getPos(),
    )!;
    expect(seen).toEqual(prefix);

    const negativeTap = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(new ArrayBuffer(4)),
      -1,
    );
    expect(() => negativeTap.getValue()).toThrow(ReadBufferError);
  });

  it("canReadMore avoids reading the underlying buffer", () => {
    let readCalls = 0;
    const fake: ISyncReadable = {
      read(_, size) {
        readCalls++;
        return new Uint8Array(size);
      },
      canReadMore() {
        return true;
      },
    };
    const tap = new SyncReadableTap(fake);
    expect(tap.canReadMore()).toBe(true);
    expect(readCalls).toBe(0);
  });

  it("handles readable buffers reporting zero length", () => {
    let readCalls = 0;
    const fake: ISyncReadable & { length(): number } = {
      length() {
        return 0;
      },
      read() {
        readCalls++;
        throw new Error("should not be called");
      },
      canReadMore() {
        return false;
      },
    };
    const tap = new SyncReadableTap(fake);
    expect(tap.canReadMore()).toBe(false);
    expect(readCalls).toBe(0);
  });
});

describe("SyncReadableTap comparison methods", () => {
  it("matchBoolean reflects ordering between boolean values", () => {
    expect(createBooleanTap(true).matchBoolean(createBooleanTap(true))).toBe(0);
    expect(createBooleanTap(true).matchBoolean(createBooleanTap(false))).toBe(
      1,
    );
    expect(createBooleanTap(false).matchBoolean(createBooleanTap(true))).toBe(
      -1,
    );
  });

  it("matchLong exercises equality and ordering branches", () => {
    expect(createLongTap(5n).matchLong(createLongTap(5n))).toBe(0);
    expect(createLongTap(1n).matchLong(createLongTap(10n))).toBe(-1);
    expect(createLongTap(11n).matchLong(createLongTap(2n))).toBe(1);
  });

  it("matchFloat exercises equality and ordering branches", () => {
    expect(createFloatTap(1.5).matchFloat(createFloatTap(1.5))).toBe(0);
    expect(createFloatTap(0.5).matchFloat(createFloatTap(2.5))).toBe(-1);
    expect(createFloatTap(3.5).matchFloat(createFloatTap(2.5))).toBe(1);
  });

  it("matchDouble exercises equality and ordering branches", () => {
    expect(createDoubleTap(-2.2).matchDouble(createDoubleTap(-2.2))).toBe(0);
    expect(createDoubleTap(1.1).matchDouble(createDoubleTap(5.2))).toBe(-1);
    expect(createDoubleTap(7.9).matchDouble(createDoubleTap(4.4))).toBe(1);
  });

  it("matchBytes reuses the string comparison logic", () => {
    const payload = toUint8Array([1, 2, 3]);
    const higher = toUint8Array([1, 2, 4]);
    expect(createBytesTap(payload).matchBytes(createBytesTap(payload))).toBe(0);
    expect(createBytesTap(payload).matchBytes(createBytesTap(higher))).toBe(-1);
  });

  it("matchInt delegates to matchLong", () => {
    const buffer1 = new ArrayBuffer(16);
    const buffer2 = new ArrayBuffer(16);
    const writer1 = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(buffer1),
    );
    const writer2 = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(buffer2),
    );

    writer1.writeInt(42);
    writer2.writeInt(100);

    const reader1 = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buffer1),
    );
    const reader2 = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buffer2),
    );

    expect(reader1.matchInt(reader2)).toBe(-1);
  });

  it("matchFixed compares fixed-length byte sequences", () => {
    const buffer1 = new ArrayBuffer(16);
    const buffer2 = new ArrayBuffer(16);
    const writer1 = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(buffer1),
    );
    const writer2 = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(buffer2),
    );

    writer1.writeFixed(toUint8Array([1, 2, 3, 4]));
    writer2.writeFixed(toUint8Array([1, 2, 3, 5]));

    const reader1 = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buffer1),
    );
    const reader2 = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(buffer2),
    );

    expect(reader1.matchFixed(reader2, 4)).toBe(-1);
  });
});

describe("SyncWritableTap string encoding", () => {
  it("writeString handles Unicode strings (non-ASCII path)", () => {
    const buffer = new ArrayBuffer(256);
    const writer = new SyncWritableTap(new SyncInMemoryWritableBuffer(buffer));
    const unicodeStr = "こんにちは世界"; // "Hello World" in Japanese

    writer.writeString(unicodeStr);

    const reader = new SyncReadableTap(new SyncInMemoryReadableBuffer(buffer));
    expect(reader.readString()).toBe(unicodeStr);
  });

  it("writeString handles large Unicode strings", () => {
    const buffer = new ArrayBuffer(8192);
    const writer = new SyncWritableTap(new SyncInMemoryWritableBuffer(buffer));
    const largeUnicode = "😀".repeat(500); // Large string exceeding 1024 chars

    writer.writeString(largeUnicode);

    const reader = new SyncReadableTap(new SyncInMemoryReadableBuffer(buffer));
    expect(reader.readString()).toBe(largeUnicode);
  });

  it("writeString retries encodeInto when the first buffer is too small", () => {
    const fallbackString = "😀".repeat(8);
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

  it("writeString throws when encodeInto still cannot consume the entire string", () => {
    const fallbackString = "😀".repeat(8);
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

  it("writeString uses writeLong when encoded length exceeds INT_MAX", () => {
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

describe("SyncTap constructor coverage", () => {
  it("writes through tap, reads through buffer", () => {
    const buffer = new ArrayBuffer(64);
    const tap = new SyncWritableTap(new SyncInMemoryWritableBuffer(buffer));

    tap.writeInt(42);
    tap.writeString("hello");
    tap.writeBytes(new Uint8Array([1, 2, 3]));

    const writtenLength = tap.getPos();
    expect(writtenLength).toBeGreaterThan(0);

    const readableBuffer = new SyncInMemoryReadableBuffer(buffer);
    expect(readableBuffer.length()).toBe(64);

    const data = readableBuffer.read(0, writtenLength);
    expect(data).toBeDefined();
    expect(data!.length).toBe(writtenLength);
  });

  it("round-trip serialization with buffer", () => {
    const buffer = new ArrayBuffer(128);
    const tap = new SyncWritableTap(new SyncInMemoryWritableBuffer(buffer));

    tap.writeInt(12345);
    tap.writeString("world");
    tap.writeFloat(3.14);

    const writtenPos = tap.getPos();
    expect(writtenPos).toBeGreaterThan(0);

    const readTap = new SyncReadableTap(new SyncInMemoryReadableBuffer(buffer));
    expect(readTap.readInt()).toBe(12345);
    expect(readTap.readString()).toBe("world");
    expect(readTap.readFloat()).toBeCloseTo(3.14, 5);

    // Verify buffer contains the data
    const readableBuffer = new SyncInMemoryReadableBuffer(buffer);
    const data = readableBuffer.read(0, writtenPos);
    expect(data).toBeDefined();
    expect(data!.length).toBe(writtenPos);
  });

  it("buffer bounds checking with tap operations", () => {
    const smallBuffer = new ArrayBuffer(8);
    const tap = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(smallBuffer),
    );

    tap.writeInt(42); // This should fit (zigzag encoded, small number)
    const pos = tap.getPos();
    expect(pos).toBe(1); // 42 encodes to 1 byte

    // Verify buffer bounds
    const readableBuffer = new SyncInMemoryReadableBuffer(smallBuffer);
    expect(readableBuffer.canReadMore(0)).toBe(true); // Can read from start
    expect(readableBuffer.canReadMore(pos)).toBe(true); // Can read from current pos
    expect(readableBuffer.canReadMore(7)).toBe(true); // Can read 1 byte at offset 7
    expect(readableBuffer.canReadMore(8)).toBe(false); // Cannot read beyond buffer
  });
});

describe("SyncWritableTap internal appendRawBytes", () => {
  it("supports offset/length writes", () => {
    const buffer = new ArrayBuffer(8);
    const writable = new SyncInMemoryWritableBuffer(buffer);
    const tap = new SyncWritableTap(writable);

    // @ts-ignore: private method access for coverage
    tap.appendRawBytes(new Uint8Array([1, 2, 3, 4]), 1, 2);
    expect(new Uint8Array(buffer, 0, tap.getPos())).toEqual(
      new Uint8Array([2, 3]),
    );
  });

  it("treats non-positive length as a no-op", () => {
    const buffer = new ArrayBuffer(8);
    const writable = new SyncInMemoryWritableBuffer(buffer);
    const tap = new SyncWritableTap(writable);

    // @ts-ignore: private method access for coverage
    tap.appendRawBytes(new Uint8Array([1, 2, 3]), 0, 0);
    expect(tap.getPos()).toBe(0);
  });
});

describe("SyncWritableTap.writeInt validation", () => {
  it("throws when value is not a 32-bit integer", () => {
    const buffer = new ArrayBuffer(16);
    const tap = new SyncWritableTap(new SyncInMemoryWritableBuffer(buffer));

    expect(() => tap.writeInt(3.14)).toThrow(RangeError);
    expect(() => tap.writeInt(2147483648)).toThrow(RangeError);
    expect(() => tap.writeInt(-2147483649)).toThrow(RangeError);
  });
});

describe("SyncWritableTap large length writeLong fallback", () => {
  it("writeBytes uses writeLong when length > 0x7FFFFFFF", () => {
    // Create a mock Uint8Array-like object with a huge length but no actual data
    const hugeLength = 0x80000000; // 2^31, just over the threshold
    const mockBuf = {
      length: hugeLength,
    } as unknown as Uint8Array;

    // Track which write methods are called
    const calls: Array<{ method: string; value: number | bigint }> = [];

    const tap = new SyncWritableTap(new LenientWritableBuffer());

    // Override writeInt and writeLong to track calls
    const originalWriteInt = tap.writeInt.bind(tap);
    const originalWriteLong = tap.writeLong.bind(tap);
    tap.writeInt = (value: number) => {
      calls.push({ method: "writeInt", value });
      return originalWriteInt(value);
    };
    tap.writeLong = (value: bigint) => {
      calls.push({ method: "writeLong", value });
      return originalWriteLong(value);
    };

    tap.writeBytes(mockBuf);

    // Should have called writeLong for the length
    expect(calls.length).toBe(1);
    expect(calls[0].method).toBe("writeLong");
    expect(calls[0].value).toBe(BigInt(hugeLength));
  });
});
