import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { assertRejects, assertThrows } from "@std/assert";

import { ReadableTap, WritableTap } from "../tap.ts";
import { SyncReadableTap, SyncWritableTap } from "../tap_sync.ts";
import {
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "../buffers/in_memory_buffer_sync.ts";
import { WriteBufferError } from "../buffers/buffer_sync.ts";
import type { ISyncReadable, ISyncWritable } from "../buffers/buffer_sync.ts";
import { encoder } from "../text_encoding.ts";

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

const toUint8Array = (values: number[]): Uint8Array =>
  Uint8Array.from(values.map((value) => ((value % 256) + 256) % 256));

const bytesOf = (buffer: ArrayBuffer): Uint8Array => new Uint8Array(buffer);

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
      2n ** 70n,
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
});

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

  it("writeBinary overflows consistently", async () => {
    const asyncBuf = new ArrayBuffer(1);
    const syncBuf = new ArrayBuffer(1);
    const asyncTap = new WritableTap(asyncBuf);
    const syncTap = new SyncWritableTap(
      new SyncInMemoryWritableBuffer(syncBuf),
    );

    await assertRejects(
      async () => await asyncTap.writeBinary("\x01\x02", 2),
      RangeError,
    );
    assertThrows(() => syncTap.writeBinary("\x01\x02", 2), WriteBufferError);
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

describe("SyncReadableTap helper coverage", () => {
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
    expect(() => negativeTap.getValue()).toThrow(RangeError);
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

  it("verifies both readers throw RangeError in the same situations", async () => {
    const empty = new ArrayBuffer(0);
    const asyncReader = new ReadableTap(empty);
    const syncReader = new SyncReadableTap(
      new SyncInMemoryReadableBuffer(empty),
    );

    await assertRejects(async () => await asyncReader.readLong(), RangeError);
    assertThrows(() => syncReader.readLong());
  });
});

describe("Sync Buffer + Tap integration", () => {
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
