import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import Tap from "./tap.ts";

type EqualsFn<T> = (actual: T | undefined, expected: T) => void;

interface WriterReaderOptions<T> {
  elems: T[];
  reader: (tap: Tap) => T | undefined;
  skipper: (tap: Tap, elem: T) => void;
  writer: (tap: Tap, elem: T) => void;
  size?: number;
  equals?: EqualsFn<T>;
}

const toUint8Array = (values: number[]): Uint8Array =>
  Uint8Array.from(values.map((value) => ((value % 256) + 256) % 256));

const arrayBufferFrom = (values: number[]): ArrayBuffer => {
  const buffer = new ArrayBuffer(values.length);
  new Uint8Array(buffer).set(toUint8Array(values));
  return buffer;
};

const newTap = (size: number, seed?: Uint8Array): Tap => {
  const buffer = new ArrayBuffer(size);
  if (seed) {
    new Uint8Array(buffer).set(seed.subarray(0, size));
  }
  return new Tap(buffer);
};

const tapFromBytes = (bytes: number[]): Tap => new Tap(arrayBufferFrom(bytes));

const readInt32LE = (bytes: Uint8Array): number =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getInt32(
    0,
    true,
  );

const writeInt32LE = (bytes: Uint8Array, offset: number, value: number): void =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).setInt32(
    offset,
    value,
    true,
  );

const expectUint8ArrayEqual = (
  actual: Uint8Array | undefined,
  expected: Uint8Array,
): void => {
  expect(actual).not.toBeUndefined();
  expect(Array.from(actual!)).toEqual(Array.from(expected));
};

const expectTapEqual = (actual: Tap, expected: Tap): void => {
  expect(actual._testOnlyPos).toBe(expected._testOnlyPos);
  expectUint8ArrayEqual(actual._testOnlyBuf, expected._testOnlyBuf);
};

function registerWriterReaderTests<T>(
  group: string,
  opts: WriterReaderOptions<T>,
): void {
  const size = opts.size ?? 1024;
  const equals: EqualsFn<T> = opts.equals ?? ((actual, expected) => {
    expect(actual).not.toBeUndefined();
    expect(actual as T).toEqual(expected);
  });

  describe(group, () => {
    // Verifies that encoding then decoding a value round-trips and preserves cursor state.
    it("write/read", () => {
      for (const elem of opts.elems) {
        const tap = newTap(size);
        opts.writer(tap, elem);
        const writtenPos = tap._testOnlyPos;
        tap.resetPos();
        const actual = opts.reader(tap);
        equals(actual, elem);
        expect(tap._testOnlyPos).toBe(writtenPos);
      }
    });

    // Confirms that reading beyond the backing buffer marks the tap as invalid.
    it("read over", () => {
      const tap = new Tap(new ArrayBuffer(0));
      opts.reader(tap);
      expect(tap.isValid()).toBe(false);
    });

    // Ensures writing beyond the backing buffer flips validity to false.
    it("write over", () => {
      const tap = new Tap(new ArrayBuffer(0));
      opts.writer(tap, opts.elems[0]);
      expect(tap.isValid()).toBe(false);
    });

    // Verifies skip helpers advance the cursor by the expected encoded length.
    it("skip", () => {
      for (const elem of opts.elems) {
        const tap = newTap(size);
        opts.writer(tap, elem);
        const expectedPos = tap._testOnlyPos;
        tap.resetPos();
        opts.skipper(tap, elem);
        expect(tap._testOnlyPos).toBe(expectedPos);
      }
    });
  });
}

// High-level coverage of Tap's primitive encoders, decoders, and helpers.
describe("Tap", () => {
  registerWriterReaderTests<bigint>("long", {
    elems: [0n, -1n, 109213n, -1211n, -1312411211n, 900719925474090n],
    reader: (tap) => tap.readLong(),
    skipper: (tap) => tap.skipLong(),
    writer: (tap, value) => tap.writeLong(value),
    equals: (actual, expected) => {
      expect(actual).not.toBeUndefined();
      expect(actual as bigint).toBe(expected);
    },
  });

  registerWriterReaderTests<number>("int", {
    elems: [
      0,
      -1,
      42,
      -1234567,
      Number.MAX_SAFE_INTEGER,
      Number.MIN_SAFE_INTEGER,
    ],
    reader: (tap) => tap.readInt(),
    skipper: (tap) => tap.skipInt(),
    writer: (tap, value) => tap.writeInt(value),
  });

  // Encoding a 48-bit positive integer should emit the expected zig-zag bytes.
  it("long write bytes", () => {
    const tap = newTap(6);
    tap.writeLong(1440756011948n);
    const expected = toUint8Array([0xd8, 0xce, 0x80, 0xbc, 0xee, 0x53]);
    expect(tap.isValid()).toBe(true);
    expectUint8ArrayEqual(tap._testOnlyBuf, expected);
  });

  // Decoding a known long sequence should reconstruct the original integer.
  it("long read bytes", () => {
    const tap = tapFromBytes([0xd8, 0xce, 0x80, 0xbc, 0xee, 0x53]);
    expect(tap.readLong()).toBe(1440756011948n);
  });

  registerWriterReaderTests<boolean>("boolean", {
    elems: [true, false],
    reader: (tap) => tap.readBoolean(),
    skipper: (tap) => tap.skipBoolean(),
    writer: (tap, value) => tap.writeBoolean(value),
  });

  registerWriterReaderTests<number>("float", {
    elems: [1, 3.1, -5, 1e9],
    reader: (tap) => tap.readFloat(),
    skipper: (tap) => tap.skipFloat(),
    writer: (tap, value) => tap.writeFloat(value),
    equals: (actual, expected) => {
      expect(actual).not.toBeUndefined();
      expect(actual as number).toBeCloseTo(expected, 5);
    },
  });

  registerWriterReaderTests<number>("double", {
    elems: [1, 3.1, -5, 1e12],
    reader: (tap) => tap.readDouble(),
    skipper: (tap) => tap.skipDouble(),
    writer: (tap, value) => tap.writeDouble(value),
    equals: (actual, expected) => {
      expect(actual).not.toBeUndefined();
      expect(actual as number).toBeCloseTo(expected, 10);
    },
  });

  registerWriterReaderTests<string>("string", {
    elems: ["ahierw", "", "alh hewlii! rew"],
    reader: (tap) => tap.readString(),
    skipper: (tap) => tap.skipString(),
    writer: (tap, value) => tap.writeString(value),
  });

  registerWriterReaderTests<Uint8Array>("bytes", {
    elems: [
      toUint8Array([0x61, 0x62, 0x63]),
      new Uint8Array(0),
      toUint8Array([1, 5, 255]),
    ],
    reader: (tap) => tap.readBytes(),
    skipper: (tap) => tap.skipBytes(),
    writer: (tap, value) => tap.writeBytes(value),
    equals: expectUint8ArrayEqual,
  });

  registerWriterReaderTests<Uint8Array>("fixed", {
    elems: [toUint8Array([1, 5, 255])],
    reader: (tap) => tap.readFixed(3),
    skipper: (tap) => tap.skipFixed(3),
    writer: (tap, value) => tap.writeFixed(value, 3),
    equals: expectUint8ArrayEqual,
    size: 3,
  });

  it("writeFixed uses buffer length when len is omitted", () => {
    const tap = newTap(4);
    const payload = toUint8Array([10, 20, 30, 40]);
    tap.writeFixed(payload);
    expect(tap._testOnlyPos).toBe(payload.length);
    expectUint8ArrayEqual(
      tap._testOnlyBuf.subarray(0, payload.length),
      payload,
    );
  });

  describe("comparator helpers", () => {
    it("matchBoolean compares single byte values", () => {
      const tap1 = newTap(1);
      const tap2 = newTap(1);
      tap1.writeBoolean(true);
      tap2.writeBoolean(false);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchBoolean(tap2)).toBeGreaterThan(0);
    });

    it("matchInt and matchLong compare numeric order", () => {
      const tap1 = newTap(16);
      const tap2 = newTap(16);
      tap1.writeLong(5n);
      tap2.writeLong(2n);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchLong(tap2)).toBeGreaterThan(0);
      tap1.resetPos();
      tap2.resetPos();
      tap1.writeLong(-3n);
      tap2.writeLong(6n);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchLong(tap2)).toBeLessThan(0);
      tap1.resetPos();
      tap2.resetPos();
      tap1.writeInt(-10);
      tap2.writeInt(-10);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchInt(tap2)).toBe(0);
    });

    it("matchLong returns -1 when first tap is smaller", () => {
      const tap1 = newTap(8);
      const tap2 = newTap(8);
      tap1.writeLong(-1n);
      tap2.writeLong(4n);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchLong(tap2)).toBe(-1);
    });

    it("matchFloat and matchDouble track relative order", () => {
      const tap1 = newTap(16);
      const tap2 = newTap(16);
      tap1.writeFloat(1.5);
      tap2.writeFloat(3.2);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchFloat(tap2)).toBeLessThan(0);
      tap1.resetPos();
      tap2.resetPos();
      tap1.writeDouble(-10.5);
      tap2.writeDouble(-10.5);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchDouble(tap2)).toBe(0);
    });

    it("matchFloat returns 0 when values are equal", () => {
      const tap1 = newTap(8);
      const tap2 = newTap(8);
      tap1.writeFloat(4.25);
      tap2.writeFloat(4.25);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchFloat(tap2)).toBe(0);
    });

    it("matchFloat returns 1 when first value is greater", () => {
      const tap1 = newTap(8);
      const tap2 = newTap(8);
      tap1.writeFloat(9.5);
      tap2.writeFloat(3.2);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchFloat(tap2)).toBeGreaterThan(0);
    });

    it("matchDouble returns -1 when first tap is smaller", () => {
      const tap1 = newTap(16);
      const tap2 = newTap(16);
      tap1.writeDouble(-2.5);
      tap2.writeDouble(4.0);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchDouble(tap2)).toBe(-1);
    });

    it("matchFloat and matchDouble return 0 when reads underflow", () => {
      const buf1 = tapFromBytes([0x00, 0x00, 0x00]);
      const buf2 = tapFromBytes([0x7f, 0x00]);
      expect(buf1.matchFloat(buf2)).toBe(0);
      expect(buf1.matchDouble(buf2)).toBe(0);
    });

    it("matchDouble returns 1 when first value is greater", () => {
      const tap1 = newTap(16);
      const tap2 = newTap(16);
      tap1.writeDouble(123.456);
      tap2.writeDouble(-10.5);
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchDouble(tap2)).toBeGreaterThan(0);
    });

    it("matchFixed compares fixed-size byte arrays", () => {
      const tap1 = newTap(4);
      const tap2 = newTap(4);
      tap1.writeFixed(toUint8Array([1, 2, 3, 4]));
      tap2.writeFixed(toUint8Array([1, 2, 3, 5]));
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchFixed(tap2, 4)).toBeLessThan(0);
    });

    it("matchString and matchBytes compare encoded lengths and content", () => {
      const tap1 = newTap(32);
      const tap2 = newTap(32);
      tap1.writeString("abc");
      tap2.writeString("abd");
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchString(tap2)).toBeLessThan(0);
      tap1.resetPos();
      tap2.resetPos();
      tap1.writeBytes(toUint8Array([1, 2, 3]));
      tap2.writeBytes(toUint8Array([1, 2, 4]));
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchBytes(tap2)).toBeLessThan(0);
    });

    it("matchFixed returns 0 when data is unavailable", () => {
      const tap1 = tapFromBytes([1, 2]);
      const tap2 = tapFromBytes([1]);
      expect(tap1.matchFixed(tap2, 4)).toBe(0);
    });

    it("matchFixed returns 1 when first array is greater", () => {
      const tap1 = newTap(4);
      const tap2 = newTap(4);
      tap1.writeFixed(toUint8Array([2, 0, 0, 0]));
      tap2.writeFixed(toUint8Array([1, 255, 255, 255]));
      tap1.resetPos();
      tap2.resetPos();
      expect(tap1.matchFixed(tap2, 4)).toBeGreaterThan(0);
    });
  });

  // Writing binary data within the buffer bounds should populate bytes and retain zeros afterward.
  it("writeBinary within bounds", () => {
    const tap = newTap(3);
    tap.writeBinary("\x01\x02", 2);
    const expected = toUint8Array([1, 2, 0]);
    expectUint8ArrayEqual(tap._testOnlyBuf, expected);
  });

  // writeBinary should stop when exceeding buffer length, leaving untouched bytes at default values.
  it("writeBinary overflow", () => {
    const tap = newTap(1);
    tap.writeBinary("\x01\x02", 2);
    const expected = toUint8Array([0]);
    expectUint8ArrayEqual(tap._testOnlyBuf.subarray(0, 1), expected);
  });

  // Exercise the long packing helpers which convert between 8-byte two's-complement and zig-zag forms.
  describe("pack & unpack longs", () => {
    // Decoding a long that fits in a single byte should preserve sign for positive and negative values.
    it("unpack single byte", () => {
      const tap = newTap(10);
      tap.writeLong(5n);
      tap.resetPos();
      expectUint8ArrayEqual(
        tap.unpackLongBytes(),
        toUint8Array([5, 0, 0, 0, 0, 0, 0, 0]),
      );
      tap.resetPos();
      tap.writeLong(-5n);
      tap.resetPos();
      expectUint8ArrayEqual(
        tap.unpackLongBytes(),
        toUint8Array([-5, -1, -1, -1, -1, -1, -1, -1]),
      );
      tap.resetPos();
    });

    // unpackLongBytes should correctly expand multi-byte zig-zag sequences back to little-endian integers.
    it("unpack multiple bytes", () => {
      const tap = newTap(10);
      let value = 18932;
      tap.writeLong(BigInt(value));
      tap.resetPos();
      expect(readInt32LE(tap.unpackLongBytes())).toBe(value);
      tap.resetPos();
      value = -3210984;
      tap.writeLong(BigInt(value));
      tap.resetPos();
      expect(readInt32LE(tap.unpackLongBytes())).toBe(value);
    });

    // packLongBytes should emit minimal zig-zag encodings for small positive and negative values.
    it("pack single byte", () => {
      const tap = newTap(10);
      const buffer = new Uint8Array(8);
      buffer.fill(0);
      writeInt32LE(buffer, 0, 12);
      tap.packLongBytes(buffer);
      expect(tap._testOnlyPos).toBe(1);
      tap.resetPos();
      expect(tap.readLong()).toBe(12n);
      tap.resetPos();
      writeInt32LE(buffer, 0, -37);
      writeInt32LE(buffer, 4, -1);
      tap.packLongBytes(buffer);
      expect(tap._testOnlyPos).toBe(1);
      tap.resetPos();
      expect(tap.readLong()).toBe(-37n);
      tap.resetPos();
      writeInt32LE(buffer, 0, -1);
      writeInt32LE(buffer, 4, -1);
      tap.packLongBytes(buffer);
      expectUint8ArrayEqual(
        tap._testOnlyBuf.subarray(0, tap._testOnlyPos),
        toUint8Array([1]),
      );
      tap.resetPos();
      expect(tap.readLong()).toBe(-1n);
    });

    // Packed bytes should exactly round-trip through unpack, pack, and equality comparisons.
    it("roundtrip", () => {
      const cases = [
        1231514n,
        -123n,
        124124n,
        109283109271n,
        BigInt(Number.MAX_SAFE_INTEGER),
        BigInt(Number.MIN_SAFE_INTEGER),
        0n,
        -1n,
      ];

      for (const value of cases) {
        const tap1 = newTap(10);
        const tap2 = newTap(10);
        tap1.writeLong(value);
        tap1.resetPos();
        tap2.packLongBytes(tap1.unpackLongBytes());
        expectTapEqual(tap2, tap1);
      }
    });
  });

  it("readInt throws when value exceeds safe integer range", () => {
    const tap = newTap(16);
    const big = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    tap.writeLong(big);
    tap.resetPos();
    expect(() => tap.readInt()).toThrow(RangeError);
  });

  it("readBytes throws when length exceeds safe integer range", () => {
    const tap = newTap(16);
    const big = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    tap.writeLong(big);
    tap.resetPos();
    expect(() => tap.readBytes()).toThrow(RangeError);
  });

  it("readInt, writeInt, and skipInt delegate to long helpers", () => {
    const tap = newTap(16);
    tap.writeInt(42);
    tap.writeInt(-7);
    const afterWrites = tap._testOnlyPos;
    tap.resetPos();
    expect(tap.readInt()).toBe(42);
    tap.skipInt();
    expect(tap._testOnlyPos).toBe(afterWrites);
  });

  it("constructor rejects non-ArrayBuffer input", () => {
    expect(() => new Tap({} as unknown as ArrayBuffer)).toThrow(TypeError);
  });

  it("constructor rejects invalid positions", () => {
    const buf = new ArrayBuffer(8);
    expect(() => new Tap(buf, -1)).toThrow(RangeError);
    expect(() => new Tap(buf, 1.5)).toThrow(RangeError);
    expect(() => new Tap(buf, Number.MAX_SAFE_INTEGER + 1)).toThrow(RangeError);
  });

  // getValue should expose only the written region when the position is within bounds.
  it("getValue returns written bytes", () => {
    const tap = newTap(10);
    tap.writeBoolean(true);
    tap.writeBoolean(false);
    tap.writeBoolean(true);
    expectUint8ArrayEqual(tap.getValue(), toUint8Array([1, 0, 1]));
  });

  it("getValue throws when position exceeds buffer length", () => {
    const tap = new Tap(new ArrayBuffer(4), 10);
    expect(() => tap.getValue()).toThrow(RangeError);
  });

  it("readLong handles values requiring extra continuation bytes", () => {
    const large = (1n << 70n) + 123n;
    const tap = newTap(16);
    tap.writeLong(large);
    tap.resetPos();
    expect(tap.readLong()).toBe(large);
  });

  it("readLong correctly decodes near 2^56", () => {
    const near = (1n << 56n) + 1n;
    const tap = newTap(16);
    tap.writeLong(near);
    tap.resetPos();
    expect(tap.readLong()).toBe(near);
  });

  it("readLong processes additional continuation bytes", () => {
    const bytes = [
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
    ];
    const tap = tapFromBytes(bytes);
    expect(tap.readLong()).toBe(0n);
  });
});
