import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import Tap from "./tap.ts";
import { IBuffer } from "./buffers/buffer.ts";

type EqualsFn<T> = (actual: T | undefined, expected: T) => void;

interface WriterReaderOptions<T> {
  elems: T[];
  reader: (tap: Tap) => Promise<T | undefined>;
  skipper: (tap: Tap, elem: T) => void;
  writer: (tap: Tap, elem: T) => Promise<void>;
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

const expectTapEqual = async (actual: Tap, expected: Tap): Promise<void> => {
  expect(actual._testOnlyPos).toBe(expected._testOnlyPos);
  expectUint8ArrayEqual(
    await actual._testOnlyBuf(),
    await expected._testOnlyBuf(),
  );
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
    it("write/read", async () => {
      for (const elem of opts.elems) {
        const tap = newTap(size);
        await opts.writer(tap, elem);
        const writtenPos = tap._testOnlyPos;
        tap.resetPos();
        const actual = await opts.reader(tap);
        equals(actual, elem);
        expect(tap._testOnlyPos).toBe(writtenPos);
      }
    });

    // Confirms that reading beyond the backing buffer marks the tap as invalid.
    it("read over", async () => {
      const tap = new Tap(new ArrayBuffer(0));
      await opts.reader(tap);
      expect(await tap.isValid()).toBe(false);
    });

    // Ensures writing beyond the backing buffer flips validity to false.
    it("write over", async () => {
      const tap = new Tap(new ArrayBuffer(0));
      await opts.writer(tap, opts.elems[0]);
      expect(await tap.isValid()).toBe(false);
    });

    // Verifies skip helpers advance the cursor by the expected encoded length.
    it("skip", async () => {
      for (const elem of opts.elems) {
        const tap = newTap(size);
        await opts.writer(tap, elem);
        const expectedPos = tap._testOnlyPos;
        tap.resetPos();
        await opts.skipper(tap, elem);
        expect(tap._testOnlyPos).toBe(expectedPos);
      }
    });
  });
}

// High-level coverage of Tap's primitive encoders, decoders, and helpers.
describe("Tap", () => {
  registerWriterReaderTests<bigint>("long", {
    elems: [0n, -1n, 109213n, -1211n, -1312411211n, 900719925474090n],
    reader: async (tap) => await tap.readLong(),
    skipper: async (tap) => await tap.skipLong(),
    writer: async (tap, value) => await tap.writeLong(value),
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
    reader: async (tap) => await tap.readInt(),
    skipper: async (tap) => await tap.skipInt(),
    writer: async (tap, value) => await tap.writeInt(value),
  });

  // Encoding a 48-bit positive integer should emit the expected zig-zag bytes.
  it("long write bytes", async () => {
    const tap = newTap(6);
    await tap.writeLong(1440756011948n);
    const expected = toUint8Array([0xd8, 0xce, 0x80, 0xbc, 0xee, 0x53]);
    expect(await tap.isValid()).toBe(true);
    expectUint8ArrayEqual(await tap._testOnlyBuf(), expected);
  });

  // Decoding a known long sequence should reconstruct the original integer.
  it("long read bytes", async () => {
    const tap = tapFromBytes([0xd8, 0xce, 0x80, 0xbc, 0xee, 0x53]);
    expect(await tap.readLong()).toBe(1440756011948n);
  });

  registerWriterReaderTests<boolean>("boolean", {
    elems: [true, false],
    reader: async (tap) => await tap.readBoolean(),
    skipper: async (tap) => await tap.skipBoolean(),
    writer: async (tap, value) => await tap.writeBoolean(value),
  });

  registerWriterReaderTests<number>("float", {
    elems: [1, 3.1, -5, 1e9],
    reader: async (tap) => await tap.readFloat(),
    skipper: async (tap) => await tap.skipFloat(),
    writer: async (tap, value) => await tap.writeFloat(value),
    equals: (actual, expected) => {
      expect(actual).not.toBeUndefined();
      expect(actual as number).toBeCloseTo(expected, 5);
    },
  });

  registerWriterReaderTests<number>("double", {
    elems: [1, 3.1, -5, 1e12],
    reader: async (tap) => await tap.readDouble(),
    skipper: async (tap) => await tap.skipDouble(),
    writer: async (tap, value) => await tap.writeDouble(value),
    equals: (actual, expected) => {
      expect(actual).not.toBeUndefined();
      expect(actual as number).toBeCloseTo(expected, 10);
    },
  });

  registerWriterReaderTests<string>("string", {
    elems: ["ahierw", "", "alh hewlii! rew"],
    reader: async (tap) => await tap.readString(),
    skipper: async (tap) => await tap.skipString(),
    writer: async (tap, value) => await tap.writeString(value),
  });

  registerWriterReaderTests<Uint8Array>("bytes", {
    elems: [
      toUint8Array([0x61, 0x62, 0x63]),
      new Uint8Array(0),
      toUint8Array([1, 5, 255]),
    ],
    reader: async (tap) => await tap.readBytes(),
    skipper: async (tap) => await tap.skipBytes(),
    writer: async (tap, value) => await tap.writeBytes(value),
    equals: expectUint8ArrayEqual,
  });

  registerWriterReaderTests<Uint8Array>("fixed", {
    elems: [toUint8Array([1, 5, 255])],
    reader: async (tap) => await tap.readFixed(3),
    skipper: async (tap) => await tap.skipFixed(3),
    writer: async (tap, value) => await tap.writeFixed(value, 3),
    equals: expectUint8ArrayEqual,
    size: 3,
  });

  it("writeFixed uses buffer length when len is omitted", async () => {
    const tap = newTap(4);
    const payload = toUint8Array([10, 20, 30, 40]);
    await tap.writeFixed(payload);
    expect(tap._testOnlyPos).toBe(payload.length);
    const buf = await tap._testOnlyBuf();
    expectUint8ArrayEqual(
      buf.subarray(0, payload.length),
      payload,
    );
  });

  describe("comparator helpers", () => {
    it("matchBoolean compares single byte values", async () => {
      const tap1 = newTap(1);
      const tap2 = newTap(1);
      await tap1.writeBoolean(true);
      await tap2.writeBoolean(false);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchBoolean(tap2)).toBeGreaterThan(0);
    });

    it("matchInt and matchLong compare numeric order", async () => {
      const tap1 = newTap(16);
      const tap2 = newTap(16);
      await tap1.writeLong(5n);
      await tap2.writeLong(2n);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchLong(tap2)).toBeGreaterThan(0);
      tap1.resetPos();
      tap2.resetPos();
      await tap1.writeLong(-3n);
      await tap2.writeLong(6n);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchLong(tap2)).toBeLessThan(0);
      tap1.resetPos();
      tap2.resetPos();
      await tap1.writeInt(-10);
      await tap2.writeInt(-10);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchInt(tap2)).toBe(0);
    });

    it("matchLong returns -1 when first tap is smaller", async () => {
      const tap1 = newTap(8);
      const tap2 = newTap(8);
      await tap1.writeLong(-1n);
      await tap2.writeLong(4n);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchLong(tap2)).toBe(-1);
    });

    it("matchFloat and matchDouble track relative order", async () => {
      const tap1 = newTap(16);
      const tap2 = newTap(16);
      await tap1.writeFloat(1.5);
      await tap2.writeFloat(3.2);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchFloat(tap2)).toBeLessThan(0);
      tap1.resetPos();
      tap2.resetPos();
      await tap1.writeDouble(-10.5);
      await tap2.writeDouble(4.0);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchDouble(tap2)).toBeLessThan(0);
    });

    it("matchFloat returns 0 when values are equal", async () => {
      const tap1 = newTap(8);
      const tap2 = newTap(8);
      await tap1.writeFloat(4.25);
      await tap2.writeFloat(4.25);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchFloat(tap2)).toBe(0);
    });

    it("matchFloat returns 1 when first value is greater", async () => {
      const tap1 = newTap(8);
      const tap2 = newTap(8);
      await tap1.writeFloat(9.5);
      await tap2.writeFloat(3.2);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchFloat(tap2)).toBeGreaterThan(0);
    });

    it("matchDouble returns -1 when first tap is smaller", async () => {
      const tap1 = newTap(16);
      const tap2 = newTap(16);
      await tap1.writeDouble(-2.5);
      await tap2.writeDouble(4.0);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchDouble(tap2)).toBe(-1);
    });

    it("matchFloat and matchDouble return 0 when reads underflow", async () => {
      const buf1 = tapFromBytes([0x00, 0x00, 0x00]);
      const buf2 = tapFromBytes([0x7f, 0x00]);
      expect(await buf1.matchFloat(buf2)).toBe(0);
      expect(await buf1.matchDouble(buf2)).toBe(0);
    });

    it("matchDouble returns 1 when first value is greater", async () => {
      const tap1 = newTap(16);
      const tap2 = newTap(16);
      await tap1.writeDouble(123.456);
      await tap2.writeDouble(-10.5);
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchDouble(tap2)).toBeGreaterThan(0);
    });

    it("matchFixed compares fixed-size byte arrays", async () => {
      const tap1 = newTap(4);
      const tap2 = newTap(4);
      await tap1.writeFixed(toUint8Array([1, 2, 3, 4]));
      await tap2.writeFixed(toUint8Array([1, 2, 3, 5]));
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchFixed(tap2, 4)).toBeLessThan(0);
    });

    it("matchString and matchBytes compare encoded lengths and content", async () => {
      const tap1 = newTap(32);
      const tap2 = newTap(32);
      await tap1.writeString("abc");
      await tap2.writeString("abd");
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchString(tap2)).toBeLessThan(0);
      tap1.resetPos();
      tap2.resetPos();
      await tap1.writeBytes(toUint8Array([1, 2, 3]));
      await tap2.writeBytes(toUint8Array([1, 2, 4]));
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchBytes(tap2)).toBeLessThan(0);
    });

    it("matchFixed returns 0 when data is unavailable", async () => {
      const tap1 = tapFromBytes([1, 2]);
      const tap2 = tapFromBytes([1]);
      expect(await tap1.matchFixed(tap2, 4)).toBe(0);
    });

    it("matchFixed returns 1 when first array is greater", async () => {
      const tap1 = newTap(4);
      const tap2 = newTap(4);
      await tap1.writeFixed(toUint8Array([2, 0, 0, 0]));
      await tap2.writeFixed(toUint8Array([1, 255, 255, 255]));
      tap1.resetPos();
      tap2.resetPos();
      expect(await tap1.matchFixed(tap2, 4)).toBeGreaterThan(0);
    });
  });

  // Writing binary data within the buffer bounds should populate bytes and retain zeros afterward.
  it("writeBinary within bounds", async () => {
    const tap = newTap(3);
    await tap.writeBinary("\x01\x02", 2);
    const expected = toUint8Array([1, 2, 0]);
    expectUint8ArrayEqual(await tap._testOnlyBuf(), expected);
  });

  // writeBinary should stop when exceeding buffer length, leaving untouched bytes at default values.
  it("writeBinary overflow", async () => {
    const tap = newTap(1);
    await tap.writeBinary("\x01\x02", 2);
    const expected = toUint8Array([0]);
    const buf = await tap._testOnlyBuf();
    expectUint8ArrayEqual(buf.subarray(0, 1), expected);
  });

  // Exercise the long packing helpers which convert between 8-byte two's-complement and zig-zag forms.
  describe("pack & unpack longs", () => {
    // Decoding a long that fits in a single byte should preserve sign for positive and negative values.
    it("unpack single byte", async () => {
      const tap = newTap(10);
      await tap.writeLong(5n);
      tap.resetPos();
      expectUint8ArrayEqual(
        await tap.unpackLongBytes(),
        toUint8Array([5, 0, 0, 0, 0, 0, 0, 0]),
      );
      tap.resetPos();
      await tap.writeLong(-5n);
      tap.resetPos();
      expectUint8ArrayEqual(
        await tap.unpackLongBytes(),
        toUint8Array([-5, -1, -1, -1, -1, -1, -1, -1]),
      );
      tap.resetPos();
    });

    // unpackLongBytes should correctly expand multi-byte zig-zag sequences back to little-endian integers.
    it("unpack multiple bytes", async () => {
      const tap = newTap(10);
      let value = 18932;
      await tap.writeLong(BigInt(value));
      tap.resetPos();
      expect(readInt32LE(await tap.unpackLongBytes())).toBe(value);
      tap.resetPos();
      value = -3210984;
      await tap.writeLong(BigInt(value));
      tap.resetPos();
      expect(readInt32LE(await tap.unpackLongBytes())).toBe(value);
    });

    // packLongBytes should emit minimal zig-zag encodings for small positive and negative values.
    it("pack single byte", async () => {
      const tap = newTap(10);
      const buffer = new Uint8Array(8);
      buffer.fill(0);
      writeInt32LE(buffer, 0, 12);
      await tap.packLongBytes(buffer);
      expect(tap._testOnlyPos).toBe(1);
      tap.resetPos();
      expect(await tap.readLong()).toBe(12n);
      tap.resetPos();
      writeInt32LE(buffer, 0, -37);
      writeInt32LE(buffer, 4, -1);
      await tap.packLongBytes(buffer);
      expect(tap._testOnlyPos).toBe(1);
      tap.resetPos();
      expect(await tap.readLong()).toBe(-37n);
      tap.resetPos();
      writeInt32LE(buffer, 0, -1);
      writeInt32LE(buffer, 4, -1);
      await tap.packLongBytes(buffer);
      const buf = await tap._testOnlyBuf();
      expectUint8ArrayEqual(
        buf.subarray(0, tap._testOnlyPos),
        toUint8Array([1]),
      );
      tap.resetPos();
      expect(await tap.readLong()).toBe(-1n);
    });

    // Packed bytes should exactly round-trip through unpack, pack, and equality comparisons.
    it("roundtrip", async () => {
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
        await tap1.writeLong(value);
        tap1.resetPos();
        await tap2.packLongBytes(await tap1.unpackLongBytes());
        await expectTapEqual(tap2, tap1);
      }
    });
  });

  it("readInt throws when value exceeds safe integer range", async () => {
    const tap = newTap(16);
    const big = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    await tap.writeLong(big);
    tap.resetPos();
    await expect(tap.readInt()).rejects.toThrow(RangeError);
  });

  it("readBytes throws when length exceeds safe integer range", async () => {
    const tap = newTap(16);
    const big = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    await tap.writeLong(big);
    tap.resetPos();
    await expect(tap.readBytes()).rejects.toThrow(RangeError);
  });

  it("readInt, writeInt, and skipInt delegate to long helpers", async () => {
    const tap = newTap(16);
    await tap.writeInt(42);
    await tap.writeInt(-7);
    const afterWrites = tap._testOnlyPos;
    tap.resetPos();
    expect(await tap.readInt()).toBe(42);
    await tap.skipInt();
    expect(tap._testOnlyPos).toBe(afterWrites);
  });

  it("constructor rejects non-ArrayBuffer input", () => {
    expect(() => new Tap({} as unknown as ArrayBuffer)).toThrow(TypeError);
  });

  it("constructor accepts IBuffer", async () => {
    const mockBuffer: IBuffer = {
      length: () => Promise.resolve(10),
      read: (offset, size) => {
        if (offset + size > 10) return Promise.resolve(undefined);
        return Promise.resolve(new Uint8Array(size).fill(42));
      },
      write: (_offset, _data) => Promise.resolve(),
    };
    const tap = new Tap(mockBuffer);
    expect(await tap.isValid()).toBe(true);
    expect(await tap.readBoolean()).toBe(true); // 42 & 1 = 0, but wait, readBoolean reads 1 byte, !!value, 42 is truthy
  });

  it("constructor rejects invalid positions", () => {
    const buf = new ArrayBuffer(8);
    expect(() => new Tap(buf, -1)).toThrow(RangeError);
    expect(() => new Tap(buf, 1.5)).toThrow(RangeError);
    expect(() => new Tap(buf, Number.MAX_SAFE_INTEGER + 1)).toThrow(RangeError);
  });

  // getValue should expose only the written region when the position is within bounds.
  it("getValue returns written bytes", async () => {
    const tap = newTap(10);
    await tap.writeBoolean(true);
    await tap.writeBoolean(false);
    await tap.writeBoolean(true);
    expectUint8ArrayEqual(await tap.getValue(), toUint8Array([1, 0, 1]));
  });

  it("getValue throws when position exceeds buffer length", async () => {
    const tap = new Tap(new ArrayBuffer(4), 10);
    await expect(tap.getValue()).rejects.toThrow(RangeError);
  });

  it("_testOnlyBuf returns the full buffer", async () => {
    const tap = newTap(5);
    await tap.writeBoolean(true);
    await tap.writeBoolean(false);
    const buf = await tap._testOnlyBuf();
    expect(buf.length).toBe(5);
    expect(buf[0]).toBe(1); // true
    expect(buf[1]).toBe(0); // false
    expect(buf[2]).toBe(0); // rest zero
    expect(buf[3]).toBe(0);
    expect(buf[4]).toBe(0);
  });

  it("_testOnlyBuf returns empty array when read fails", async () => {
    const mockBuffer: IBuffer = {
      length: () => Promise.resolve(10),
      read: (_offset, _size) => Promise.resolve(undefined), // always fail
      write: (_offset, _data) => Promise.resolve(),
    };
    const tap = new Tap(mockBuffer);
    const buf = await tap._testOnlyBuf();
    expect(buf).toEqual(new Uint8Array());
  });

  it("readFloat returns undefined when read fails", async () => {
    const mockBuffer: IBuffer = {
      length: () => Promise.resolve(10),
      read: (_offset, _size) => Promise.resolve(undefined), // always fail
      write: (_offset, _data) => Promise.resolve(),
    };
    const tap = new Tap(mockBuffer);
    const result = await tap.readFloat();
    expect(result).toBeUndefined();
  });

  it("readDouble returns undefined when read fails", async () => {
    const mockBuffer: IBuffer = {
      length: () => Promise.resolve(10),
      read: (_offset, _size) => Promise.resolve(undefined), // always fail
      write: (_offset, _data) => Promise.resolve(),
    };
    const tap = new Tap(mockBuffer);
    const result = await tap.readDouble();
    expect(result).toBeUndefined();
  });

  it("matchString returns 0 when read fails", async () => {
    const mockBuffer: IBuffer = {
      length: () => Promise.resolve(10),
      read: (_offset, _size) => Promise.resolve(undefined), // always fail
      write: (_offset, _data) => Promise.resolve(),
    };
    const tap1 = new Tap(mockBuffer);
    const tap2 = new Tap(mockBuffer);
    // Write some data to advance pos and trigger read in matchString
    await tap1.writeLong(3n); // length 3
    await tap2.writeLong(3n);
    tap1.resetPos();
    tap2.resetPos();
    const result = await tap1.matchString(tap2);
    expect(result).toBe(0);
  });

  it("readLong handles values requiring extra continuation bytes", async () => {
    const large = (1n << 70n) + 123n;
    const tap = newTap(16);
    await tap.writeLong(large);
    tap.resetPos();
    expect(await tap.readLong()).toBe(large);
  });

  it("readLong correctly decodes near 2^56", async () => {
    const near = (1n << 56n) + 1n;
    const tap = newTap(16);
    await tap.writeLong(near);
    tap.resetPos();
    expect(await tap.readLong()).toBe(near);
  });

  it("readLong processes additional continuation bytes", async () => {
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
      0x80,
      0x00,
    ];
    const tap = tapFromBytes(bytes);
    expect(await tap.readLong()).toBe(0n);
  });
});
