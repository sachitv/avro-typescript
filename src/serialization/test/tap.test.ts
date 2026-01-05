import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { assertRejects } from "@std/assert";
import { ReadBufferError, WriteBufferError } from "../buffers/buffer_error.ts";
import { ReadableTap, WritableTap } from "../tap.ts";
import type { IReadableBuffer, IWritableBuffer } from "../buffers/buffer.ts";
import { TestTap as Tap } from "./test_tap.ts";
import { encoder } from "../text_encoding.ts";

type EqualsFn<T> = (actual: T, expected: T) => void;

interface WriterReaderOptions<T> {
  elems: T[];
  reader: (tap: Tap) => Promise<T>;
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

const expectUint8ArrayEqual = (
  actual: Uint8Array | undefined,
  expected: Uint8Array,
): void => {
  expect(actual).not.toBeUndefined();
  expect(Array.from(actual!)).toEqual(Array.from(expected));
};

class ErrorBuffer implements IReadableBuffer {
  constructor(private readonly error: Error) {}

  // deno-lint-ignore require-await
  async read(_offset: number, _size: number): Promise<Uint8Array> {
    throw this.error;
  }

  // deno-lint-ignore require-await
  async canReadMore(): Promise<boolean> {
    throw this.error;
  }
}

class UndefinedReadBuffer implements IReadableBuffer {
  constructor(private readonly limit: number) {}

  // deno-lint-ignore require-await
  async read(offset: number, size: number): Promise<Uint8Array> {
    if (offset + size > this.limit) {
      throw new ReadBufferError(
        "Operation exceeds buffer bounds",
        offset,
        size,
        this.limit,
      );
    }
    return new Uint8Array(size);
  }

  // deno-lint-ignore require-await
  async canReadMore(offset: number): Promise<boolean> {
    return offset < this.limit;
  }
}

class NullFloatDoubleTap extends ReadableTap {
  constructor() {
    super(new ArrayBuffer(0));
  }

  // deno-lint-ignore require-await
  override async readFloat(): Promise<number> {
    this.pos += 4;
    throw new ReadBufferError(
      "Attempt to read beyond buffer bounds.",
      this.pos - 4,
      4,
      0,
    );
  }

  // deno-lint-ignore require-await
  override async readDouble(): Promise<number> {
    this.pos += 8;
    throw new ReadBufferError(
      "Attempt to read beyond buffer bounds.",
      this.pos - 8,
      8,
      0,
    );
  }
}

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
    it("write/read", async () => {
      for (const elem of opts.elems) {
        const tap = newTap(size);
        await opts.writer(tap, elem);
        const writtenPos = tap.getPos();
        tap._testOnlyResetPos();
        const actual = await opts.reader(tap);
        equals(actual, elem);
        expect(tap.getPos()).toBe(writtenPos);
      }
    });

    it("read over", async () => {
      const tap = new Tap(new ArrayBuffer(0));
      await assertRejects(async () => await opts.reader(tap), ReadBufferError);
    });

    it("write over", async () => {
      const tap = new Tap(new ArrayBuffer(0));
      await assertRejects(
        async () => await opts.writer(tap, opts.elems[0]),
        WriteBufferError,
      );
    });

    it("skip", async () => {
      for (const elem of opts.elems) {
        const tap = newTap(size);
        await opts.writer(tap, elem);
        const expectedPos = tap.getPos();
        tap._testOnlyResetPos();
        await opts.skipper(tap, elem);
        expect(tap.getPos()).toBe(expectedPos);
      }
    });
  });
}

describe("Tap primitive round-trips", () => {
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
      2147483647,
      -2147483648,
    ],
    reader: async (tap) => await tap.readInt(),
    skipper: async (tap) => await tap.skipInt(),
    writer: async (tap, value) => await tap.writeInt(value),
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
    writer: async (tap, value) => await tap.writeFixed(value),
    equals: expectUint8ArrayEqual,
    size: 3,
  });

  describe("long encoding specifics", () => {
    it("long write bytes", async () => {
      const tap = newTap(6);
      await tap.writeLong(1440756011948n);
      const expected = toUint8Array([0xd8, 0xce, 0x80, 0xbc, 0xee, 0x53]);
      expect(await tap.isValid()).toBe(true);
      expectUint8ArrayEqual(await tap._testOnlyBuf(), expected);
    });

    it("long read bytes", async () => {
      const tap = tapFromBytes([0xd8, 0xce, 0x80, 0xbc, 0xee, 0x53]);
      expect(await tap.readLong()).toBe(1440756011948n);
    });
  });
});

describe("Tap comparator helpers", () => {
  it("matchBoolean compares single byte values", async () => {
    const tap1 = newTap(1);
    const tap2 = newTap(1);
    await tap1.writeBoolean(true);
    await tap2.writeBoolean(false);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchBoolean(tap2)).toBeGreaterThan(0);
  });

  it("matchInt and matchLong compare numeric order", async () => {
    const tap1 = newTap(16);
    const tap2 = newTap(16);
    await tap1.writeLong(5n);
    await tap2.writeLong(2n);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchLong(tap2)).toBeGreaterThan(0);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    await tap1.writeLong(-3n);
    await tap2.writeLong(6n);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchLong(tap2)).toBeLessThan(0);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    await tap1.writeInt(-10);
    await tap2.writeInt(-10);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchInt(tap2)).toBe(0);
  });

  it("matchLong returns -1 when first tap is smaller", async () => {
    const tap1 = newTap(8);
    const tap2 = newTap(8);
    await tap1.writeLong(-1n);
    await tap2.writeLong(4n);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchLong(tap2)).toBe(-1);
  });

  it("matchFloat and matchDouble track relative order", async () => {
    const tap1 = newTap(16);
    const tap2 = newTap(16);
    await tap1.writeFloat(1.5);
    await tap2.writeFloat(3.2);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchFloat(tap2)).toBeLessThan(0);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    await tap1.writeDouble(-10.5);
    await tap2.writeDouble(4.0);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchDouble(tap2)).toBeLessThan(0);
  });

  it("matchFloat returns 0 when values are equal", async () => {
    const tap1 = newTap(8);
    const tap2 = newTap(8);
    await tap1.writeFloat(4.25);
    await tap2.writeFloat(4.25);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchFloat(tap2)).toBe(0);
  });

  it("matchFloat throws when float reads are unavailable", async () => {
    const tap1 = new NullFloatDoubleTap();
    const tap2 = new NullFloatDoubleTap();
    await assertRejects(
      async () => await tap1.matchFloat(tap2),
      ReadBufferError,
    );
  });

  it("matchFloat returns 1 when first value is greater", async () => {
    const tap1 = newTap(8);
    const tap2 = newTap(8);
    await tap1.writeFloat(9.5);
    await tap2.writeFloat(3.2);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchFloat(tap2)).toBeGreaterThan(0);
  });

  it("matchDouble returns -1 when first tap is smaller", async () => {
    const tap1 = newTap(16);
    const tap2 = newTap(16);
    await tap1.writeDouble(-2.5);
    await tap2.writeDouble(4.0);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchDouble(tap2)).toBe(-1);
  });

  it("matchDouble throws when double reads are unavailable", async () => {
    const tap1 = new NullFloatDoubleTap();
    const tap2 = new NullFloatDoubleTap();
    await assertRejects(
      async () => await tap1.matchDouble(tap2),
      ReadBufferError,
    );
  });

  it("matchDouble returns 0 when values are equal", async () => {
    const tap1 = newTap(16);
    const tap2 = newTap(16);
    await tap1.writeDouble(4.25);
    await tap2.writeDouble(4.25);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchDouble(tap2)).toBe(0);
  });

  it("matchFloat and matchDouble reject when reads underflow", async () => {
    const buf1 = tapFromBytes([0x00, 0x00, 0x00]);
    const buf2 = tapFromBytes([0x7f, 0x00]);
    await assertRejects(
      async () => await buf1.matchFloat(buf2),
      ReadBufferError,
    );
    await assertRejects(
      async () => await buf1.matchDouble(buf2),
      ReadBufferError,
    );
  });

  it("matchDouble returns 1 when first value is greater", async () => {
    const tap1 = newTap(16);
    const tap2 = newTap(16);
    await tap1.writeDouble(123.456);
    await tap2.writeDouble(-10.5);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchDouble(tap2)).toBeGreaterThan(0);
  });

  it("matchFixed compares fixed-size byte arrays", async () => {
    const tap1 = newTap(4);
    const tap2 = newTap(4);
    await tap1.writeFixed(toUint8Array([1, 2, 3, 4]));
    await tap2.writeFixed(toUint8Array([1, 2, 3, 5]));
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchFixed(tap2, 4)).toBeLessThan(0);
  });

  it("matchString and matchBytes compare encoded lengths and content", async () => {
    const tap1 = newTap(32);
    const tap2 = newTap(32);
    await tap1.writeString("abc");
    await tap2.writeString("abd");
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchString(tap2)).toBeLessThan(0);
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    await tap1.writeBytes(toUint8Array([1, 2, 3]));
    await tap2.writeBytes(toUint8Array([1, 2, 4]));
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchBytes(tap2)).toBeLessThan(0);
  });

  it("matchFixed rejects when data is unavailable", async () => {
    const tap1 = tapFromBytes([1, 2]);
    const tap2 = tapFromBytes([1]);
    await assertRejects(
      async () => await tap1.matchFixed(tap2, 4),
      ReadBufferError,
    );
  });

  it("matchFixed returns 1 when first array is greater", async () => {
    const tap1 = newTap(4);
    const tap2 = newTap(4);
    await tap1.writeFixed(toUint8Array([2, 0, 0, 0]));
    await tap2.writeFixed(toUint8Array([1, 255, 255, 255]));
    tap1._testOnlyResetPos();
    tap2._testOnlyResetPos();
    expect(await tap1.matchFixed(tap2, 4)).toBeGreaterThan(0);
  });

  it("matchDouble rejects when first tap runs out of data", async () => {
    const tap1 = tapFromBytes([0x00, 0x00, 0x00, 0x00]);
    const tap2 = tapFromBytes([
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
    ]);
    await assertRejects(
      async () => await tap1.matchDouble(tap2),
      ReadBufferError,
    );
  });

  it("matchDouble rejects when second tap runs out of data", async () => {
    const tap1 = tapFromBytes([
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
    ]);
    const tap2 = tapFromBytes([0x00, 0x00, 0x00, 0x00]);
    await assertRejects(
      async () => await tap1.matchDouble(tap2),
      ReadBufferError,
    );
  });

  it("matchFixed throws when read fails", async () => {
    const mockBuffer: IReadableBuffer = {
      read: (_offset: number, _size: number) =>
        Promise.reject(
          new ReadBufferError("Operation exceeds buffer bounds", 0, 1, 0),
        ),
      canReadMore: () => Promise.resolve(false),
    };
    const tap1 = new ReadableTap(mockBuffer);
    const tap2 = new ReadableTap(mockBuffer);
    await assertRejects(
      async () => await tap1.matchFixed(tap2, 1),
      ReadBufferError,
    );
  });
});

describe("WritableTap byte emission", () => {
  it("writeFixed uses buffer length when len is omitted", async () => {
    const tap = newTap(4);
    const payload = toUint8Array([10, 20, 30, 40]);
    await tap.writeFixed(payload);
    expect(tap.getPos()).toBe(payload.length);
    const buf = await tap._testOnlyBuf();
    expectUint8ArrayEqual(buf.subarray(0, payload.length), payload);
  });

  it("writeBinary within bounds", async () => {
    const tap = newTap(3);
    await tap.writeBinary("\x01\x02", 2);
    const expected = toUint8Array([1, 2, 0]);
    expectUint8ArrayEqual(await tap._testOnlyBuf(), expected);
  });

  it("writeBinary overflow rejects when buffer is too small", async () => {
    const tap = newTap(1);
    await assertRejects(
      async () => await tap.writeBinary("\x01\x02", 2),
      WriteBufferError,
    );
    const expected = toUint8Array([0]);
    const buf = await tap._testOnlyBuf();
    expectUint8ArrayEqual(buf.subarray(0, 1), expected);
  });

  it("writeBinary with len=0 does nothing", async () => {
    const tap = newTap(10);
    const initialPos = tap.getPos();
    await tap.writeBinary("abc", 0);
    expect(tap.getPos()).toBe(initialPos);
  });

  it("writeFixed with empty buffer does nothing", async () => {
    const tap = newTap(10);
    const initialPos = tap.getPos();
    await tap.writeFixed(new Uint8Array(0));
    expect(tap.getPos()).toBe(initialPos);
  });
});

describe("Numeric guard rails", () => {
  it("readInt throws when value exceeds safe integer range", async () => {
    const tap = newTap(16);
    const big = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    await tap.writeLong(big);
    tap._testOnlyResetPos();
    await assertRejects(async () => await tap.readInt(), RangeError);
  });

  it("readBytes throws when length exceeds safe integer range", async () => {
    const tap = newTap(16);
    const big = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    await tap.writeLong(big);
    tap._testOnlyResetPos();
    await assertRejects(async () => await tap.readBytes(), RangeError);
  });

  it("readInt, writeInt, and skipInt delegate to long helpers", async () => {
    const tap = newTap(16);
    await tap.writeInt(42);
    await tap.writeInt(-7);
    const afterWrites = tap.getPos();
    tap._testOnlyResetPos();
    expect(await tap.readInt()).toBe(42);
    await tap.skipInt();
    expect(tap.getPos()).toBe(afterWrites);
  });

  it("writeInt throws when value exceeds 32-bit range", async () => {
    const tap = newTap(16);
    // 2147483648 is INT32_MAX + 1
    await assertRejects(async () => await tap.writeInt(2147483648), RangeError);
    // -2147483649 is INT32_MIN - 1
    await assertRejects(
      async () => await tap.writeInt(-2147483649),
      RangeError,
    );
    // 9007199254740991 exceeds INT32_MAX
    await assertRejects(
      async () => await tap.writeInt(Number.MAX_SAFE_INTEGER),
      RangeError,
    );
    // -9007199254740991 is below INT32_MIN
    await assertRejects(
      async () => await tap.writeInt(Number.MIN_SAFE_INTEGER),
      RangeError,
    );
    // 3.14 is not an integer
    await assertRejects(async () => await tap.writeInt(3.14), RangeError);
  });
});

describe("Construction & buffer compatibility", () => {
  it("constructor rejects non-ArrayBuffer input", () => {
    expect(() => new ReadableTap({} as unknown as ArrayBuffer)).toThrow(
      TypeError,
    );
    expect(() => new WritableTap({} as unknown as ArrayBuffer)).toThrow(
      TypeError,
    );
  });

  it("ReadableTap accepts ReadableBuffer", async () => {
    const total = 10;
    const mockBuffer: IReadableBuffer = {
      read: (offset: number, size: number) => {
        if (offset + size > total) {
          return Promise.reject(
            new ReadBufferError(
              "Operation exceeds buffer bounds",
              offset,
              size,
              total,
            ),
          );
        }
        return Promise.resolve(new Uint8Array(size).fill(42));
      },
      canReadMore: (offset: number) => Promise.resolve(offset < total),
    };
    const tap = new ReadableTap(mockBuffer);
    expect(await tap.isValid()).toBe(true);
    expect(await tap.readBoolean()).toBe(true);
  });

  it("WritableTap accepts WritableBuffer", async () => {
    const writes: Array<{ offset: number; data: Uint8Array }> = [];
    let offset = 0;
    const mockBuffer: IWritableBuffer = {
      // deno-lint-ignore require-await
      appendBytes: async (data: Uint8Array) => {
        writes.push({ offset, data: data.slice() });
        offset += data.length;
      },
      // deno-lint-ignore require-await
      isValid: async () => true,
      // deno-lint-ignore require-await
      canAppendMore: async (_size: number) => true,
    };
    const tap = new WritableTap(mockBuffer);
    await tap.writeBoolean(true);
    expect(await tap.isValid()).toBe(true);
    expect(writes).toHaveLength(1);
    expect(Array.from(writes[0].data)).toEqual([1]);
    expect(writes[0].offset).toBe(0);
  });

  it("WritableTap isValid delegates to isValid()", async () => {
    let allowWrites = true;
    const mockBuffer: IWritableBuffer = {
      // deno-lint-ignore require-await
      appendBytes: async () => {
        allowWrites = false;
      },
      // deno-lint-ignore require-await
      isValid: async () => allowWrites,
      // deno-lint-ignore require-await
      canAppendMore: async (_size: number) => allowWrites,
    };
    const tap = new WritableTap(mockBuffer);
    expect(await tap.isValid()).toBe(true);
    await tap.writeBoolean(true);
    expect(await tap.isValid()).toBe(false);
  });

  it("appendRawBytes returns early for empty bytes", async () => {
    const mockBuffer: IWritableBuffer = {
      // deno-lint-ignore require-await
      appendBytes: async () => {
        throw new Error("appendBytes should not be called");
      },
      // deno-lint-ignore require-await
      isValid: async () => true,
      // deno-lint-ignore require-await
      canAppendMore: async (_size: number) => true,
    };
    const tap = new WritableTap(mockBuffer);
    const initialPos = tap.getPos();
    // deno-lint-ignore no-explicit-any
    await (tap as any).appendRawBytes(new Uint8Array(0));
    expect(tap.getPos()).toBe(initialPos);
  });

  it("constructor rejects invalid positions", () => {
    const buf = new ArrayBuffer(8);
    expect(() => new ReadableTap(buf, -1)).toThrow(ReadBufferError);
    expect(() => new ReadableTap(buf, 1.5)).toThrow(ReadBufferError);
    expect(() => new ReadableTap(buf, Number.MAX_SAFE_INTEGER + 1)).toThrow(
      ReadBufferError,
    );
    expect(() => new WritableTap(buf, -1)).toThrow(WriteBufferError);
    expect(() => new WritableTap(buf, 1.5)).toThrow(WriteBufferError);
    expect(() => new WritableTap(buf, Number.MAX_SAFE_INTEGER + 1)).toThrow(
      WriteBufferError,
    );
  });

  it("constructor rejects readable buffers missing canReadMore", () => {
    const mockBuffer = {
      // deno-lint-ignore require-await
      read: async (_offset: number, _size: number) => new Uint8Array(0),
    };
    expect(
      () => new ReadableTap(mockBuffer as unknown as IReadableBuffer),
    ).toThrow(TypeError);
  });

  it("isValid returns false when buffer.read throws ReadBufferError", async () => {
    const mockBuffer: IReadableBuffer = {
      read: () => {
        throw new ReadBufferError("Buffer exhausted", 0, 0, 0);
      },
      canReadMore: () => Promise.resolve(false),
    };
    const tap = new ReadableTap(mockBuffer);
    const result = await tap.isValid();
    expect(result).toBe(false);
  });

  it("isValid rethrows non-ReadBufferError exceptions", async () => {
    const customError = new Error("custom error");
    const mockBuffer: IReadableBuffer = {
      read: () => {
        throw customError;
      },
      canReadMore: () => Promise.resolve(false),
    };
    const tap = new ReadableTap(mockBuffer);
    await assertRejects(
      async () => await tap.isValid(),
      Error,
      "custom error",
    );
  });
});

describe("ReadableTap buffer exposure", () => {
  it("getValue returns written bytes", async () => {
    const tap = newTap(10);
    await tap.writeBoolean(true);
    await tap.writeBoolean(false);
    await tap.writeBoolean(true);
    expectUint8ArrayEqual(await tap.getValue(), toUint8Array([1, 0, 1]));
  });

  it("getValue throws when position exceeds buffer length", async () => {
    const tap = new ReadableTap(new ArrayBuffer(4), 10);
    await assertRejects(async () => await tap.getValue(), ReadBufferError);
  });

  it("_testOnlyBuf returns the full buffer", async () => {
    const buffer = new ArrayBuffer(5);
    const writer = new WritableTap(buffer);
    await writer.writeBoolean(true);
    await writer.writeBoolean(false);
    const buf = await new ReadableTap(buffer)._testOnlyBuf();
    expect(buf.length).toBe(5);
    expect(buf[0]).toBe(1);
    expect(buf[1]).toBe(0);
    expect(buf[2]).toBe(0);
    expect(buf[3]).toBe(0);
    expect(buf[4]).toBe(0);
  });

  it("_testOnlyBuf throws when read fails", async () => {
    const mockBuffer: IReadableBuffer = {
      read: (offset: number, size: number) =>
        Promise.reject(
          new ReadBufferError(
            "Operation exceeds buffer bounds",
            offset,
            size,
            0,
          ),
        ),
      canReadMore: () => Promise.resolve(false),
    };
    const tap = new ReadableTap(mockBuffer);
    // Set position to ensure readLength > 0 so we don't hit early return
    // deno-lint-ignore no-explicit-any
    (tap as any).pos = 1;
    await assertRejects(async () => await tap._testOnlyBuf(), ReadBufferError);
  });

  it("_testOnlyBuf throws when bytes is undefined", async () => {
    const mockBuffer: IReadableBuffer = {
      read: (offset: number, size: number) =>
        Promise.reject(
          new ReadBufferError(
            "Operation exceeds buffer bounds",
            offset,
            size,
            0,
          ),
        ),
      canReadMore: () => Promise.resolve(false),
    };
    const tap = new ReadableTap(mockBuffer);
    // Manually set position to ensure readLength > 0 so we don't hit early return
    // deno-lint-ignore no-explicit-any
    (tap as any).pos = 5;
    await assertRejects(async () => await tap._testOnlyBuf(), ReadBufferError);
  });

  it("_testOnlyBuf returns empty array when buffer is empty", async () => {
    const tap = new ReadableTap(new ArrayBuffer(0));
    const buf = await tap._testOnlyBuf();
    expect(buf).toEqual(new Uint8Array());
  });

  it("isValid rethrows unexpected errors from the buffer", async () => {
    const tap = new ReadableTap(new ErrorBuffer(new Error("boom")));
    await assertRejects(async () => await tap.isValid(), Error);
  });

  it("getValue throws when the buffer returns undefined", async () => {
    const tap = new ReadableTap(new UndefinedReadBuffer(2), 3);
    await assertRejects(async () => await tap.getValue(), ReadBufferError);
  });
});

describe("ReadableTap fallbacks", () => {
  it("readFloat throws when read fails", async () => {
    const mockBuffer: IReadableBuffer = {
      read: (offset: number, size: number) =>
        Promise.reject(
          new ReadBufferError(
            "Operation exceeds buffer bounds",
            offset,
            size,
            0,
          ),
        ),
      canReadMore: () => Promise.resolve(false),
    };
    const tap = new ReadableTap(mockBuffer);
    await assertRejects(async () => await tap.readFloat(), ReadBufferError);
  });

  it("readDouble throws when read fails", async () => {
    const mockBuffer: IReadableBuffer = {
      read: (offset: number, size: number) =>
        Promise.reject(
          new ReadBufferError(
            "Operation exceeds buffer bounds",
            offset,
            size,
            0,
          ),
        ),
      canReadMore: () => Promise.resolve(false),
    };
    const tap = new ReadableTap(mockBuffer);
    await assertRejects(async () => await tap.readDouble(), ReadBufferError);
  });

  it("matchString throws when content read fails", async () => {
    let callCount = 0;
    const mockBuffer: IReadableBuffer = {
      read: (offset: number, size: number) => {
        callCount++;
        if (callCount <= 2) { // Allow readLong to succeed for both taps
          return Promise.resolve(new Uint8Array([2])); // Varint for 1
        }
        return Promise.reject(
          new ReadBufferError(
            "Operation exceeds buffer bounds",
            offset,
            size,
            0,
          ),
        );
      },
      canReadMore: () => Promise.resolve(true),
    };
    const tap1 = new ReadableTap(mockBuffer);
    const tap2 = new ReadableTap(mockBuffer);
    await assertRejects(
      async () => await tap1.matchString(tap2),
      ReadBufferError,
    );
  });

  it("readString throws when read fails", async () => {
    const mockBuffer: IReadableBuffer = {
      read: (offset: number, size: number) =>
        Promise.reject(
          new ReadBufferError(
            "Operation exceeds buffer bounds",
            offset,
            size,
            0,
          ),
        ),
      canReadMore: () => Promise.resolve(false),
    };
    const tap = new ReadableTap(mockBuffer);
    await assertRejects(async () => await tap.readString(), ReadBufferError);
  });

  it("readBytes throws when read fails", async () => {
    const mockBuffer: IReadableBuffer = {
      read: (offset: number, size: number) =>
        Promise.reject(
          new ReadBufferError(
            "Operation exceeds buffer bounds",
            offset,
            size,
            0,
          ),
        ),
      canReadMore: () => Promise.resolve(false),
    };
    const tap = new ReadableTap(mockBuffer);
    await assertRejects(async () => await tap.readBytes(), ReadBufferError);
  });

  it("readFixed throws when read fails", async () => {
    const mockBuffer: IReadableBuffer = {
      read: (offset: number, size: number) =>
        Promise.reject(
          new ReadBufferError(
            "Operation exceeds buffer bounds",
            offset,
            size,
            0,
          ),
        ),
      canReadMore: () => Promise.resolve(false),
    };
    const tap = new ReadableTap(mockBuffer);
    await assertRejects(async () => await tap.readFixed(1), ReadBufferError);
  });
});

describe("Long decoding edge cases", () => {
  it("writeLong rejects values exceeding int64 range", async () => {
    // Values > int64 are out of spec for Avro long and must be rejected
    // Avro spec defines long as int64 (-2^63 to 2^63-1)
    const large = (1n << 70n) + 123n; // Out of spec value
    const tap = newTap(16);
    await assertRejects(
      async () => await tap.writeLong(large),
      RangeError,
      "out of range for Avro long",
    );
  });

  it("readLong correctly handles max int64", async () => {
    const maxInt64 = (1n << 63n) - 1n;
    const tap = newTap(16);
    await tap.writeLong(maxInt64);
    tap._testOnlyResetPos();
    expect(await tap.readLong()).toBe(maxInt64);
  });

  it("readLong correctly handles min int64", async () => {
    const minInt64 = -(1n << 63n);
    const tap = newTap(16);
    await tap.writeLong(minInt64);
    tap._testOnlyResetPos();
    expect(await tap.readLong()).toBe(minInt64);
  });

  it("readLong correctly decodes near 2^56", async () => {
    const near = (1n << 56n) + 1n;
    const tap = newTap(16);
    await tap.writeLong(near);
    tap._testOnlyResetPos();
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

describe("canReadMore", () => {
  it("returns true when data is available at current position", async () => {
    const tap = tapFromBytes([1, 2, 3]);
    expect(await tap.canReadMore()).toBe(true);
  });

  it("returns false when at end of buffer", async () => {
    const tap = tapFromBytes([1, 2, 3]);
    tap._testOnlyResetPos();
    await tap.readFixed(3); // Read all data
    expect(await tap.canReadMore()).toBe(false);
  });

  it("returns false for empty buffer", async () => {
    const tap = new Tap(new ArrayBuffer(0));
    expect(await tap.canReadMore()).toBe(false);
  });

  it("does not advance cursor position", async () => {
    const tap = tapFromBytes([1, 2, 3]);
    const initialPos = tap.getPos();
    await tap.canReadMore();
    expect(tap.getPos()).toBe(initialPos);
  });

  it("returns true when buffer has data beyond current position", async () => {
    const tap = tapFromBytes([1, 2, 3, 4, 5]);
    await tap.readFixed(2); // Read first 2 bytes
    expect(await tap.canReadMore()).toBe(true);
  });

  it("works with mock buffer that throws ReadBufferError", async () => {
    const mockBuffer: IReadableBuffer = {
      read: (offset: number, size: number) =>
        Promise.reject(
          new ReadBufferError(
            "Operation exceeds buffer bounds",
            offset,
            size,
            0,
          ),
        ),
      canReadMore: () => Promise.resolve(false),
    };
    const tap = new ReadableTap(mockBuffer);
    expect(await tap.canReadMore()).toBe(false);
  });

  it("works with mock buffer that returns empty Uint8Array", async () => {
    const mockBuffer: IReadableBuffer = {
      read: (_offset: number, size: number) =>
        Promise.resolve(new Uint8Array(size)),
      canReadMore: () => Promise.resolve(true),
    };
    const tap = new ReadableTap(mockBuffer);
    expect(await tap.canReadMore()).toBe(true);
  });

  it("handles buffer read errors gracefully", async () => {
    const mockBuffer: IReadableBuffer = {
      read: () => Promise.reject(new Error("read error")),
      canReadMore: () => Promise.reject(new Error("read error")),
    };
    const tap = new ReadableTap(mockBuffer);
    await assertRejects(async () => await tap.canReadMore(), Error);
  });
});

describe("Cursor management", () => {
  it("_testOnlyResetPos resets position to 0", async () => {
    const buffer = new ArrayBuffer(10);
    const tap = new WritableTap(buffer);
    await tap.writeBoolean(true);
    expect(tap.getPos()).toBe(1);
    tap._testOnlyResetPos();
    expect(tap.getPos()).toBe(0);
  });
});

describe("WritableTap large length writeLong fallback", () => {
  it("writeBytes uses writeLong when length > 0x7FFFFFFF", async () => {
    // Create a mock Uint8Array-like object with a huge length but no actual data
    const hugeLength = 0x80000000; // 2^31, just over the threshold
    const mockBuf = {
      length: hugeLength,
    } as unknown as Uint8Array;

    // Track which write methods are called
    const calls: Array<{ method: string; value: number | bigint }> = [];
    const mockBuffer: IWritableBuffer = {
      appendBytes: () => Promise.resolve(),
      isValid: () => Promise.resolve(true),
      canAppendMore: () => Promise.resolve(true),
    };

    const tap = new WritableTap(mockBuffer);
    // Override writeInt and writeLong to track calls
    const originalWriteInt = tap.writeInt.bind(tap);
    const originalWriteLong = tap.writeLong.bind(tap);
    // deno-lint-ignore require-await
    tap.writeInt = async (value: number) => {
      calls.push({ method: "writeInt", value });
      return originalWriteInt(value);
    };
    // deno-lint-ignore require-await
    tap.writeLong = async (value: bigint) => {
      calls.push({ method: "writeLong", value });
      return originalWriteLong(value);
    };

    await tap.writeBytes(mockBuf);

    // Should have called writeLong for the length
    expect(calls.length).toBe(1);
    expect(calls[0].method).toBe("writeLong");
    expect(calls[0].value).toBe(BigInt(hugeLength));
  });

  it("writeString uses writeLong when encoded length > 0x7FFFFFFF", async () => {
    // Mock encoder.encode to return a Uint8Array-like object with huge length
    const hugeLength = 0x80000000; // 2^31, just over the threshold
    const originalEncode = encoder.encode.bind(encoder);

    const mockBuffer: IWritableBuffer = {
      appendBytes: () => Promise.resolve(),
      isValid: () => Promise.resolve(true),
      canAppendMore: () => Promise.resolve(true),
    };

    const tap = new WritableTap(mockBuffer);

    // Track which write methods are called
    const calls: Array<{ method: string; value: number | bigint }> = [];
    const originalWriteInt = tap.writeInt.bind(tap);
    const originalWriteLong = tap.writeLong.bind(tap);
    // deno-lint-ignore require-await
    tap.writeInt = async (value: number) => {
      calls.push({ method: "writeInt", value });
      return originalWriteInt(value);
    };
    // deno-lint-ignore require-await
    tap.writeLong = async (value: bigint) => {
      calls.push({ method: "writeLong", value });
      return originalWriteLong(value);
    };

    try {
      // Mock encoder.encode to return a fake huge Uint8Array
      // deno-lint-ignore no-explicit-any
      (encoder as any).encode = (_str: string) => {
        return { length: hugeLength } as unknown as Uint8Array;
      };

      await tap.writeString("test");
    } finally {
      // Restore original encoder.encode
      encoder.encode = originalEncode;
    }

    // Should have called writeLong for the length
    expect(calls.length).toBe(1);
    expect(calls[0].method).toBe("writeLong");
    expect(calls[0].value).toBe(BigInt(hugeLength));
  });
});
