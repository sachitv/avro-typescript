import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { assertThrows } from "@std/assert";

import { DirectSyncReadableTap } from "../direct_tap_sync.ts";
import { SyncWritableTap } from "../tap_sync.ts";
import { SyncInMemoryWritableBuffer } from "../buffers/in_memory_buffer_sync.ts";

// Helper to create a DirectSyncReadableTap from values written by SyncWritableTap
function createTapWithWrites(
  writes: (tap: SyncWritableTap) => void,
  size = 256,
): DirectSyncReadableTap {
  const buffer = new ArrayBuffer(size);
  const writer = new SyncWritableTap(new SyncInMemoryWritableBuffer(buffer));
  writes(writer);
  return new DirectSyncReadableTap(new Uint8Array(buffer, 0, writer.getPos()));
}

describe("DirectSyncReadableTap", () => {
  describe("constructor and position management", () => {
    it("initializes with default position 0", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf);
      expect(tap.pos).toBe(0);
    });

    it("initializes with custom position", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf, 2);
      expect(tap.pos).toBe(2);
    });

    it("allows setting position", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf);
      tap.pos = 1;
      expect(tap.pos).toBe(1);
    });
  });

  describe("isValid and canReadMore", () => {
    it("isValid returns true when position is within bounds", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf);
      expect(tap.isValid()).toBe(true);
    });

    it("isValid returns true when position equals buffer length", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf, 3);
      expect(tap.isValid()).toBe(true);
    });

    it("isValid returns false when position exceeds buffer length", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf, 4);
      expect(tap.isValid()).toBe(false);
    });

    it("isValid returns false when position is negative", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf, -1);
      expect(tap.isValid()).toBe(false);
    });

    it("canReadMore returns true when data remains", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf);
      expect(tap.canReadMore()).toBe(true);
    });

    it("canReadMore returns false when at end", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf, 3);
      expect(tap.canReadMore()).toBe(false);
    });
  });

  describe("getValue", () => {
    it("returns empty array at start", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf);
      expect(tap.getValue()).toEqual(new Uint8Array(0));
    });

    it("returns bytes read so far", () => {
      const buf = new Uint8Array([1, 2, 3]);
      const tap = new DirectSyncReadableTap(buf, 2);
      expect(tap.getValue()).toEqual(new Uint8Array([1, 2]));
    });
  });

  describe("readBoolean and skipBoolean", () => {
    it("reads true", () => {
      const tap = createTapWithWrites((t) => t.writeBoolean(true));
      expect(tap.readBoolean()).toBe(true);
    });

    it("reads false", () => {
      const tap = createTapWithWrites((t) => t.writeBoolean(false));
      expect(tap.readBoolean()).toBe(false);
    });

    it("skips boolean", () => {
      const tap = createTapWithWrites((t) => {
        t.writeBoolean(true);
        t.writeInt(42);
      });
      tap.skipBoolean();
      expect(tap.readInt()).toBe(42);
    });
  });

  describe("readInt and skipInt", () => {
    it("reads positive int", () => {
      const tap = createTapWithWrites((t) => t.writeInt(42));
      expect(tap.readInt()).toBe(42);
    });

    it("reads negative int", () => {
      const tap = createTapWithWrites((t) => t.writeInt(-123));
      expect(tap.readInt()).toBe(-123);
    });

    it("reads INT32_MAX", () => {
      const tap = createTapWithWrites((t) => t.writeInt(2147483647));
      expect(tap.readInt()).toBe(2147483647);
    });

    it("reads INT32_MIN", () => {
      const tap = createTapWithWrites((t) => t.writeInt(-2147483648));
      expect(tap.readInt()).toBe(-2147483648);
    });

    it("throws on 5th byte with high bits set", () => {
      // Create a buffer with an invalid 5-byte varint (high bits in 5th byte)
      const bytes = new Uint8Array([0x80, 0x80, 0x80, 0x80, 0xF0]);
      const tap = new DirectSyncReadableTap(bytes);
      assertThrows(
        () => tap.readInt(),
        RangeError,
        "5th byte of varint has bits above 0x0F set",
      );
    });

    it("skipInt advances past varint", () => {
      const tap = createTapWithWrites((t) => {
        t.writeInt(42);
        t.writeInt(100);
      });
      tap.skipInt();
      expect(tap.readInt()).toBe(100);
    });
  });

  describe("readLong and skipLong", () => {
    it("reads positive long", () => {
      const tap = createTapWithWrites((t) => t.writeLong(123456789n));
      expect(tap.readLong()).toBe(123456789n);
    });

    it("reads negative long", () => {
      const tap = createTapWithWrites((t) => t.writeLong(-987654321n));
      expect(tap.readLong()).toBe(-987654321n);
    });

    it("reads INT64_MAX", () => {
      const tap = createTapWithWrites((t) => t.writeLong(9223372036854775807n));
      expect(tap.readLong()).toBe(9223372036854775807n);
    });

    it("reads INT64_MIN", () => {
      const tap = createTapWithWrites((t) =>
        t.writeLong(-9223372036854775808n)
      );
      expect(tap.readLong()).toBe(-9223372036854775808n);
    });

    it("throws on varint exceeding 64 bits", () => {
      // Create a buffer with 11 continuation bytes (exceeds 10 byte max)
      const bytes = new Uint8Array([
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80, // 10 bytes with continuation
        0x01, // 11th byte
      ]);
      const tap = new DirectSyncReadableTap(bytes);
      assertThrows(
        () => tap.readLong(),
        RangeError,
        "Varint requires more than 10 bytes",
      );
    });

    it("skipLong advances past varint", () => {
      const tap = createTapWithWrites((t) => {
        t.writeLong(123456789n);
        t.writeLong(987654321n);
      });
      tap.skipLong();
      expect(tap.readLong()).toBe(987654321n);
    });
  });

  describe("readFloat and skipFloat", () => {
    it("reads float", () => {
      const tap = createTapWithWrites((t) => t.writeFloat(3.14));
      expect(tap.readFloat()).toBeCloseTo(3.14, 5);
    });

    it("reads negative float", () => {
      const tap = createTapWithWrites((t) => t.writeFloat(-2.5));
      expect(tap.readFloat()).toBeCloseTo(-2.5, 5);
    });

    it("skipFloat advances 4 bytes", () => {
      const tap = createTapWithWrites((t) => {
        t.writeFloat(1.0);
        t.writeInt(42);
      });
      tap.skipFloat();
      expect(tap.readInt()).toBe(42);
    });
  });

  describe("readDouble and skipDouble", () => {
    it("reads double", () => {
      const tap = createTapWithWrites((t) => t.writeDouble(3.141592653589793));
      expect(tap.readDouble()).toBeCloseTo(3.141592653589793, 10);
    });

    it("reads negative double", () => {
      const tap = createTapWithWrites((t) => t.writeDouble(-123.456789));
      expect(tap.readDouble()).toBeCloseTo(-123.456789, 10);
    });

    it("skipDouble advances 8 bytes", () => {
      const tap = createTapWithWrites((t) => {
        t.writeDouble(1.0);
        t.writeInt(42);
      });
      tap.skipDouble();
      expect(tap.readInt()).toBe(42);
    });
  });

  describe("readFixed and skipFixed", () => {
    it("reads fixed bytes", () => {
      const tap = createTapWithWrites((t) =>
        t.writeFixed(new Uint8Array([1, 2, 3, 4]))
      );
      expect(tap.readFixed(4)).toEqual(new Uint8Array([1, 2, 3, 4]));
    });

    it("skipFixed advances by length", () => {
      const tap = createTapWithWrites((t) => {
        t.writeFixed(new Uint8Array([1, 2, 3, 4]));
        t.writeInt(42);
      });
      tap.skipFixed(4);
      expect(tap.readInt()).toBe(42);
    });
  });

  describe("readBytes and skipBytes", () => {
    it("reads bytes", () => {
      const tap = createTapWithWrites((t) =>
        t.writeBytes(new Uint8Array([5, 6, 7]))
      );
      expect(tap.readBytes()).toEqual(new Uint8Array([5, 6, 7]));
    });

    it("reads empty bytes", () => {
      const tap = createTapWithWrites((t) => t.writeBytes(new Uint8Array(0)));
      expect(tap.readBytes()).toEqual(new Uint8Array(0));
    });

    it("throws on negative bytes length", () => {
      // Manually write a negative varint for length
      const tap = createTapWithWrites((t) => t.writeLong(-5n));
      assertThrows(
        () => tap.readBytes(),
        RangeError,
        "Invalid negative bytes length",
      );
    });

    it("skipBytes advances past length-prefixed bytes", () => {
      const tap = createTapWithWrites((t) => {
        t.writeBytes(new Uint8Array([1, 2, 3]));
        t.writeInt(42);
      });
      tap.skipBytes();
      expect(tap.readInt()).toBe(42);
    });

    it("skipBytes throws on negative length", () => {
      const tap = createTapWithWrites((t) => t.writeLong(-5n));
      assertThrows(
        () => tap.skipBytes(),
        RangeError,
        "Invalid negative bytes length",
      );
    });
  });

  describe("readString and skipString", () => {
    it("reads string", () => {
      const tap = createTapWithWrites((t) => t.writeString("hello"));
      expect(tap.readString()).toBe("hello");
    });

    it("reads empty string", () => {
      const tap = createTapWithWrites((t) => t.writeString(""));
      expect(tap.readString()).toBe("");
    });

    it("reads unicode string", () => {
      const tap = createTapWithWrites((t) => t.writeString("こんにちは"));
      expect(tap.readString()).toBe("こんにちは");
    });

    it("throws on negative string length", () => {
      const tap = createTapWithWrites((t) => t.writeLong(-5n));
      assertThrows(
        () => tap.readString(),
        RangeError,
        "Invalid negative string length",
      );
    });

    it("skipString advances past length-prefixed string", () => {
      const tap = createTapWithWrites((t) => {
        t.writeString("hello");
        t.writeInt(42);
      });
      tap.skipString();
      expect(tap.readInt()).toBe(42);
    });

    it("skipString throws on negative length", () => {
      const tap = createTapWithWrites((t) => t.writeLong(-5n));
      assertThrows(
        () => tap.skipString(),
        RangeError,
        "Invalid negative string length",
      );
    });
  });

  describe("match methods", () => {
    it("matchBoolean compares equal values", () => {
      const tap1 = createTapWithWrites((t) => t.writeBoolean(true));
      const tap2 = createTapWithWrites((t) => t.writeBoolean(true));
      expect(tap1.matchBoolean(tap2)).toBe(0);
    });

    it("matchBoolean compares true > false", () => {
      const tap1 = createTapWithWrites((t) => t.writeBoolean(true));
      const tap2 = createTapWithWrites((t) => t.writeBoolean(false));
      expect(tap1.matchBoolean(tap2)).toBe(1);
    });

    it("matchBoolean compares false < true", () => {
      const tap1 = createTapWithWrites((t) => t.writeBoolean(false));
      const tap2 = createTapWithWrites((t) => t.writeBoolean(true));
      expect(tap1.matchBoolean(tap2)).toBe(-1);
    });

    it("matchInt delegates to matchLong", () => {
      const tap1 = createTapWithWrites((t) => t.writeInt(10));
      const tap2 = createTapWithWrites((t) => t.writeInt(20));
      expect(tap1.matchInt(tap2)).toBe(-1);
    });

    it("matchLong compares equal values", () => {
      const tap1 = createTapWithWrites((t) => t.writeLong(100n));
      const tap2 = createTapWithWrites((t) => t.writeLong(100n));
      expect(tap1.matchLong(tap2)).toBe(0);
    });

    it("matchLong compares less than", () => {
      const tap1 = createTapWithWrites((t) => t.writeLong(50n));
      const tap2 = createTapWithWrites((t) => t.writeLong(100n));
      expect(tap1.matchLong(tap2)).toBe(-1);
    });

    it("matchLong compares greater than", () => {
      const tap1 = createTapWithWrites((t) => t.writeLong(200n));
      const tap2 = createTapWithWrites((t) => t.writeLong(100n));
      expect(tap1.matchLong(tap2)).toBe(1);
    });

    it("matchFloat compares equal values", () => {
      const tap1 = createTapWithWrites((t) => t.writeFloat(1.5));
      const tap2 = createTapWithWrites((t) => t.writeFloat(1.5));
      expect(tap1.matchFloat(tap2)).toBe(0);
    });

    it("matchFloat compares less than", () => {
      const tap1 = createTapWithWrites((t) => t.writeFloat(1.0));
      const tap2 = createTapWithWrites((t) => t.writeFloat(2.0));
      expect(tap1.matchFloat(tap2)).toBe(-1);
    });

    it("matchFloat compares greater than", () => {
      const tap1 = createTapWithWrites((t) => t.writeFloat(3.0));
      const tap2 = createTapWithWrites((t) => t.writeFloat(2.0));
      expect(tap1.matchFloat(tap2)).toBe(1);
    });

    it("matchDouble compares equal values", () => {
      const tap1 = createTapWithWrites((t) => t.writeDouble(1.5));
      const tap2 = createTapWithWrites((t) => t.writeDouble(1.5));
      expect(tap1.matchDouble(tap2)).toBe(0);
    });

    it("matchDouble compares less than", () => {
      const tap1 = createTapWithWrites((t) => t.writeDouble(1.0));
      const tap2 = createTapWithWrites((t) => t.writeDouble(2.0));
      expect(tap1.matchDouble(tap2)).toBe(-1);
    });

    it("matchDouble compares greater than", () => {
      const tap1 = createTapWithWrites((t) => t.writeDouble(3.0));
      const tap2 = createTapWithWrites((t) => t.writeDouble(2.0));
      expect(tap1.matchDouble(tap2)).toBe(1);
    });

    it("matchFixed compares byte arrays", () => {
      const tap1 = createTapWithWrites((t) =>
        t.writeFixed(new Uint8Array([1, 2, 3]))
      );
      const tap2 = createTapWithWrites((t) =>
        t.writeFixed(new Uint8Array([1, 2, 4]))
      );
      expect(tap1.matchFixed(tap2, 3)).toBe(-1);
    });

    it("matchBytes delegates to matchString", () => {
      const tap1 = createTapWithWrites((t) =>
        t.writeBytes(new Uint8Array([1, 2]))
      );
      const tap2 = createTapWithWrites((t) =>
        t.writeBytes(new Uint8Array([1, 3]))
      );
      expect(tap1.matchBytes(tap2)).toBe(-1);
    });

    it("matchString compares byte arrays", () => {
      const tap1 = createTapWithWrites((t) => t.writeString("abc"));
      const tap2 = createTapWithWrites((t) => t.writeString("abd"));
      expect(tap1.matchString(tap2)).toBe(-1);
    });
  });

  describe("bulk read methods", () => {
    describe("readIntArrayInto", () => {
      it("reads array of ints", () => {
        const tap = createTapWithWrites((t) => {
          t.writeInt(1);
          t.writeInt(2);
          t.writeInt(3);
        });
        const result: number[] = [];
        result.length = 3;
        tap.readIntArrayInto(result, 0, 3);
        expect(result).toEqual([1, 2, 3]);
      });

      it("reads with start index", () => {
        const tap = createTapWithWrites((t) => {
          t.writeInt(10);
          t.writeInt(20);
        });
        const result: number[] = [0, 0, 0, 0];
        tap.readIntArrayInto(result, 1, 2);
        expect(result).toEqual([0, 10, 20, 0]);
      });

      it("throws on int overflow in array element", () => {
        // Array with one valid int followed by an invalid one (5th byte with high bits)
        const bytes = new Uint8Array([
          0x02, // varint: 1
          0x80,
          0x80,
          0x80,
          0x80,
          0xF0, // invalid 5-byte varint
        ]);
        const tap = new DirectSyncReadableTap(bytes);
        const result: number[] = [0, 0];
        assertThrows(
          () => tap.readIntArrayInto(result, 0, 2),
          RangeError,
          "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
        );
      });
    });

    describe("readLongArrayInto", () => {
      it("reads array of longs", () => {
        const tap = createTapWithWrites((t) => {
          t.writeLong(100n);
          t.writeLong(200n);
          t.writeLong(300n);
        });
        const result: bigint[] = [];
        result.length = 3;
        tap.readLongArrayInto(result, 0, 3);
        expect(result).toEqual([100n, 200n, 300n]);
      });

      it("reads with start index", () => {
        const tap = createTapWithWrites((t) => {
          t.writeLong(10n);
          t.writeLong(20n);
        });
        const result: bigint[] = [0n, 0n, 0n, 0n];
        tap.readLongArrayInto(result, 2, 2);
        expect(result).toEqual([0n, 0n, 10n, 20n]);
      });

      it("throws on long overflow in array element", () => {
        // Array with one valid long followed by an 11-byte varint
        const bytes = new Uint8Array([
          0x02, // varint: 1n
          0x80,
          0x80,
          0x80,
          0x80,
          0x80,
          0x80,
          0x80,
          0x80,
          0x80,
          0x80, // 10 bytes with continuation
          0x01, // 11th byte exceeds int64 range
        ]);
        const tap = new DirectSyncReadableTap(bytes);
        const result: bigint[] = [0n, 0n];
        assertThrows(
          () => tap.readLongArrayInto(result, 0, 2),
          RangeError,
          "Varint requires more than 10 bytes (int64 range exceeded)",
        );
      });
    });

    describe("readFloatArrayInto", () => {
      it("reads array of floats", () => {
        const tap = createTapWithWrites((t) => {
          t.writeFloat(1.5);
          t.writeFloat(2.5);
          t.writeFloat(3.5);
        });
        const result: number[] = [];
        result.length = 3;
        tap.readFloatArrayInto(result, 0, 3);
        expect(result[0]).toBeCloseTo(1.5, 5);
        expect(result[1]).toBeCloseTo(2.5, 5);
        expect(result[2]).toBeCloseTo(3.5, 5);
      });
    });

    describe("readDoubleArrayInto", () => {
      it("reads array of doubles", () => {
        const tap = createTapWithWrites((t) => {
          t.writeDouble(1.111);
          t.writeDouble(2.222);
        });
        const result: number[] = [];
        result.length = 2;
        tap.readDoubleArrayInto(result, 0, 2);
        expect(result[0]).toBeCloseTo(1.111, 10);
        expect(result[1]).toBeCloseTo(2.222, 10);
      });
    });

    describe("readBooleanArrayInto", () => {
      it("reads array of booleans", () => {
        const tap = createTapWithWrites((t) => {
          t.writeBoolean(true);
          t.writeBoolean(false);
          t.writeBoolean(true);
        });
        const result: boolean[] = [];
        result.length = 3;
        tap.readBooleanArrayInto(result, 0, 3);
        expect(result).toEqual([true, false, true]);
      });
    });

    describe("readStringArrayInto", () => {
      it("reads array of strings", () => {
        const tap = createTapWithWrites((t) => {
          t.writeString("hello");
          t.writeString("world");
        });
        const result: string[] = [];
        result.length = 2;
        tap.readStringArrayInto(result, 0, 2);
        expect(result).toEqual(["hello", "world"]);
      });

      it("reads unicode strings", () => {
        const tap = createTapWithWrites((t) => {
          t.writeString("日本語");
          t.writeString("中文");
        });
        const result: string[] = [];
        result.length = 2;
        tap.readStringArrayInto(result, 0, 2);
        expect(result).toEqual(["日本語", "中文"]);
      });

      it("throws on string length overflow in array element", () => {
        // Array with one valid string followed by invalid length varint
        const bytes = new Uint8Array([
          0x0A, // length: 5
          0x68,
          0x65,
          0x6C,
          0x6C,
          0x6F, // "hello"
          0x80,
          0x80,
          0x80,
          0x80,
          0xF0, // invalid 5-byte varint for length
        ]);
        const tap = new DirectSyncReadableTap(bytes);
        const result: string[] = ["", ""];
        assertThrows(
          () => tap.readStringArrayInto(result, 0, 2),
          RangeError,
          "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
        );
      });
    });

    describe("readMapIntBlockInto", () => {
      it("reads map block with int values", () => {
        const tap = createTapWithWrites((t) => {
          t.writeString("a");
          t.writeInt(1);
          t.writeString("b");
          t.writeInt(2);
        });
        const result = new Map<string, number>();
        tap.readMapIntBlockInto(result, 2);
        expect(result.get("a")).toBe(1);
        expect(result.get("b")).toBe(2);
      });

      it("throws on key length overflow", () => {
        // Map entry with invalid key length varint
        const bytes = new Uint8Array([
          0x80,
          0x80,
          0x80,
          0x80,
          0xF0, // invalid 5-byte varint for key length
        ]);
        const tap = new DirectSyncReadableTap(bytes);
        const result = new Map<string, number>();
        assertThrows(
          () => tap.readMapIntBlockInto(result, 1),
          RangeError,
          "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
        );
      });

      it("throws on value overflow", () => {
        // Map entry with valid key but invalid int value
        const bytes = new Uint8Array([
          0x02, // key length: 1
          0x61, // "a"
          0x80,
          0x80,
          0x80,
          0x80,
          0xF0, // invalid 5-byte varint for value
        ]);
        const tap = new DirectSyncReadableTap(bytes);
        const result = new Map<string, number>();
        assertThrows(
          () => tap.readMapIntBlockInto(result, 1),
          RangeError,
          "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
        );
      });
    });

    describe("readMapStringBlockInto", () => {
      it("reads map block with string values", () => {
        const tap = createTapWithWrites((t) => {
          t.writeString("key1");
          t.writeString("value1");
          t.writeString("key2");
          t.writeString("value2");
        });
        const result = new Map<string, string>();
        tap.readMapStringBlockInto(result, 2);
        expect(result.get("key1")).toBe("value1");
        expect(result.get("key2")).toBe("value2");
      });

      it("reads unicode keys and values", () => {
        const tap = createTapWithWrites((t) => {
          t.writeString("キー");
          t.writeString("値");
        });
        const result = new Map<string, string>();
        tap.readMapStringBlockInto(result, 1);
        expect(result.get("キー")).toBe("値");
      });

      it("throws on key length overflow", () => {
        // Map entry with invalid key length varint
        const bytes = new Uint8Array([
          0x80,
          0x80,
          0x80,
          0x80,
          0xF0, // invalid 5-byte varint for key length
        ]);
        const tap = new DirectSyncReadableTap(bytes);
        const result = new Map<string, string>();
        assertThrows(
          () => tap.readMapStringBlockInto(result, 1),
          RangeError,
          "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
        );
      });

      it("throws on value length overflow", () => {
        // Map entry with valid key but invalid value length varint
        const bytes = new Uint8Array([
          0x02, // key length: 1
          0x61, // "a"
          0x80,
          0x80,
          0x80,
          0x80,
          0xF0, // invalid 5-byte varint for value length
        ]);
        const tap = new DirectSyncReadableTap(bytes);
        const result = new Map<string, string>();
        assertThrows(
          () => tap.readMapStringBlockInto(result, 1),
          RangeError,
          "5th byte of varint has bits above 0x0F set (int32 range exceeded)",
        );
      });
    });
  });

  describe("DataView offset handling", () => {
    it("handles Uint8Array with non-zero byteOffset", () => {
      // Create a buffer and a view into it with an offset
      const fullBuffer = new ArrayBuffer(16);
      const fullView = new Uint8Array(fullBuffer);

      // Write data at offset 4
      const writer = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(fullBuffer),
      );
      writer.writeFloat(3.14);
      const writtenBytes = writer.getPos();

      // Create a subarray starting at offset 0 but from the same buffer
      const subView = fullView.subarray(0, writtenBytes);
      const tap = new DirectSyncReadableTap(subView);
      expect(tap.readFloat()).toBeCloseTo(3.14, 5);
    });
  });
});
