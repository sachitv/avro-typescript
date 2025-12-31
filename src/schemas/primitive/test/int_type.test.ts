import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../../serialization/tap_sync.ts";
import { ReadBufferError } from "../../../serialization/buffers/buffer_sync.ts";
import { IntType } from "../int_type.ts";
import { ValidationError } from "../../error.ts";
import { calculateVarintSize } from "../../../internal/varint.ts";

describe("IntType", () => {
  const type = new IntType();

  describe("check", () => {
    it("should return true for valid integers within range", () => {
      assert(type.check(0));
      assert(type.check(42));
      assert(type.check(-42));
      assert(type.check(2147483647));
      assert(type.check(-2147483648));
    });

    it("should return false for out-of-range integers", () => {
      assert(!type.check(2147483648));
      assert(!type.check(-2147483649));
    });

    it("should return false for non-integers", () => {
      assert(!type.check(1.5));
      assert(!type.check("42"));
      assert(!type.check(null));
      assert(!type.check(undefined));
    });

    it("should call errorHook for invalid values", () => {
      let called = false;
      const errorHook = () => {
        called = true;
      };
      type.check("invalid", errorHook);
      assert(called);
    });
  });

  describe("read", () => {
    it("should read int from tap", async () => {
      const buffer = new ArrayBuffer(5);
      const writeTap = new Tap(buffer);
      await writeTap.writeInt(123);
      const readTap = new Tap(buffer);
      assertEquals(await type.read(readTap), 123);
    });
  });

  describe("write", () => {
    it("should write int to tap", async () => {
      const buffer = new ArrayBuffer(5);
      const writeTap = new Tap(buffer);
      await type.write(writeTap, 456);
      const readTap = new Tap(buffer);
      assertEquals(await readTap.readInt(), 456);
    });

    it("should throw for invalid value", async () => {
      const buffer = new ArrayBuffer(5);
      const tap = new Tap(buffer);
      await assertRejects(() => {
        return type.write(tap, 2147483648 as unknown as number);
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip int in tap", async () => {
      const value = 42;
      const size = calculateVarintSize(value);
      const buffer = new ArrayBuffer(size + 1);
      const tap = new Tap(buffer);
      await type.write(tap, value);
      const posAfterWrite = tap.getPos();
      assertEquals(posAfterWrite, size);
      tap._testOnlyResetPos();
      await type.skip(tap);
      const posAfterSkip = tap.getPos();
      assertEquals(posAfterSkip, size);
    });
  });

  describe("toBuffer", () => {
    it("should throw ValidationError for invalid value", async () => {
      await assertRejects(() => {
        return type.toBuffer(1.5 as unknown as number);
      }, ValidationError);
    });
  });

  describe("compare", () => {
    it("should compare numbers correctly", () => {
      assertEquals(type.compare(1, 2), -1);
      assertEquals(type.compare(2, 1), 1);
      assertEquals(type.compare(1, 1), 0);
    });

    it("should handle edge cases", () => {
      assertEquals(type.compare(-2147483648, 2147483647), -1);
      assertEquals(type.compare(2147483647, -2147483648), 1);
    });
  });

  describe("match", () => {
    it("should match encoded int buffers", async () => {
      const buf1 = await type.toBuffer(1);
      const buf2 = await type.toBuffer(2);
      const bufNeg = await type.toBuffer(-1);

      assertEquals(await type.match(new Tap(buf1), new Tap(buf2)), -1);
      assertEquals(await type.match(new Tap(buf2), new Tap(buf1)), 1);
      assertEquals(
        await type.match(new Tap(buf1), new Tap(await type.toBuffer(1))),
        0,
      );
      assertEquals(
        await type.match(new Tap(bufNeg), new Tap(await type.toBuffer(-1))),
        0,
      );
    });
  });

  describe("random", () => {
    it("should return a valid int", () => {
      const value = type.random();
      assert(typeof value === "number");
      assert(Number.isInteger(value));
      assert(value >= -2147483648 && value <= 2147483647);
    });
  });

  describe("toJSON", () => {
    it('should return "int"', () => {
      assertEquals(type.toJSON(), "int");
    });
  });

  describe("sync APIs", () => {
    describe("readSync", () => {
      it("should read int synchronously from tap", () => {
        const buffer = new ArrayBuffer(5);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeInt(123);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), 123);
      });

      it("should read negative int synchronously from tap", () => {
        const buffer = new ArrayBuffer(5);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeInt(-456);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), -456);
      });

      it("should read zero synchronously from tap", () => {
        const buffer = new ArrayBuffer(1);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeInt(0);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), 0);
      });

      it("should read max int synchronously from tap", () => {
        const buffer = new ArrayBuffer(5);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeInt(2147483647);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), 2147483647);
      });

      it("should read min int synchronously from tap", () => {
        const buffer = new ArrayBuffer(5);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeInt(-2147483648);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), -2147483648);
      });
    });

    describe("writeSync", () => {
      it("should write int synchronously to tap", () => {
        const buffer = new ArrayBuffer(5);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, 789);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readInt(), 789);
      });

      it("should write negative int synchronously to tap", () => {
        const buffer = new ArrayBuffer(5);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, -999);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readInt(), -999);
      });

      it("should write zero synchronously to tap", () => {
        const buffer = new ArrayBuffer(1);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, 0);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readInt(), 0);
      });

      it("should throw for invalid value", () => {
        const buffer = new ArrayBuffer(5);
        const tap = new SyncWritableTap(buffer);
        assertThrows(() => {
          type.writeSync(tap, 2147483648 as unknown as number);
        }, ValidationError);
      });

      it("should throw for non-integer value", () => {
        const buffer = new ArrayBuffer(5);
        const tap = new SyncWritableTap(buffer);
        assertThrows(() => {
          type.writeSync(tap, 1.5 as unknown as number);
        }, ValidationError);
      });
    });

    describe("skipSync", () => {
      it("should skip int synchronously in tap", () => {
        const value = 42;
        const size = calculateVarintSize(value);
        const buffer = new ArrayBuffer(size + 1);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, value);
        const posAfterWrite = writeTap.getPos();
        assertEquals(posAfterWrite, size);
        const readTap = new SyncReadableTap(buffer);
        type.skipSync(readTap);
        const posAfterSkip = readTap.getPos();
        assertEquals(posAfterSkip, size);
      });

      it("should skip negative int synchronously in tap", () => {
        const value = -1000;
        const size = calculateVarintSize(value);
        const buffer = new ArrayBuffer(size + 1);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, value);
        const readTap = new SyncReadableTap(buffer);
        type.skipSync(readTap);
        const posAfterSkip = readTap.getPos();
        assertEquals(posAfterSkip, size);
      });
    });

    describe("toSyncBuffer", () => {
      it("should serialize int to buffer synchronously", () => {
        const value = 12345;
        const buffer = type.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(tap), value);
      });

      it("should serialize negative int to buffer synchronously", () => {
        const value = -67890;
        const buffer = type.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(tap), value);
      });

      it("should serialize zero to buffer synchronously", () => {
        const value = 0;
        const buffer = type.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(tap), value);
      });

      it("should throw ValidationError for invalid value", () => {
        assertThrows(() => {
          type.toSyncBuffer(2147483648 as unknown as number);
        }, ValidationError);
      });

      it("should throw ValidationError for non-integer value", () => {
        assertThrows(() => {
          type.toSyncBuffer(2.5 as unknown as number);
        }, ValidationError);
      });
    });

    describe("fromSyncBuffer", () => {
      it("should deserialize int from buffer synchronously", () => {
        const value = 98765;
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should deserialize negative int from buffer synchronously", () => {
        const value = -54321;
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should deserialize zero from buffer synchronously", () => {
        const value = 0;
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should throw for truncated buffer", () => {
        const buffer = new ArrayBuffer(0);
        assertThrows(
          () => {
            type.fromSyncBuffer(buffer);
          },
          ReadBufferError,
          "Operation exceeds buffer bounds",
        );
      });

      it("should throw for insufficient buffer", () => {
        // Write a large int that requires more bytes
        const largeValue = 2147483647;
        const largeBuffer = type.toSyncBuffer(largeValue);
        // Try to read from a smaller buffer
        const smallBuffer = largeBuffer.slice(0, 1);
        assertThrows(
          () => {
            type.fromSyncBuffer(smallBuffer);
          },
          ReadBufferError,
          "Operation exceeds buffer bounds",
        );
      });
    });

    describe("matchSync", () => {
      it("should match encoded int buffers synchronously", () => {
        const buf1 = type.toSyncBuffer(1);
        const buf2 = type.toSyncBuffer(2);
        const bufNeg = type.toSyncBuffer(-1);
        const bufZero = type.toSyncBuffer(0);

        assertEquals(
          type.matchSync(new SyncReadableTap(buf1), new SyncReadableTap(buf2)),
          -1,
        );
        assertEquals(
          type.matchSync(new SyncReadableTap(buf2), new SyncReadableTap(buf1)),
          1,
        );
        assertEquals(
          type.matchSync(
            new SyncReadableTap(buf1),
            new SyncReadableTap(type.toSyncBuffer(1)),
          ),
          0,
        );
        assertEquals(
          type.matchSync(
            new SyncReadableTap(bufNeg),
            new SyncReadableTap(type.toSyncBuffer(-1)),
          ),
          0,
        );
        assertEquals(
          type.matchSync(
            new SyncReadableTap(bufZero),
            new SyncReadableTap(type.toSyncBuffer(0)),
          ),
          0,
        );
      });

      it("should handle edge cases in matching", () => {
        const minBuf = type.toSyncBuffer(-2147483648);
        const maxBuf = type.toSyncBuffer(2147483647);

        assertEquals(
          type.matchSync(
            new SyncReadableTap(minBuf),
            new SyncReadableTap(maxBuf),
          ),
          -1,
        );
        assertEquals(
          type.matchSync(
            new SyncReadableTap(maxBuf),
            new SyncReadableTap(minBuf),
          ),
          1,
        );
      });
    });
  });

  describe("inheritance from PrimitiveType and BaseType", () => {
    it("should clone int values", () => {
      assertEquals(type.cloneFromValue(42), 42);
      assertEquals(type.cloneFromValue(-42), -42);
    });

    it("should throw ValidationError for invalid clone", () => {
      assertThrows(() => {
        type.cloneFromValue(2147483648 as unknown as number);
      }, ValidationError);
    });

    it("should have toBuffer and fromBuffer", async () => {
      const value = 123;
      const buffer = await type.toBuffer(value);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should have isValid", () => {
      assert(type.isValid(42));
      assert(!type.isValid(2147483648));
    });

    it("should create resolver for same type", async () => {
      const resolver = type.createResolver(type);
      const value = 789;
      const buffer = await type.toBuffer(value);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, value);
    });

    it("should throw error for different type", () => {
      // Create a fake different type
      class FakeType extends IntType {
        // Different class
      }
      const otherType = new FakeType();
      assertThrows(
        () => {
          type.createResolver(otherType);
        },
        Error,
        "Schema evolution not supported from writer type: int to reader type: int",
      );
    });
  });
});
