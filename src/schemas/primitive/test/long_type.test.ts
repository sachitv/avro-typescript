import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../../serialization/sync_tap.ts";
import { LongType } from "../long_type.ts";
import { IntType } from "../int_type.ts";
import { ValidationError } from "../../error.ts";
import { calculateVarintSize } from "../../../internal/varint.ts";

describe("LongType", () => {
  const type = new LongType();
  const minLong = -(1n << 63n);
  const maxLong = (1n << 63n) - 1n;

  describe("check", () => {
    it("should return true for bigint values", () => {
      assert(type.check(0n));
      assert(type.check(42n));
      assert(type.check(-42n));
      assert(type.check(minLong));
      assert(type.check(maxLong));
    });

    it("should return false for non-bigint values", () => {
      assert(!type.check(42));
      assert(!type.check("42"));
      assert(!type.check(null));
      assert(!type.check(undefined));
    });

    it("should return false for out-of-range bigint values", () => {
      assert(!type.check(maxLong + 1n));
      assert(!type.check(minLong - 1n));
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
    it("should read long from tap", async () => {
      const buffer = new ArrayBuffer(10);
      const writeTap = new Tap(buffer);
      await writeTap.writeLong(123n);
      const readTap = new Tap(buffer);
      assertEquals(await type.read(readTap), 123n);
    });
  });

  describe("write", () => {
    it("should write long to tap", async () => {
      const buffer = new ArrayBuffer(10);
      const writeTap = new Tap(buffer);
      await type.write(writeTap, 456n);
      const readTap = new Tap(buffer);
      assertEquals(await readTap.readLong(), 456n);
    });

    it("should throw for out-of-range value", async () => {
      const buffer = new ArrayBuffer(10);
      const tap = new Tap(buffer);
      await assertRejects(() => {
        return type.write(tap, maxLong + 1n);
      }, ValidationError);
    });

    it("should throw for invalid value", async () => {
      const buffer = new ArrayBuffer(10);
      const tap = new Tap(buffer);
      await assertRejects(() => {
        return type.write(tap, 42 as unknown as bigint);
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip long in tap", async () => {
      const value = 42n;
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
        return type.toBuffer(42 as unknown as bigint);
      }, ValidationError);
    });
  });

  describe("compare", () => {
    it("should compare bigints correctly", () => {
      assertEquals(type.compare(1n, 2n), -1);
      assertEquals(type.compare(2n, 1n), 1);
      assertEquals(type.compare(1n, 1n), 0);
    });

    it("should handle edge cases", () => {
      assertEquals(type.compare(minLong, maxLong), -1);
      assertEquals(type.compare(maxLong, minLong), 1);
    });
  });

  describe("match", () => {
    it("should match encoded long buffers", async () => {
      const buf1 = await type.toBuffer(1n);
      const buf2 = await type.toBuffer(2n);
      const buf3 = await type.toBuffer(1n);

      assertEquals(await type.match(new Tap(buf1), new Tap(buf2)), -1); // 1n < 2n
      assertEquals(await type.match(new Tap(buf2), new Tap(buf1)), 1); // 2n > 1n
      assertEquals(await type.match(new Tap(buf1), new Tap(buf3)), 0); // 1n == 1n
    });
  });

  describe("random", () => {
    it("should return a valid bigint", () => {
      const value = type.random();
      assert(typeof value === "bigint");
      assert(value >= minLong && value <= maxLong);
    });
  });

  describe("toJSON", () => {
    it('should return "long"', () => {
      assertEquals(type.toJSON(), "long");
    });
  });

  describe("inheritance from PrimitiveType and BaseType", () => {
    it("should clone bigint values", () => {
      assertEquals(type.cloneFromValue(42n), 42n);
      assertEquals(type.cloneFromValue(-42n), -42n);
    });

    it("should clone JSON integer defaults", () => {
      assertEquals(type.cloneFromValue(123), 123n);
    });

    it("should throw ValidationError for invalid clone", () => {
      assertThrows(() => {
        type.cloneFromValue(42.5 as unknown);
      }, ValidationError);
    });

    it("should have toBuffer and fromBuffer", async () => {
      const value = 123n;
      const buffer = await type.toBuffer(value);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should have isValid", () => {
      assert(type.isValid(42n));
      assert(!type.isValid(maxLong + 1n));
    });

    it("should create resolver for same type", async () => {
      const resolver = type.createResolver(type);
      const value = 789n;
      const buffer = await type.toBuffer(value);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, value);
    });

    it("should create resolver for int type", async () => {
      const resolver = type.createResolver(new IntType());
      const value = 123n;
      const intType = new IntType();
      const buffer = await intType.toBuffer(Number(value));
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, value);
    });

    it("should throw error for different type", () => {
      // Create a fake different type
      class FakeType extends LongType {
        // Different class
      }
      const otherType = new FakeType();
      assertThrows(
        () => {
          type.createResolver(otherType);
        },
        Error,
        "Schema evolution not supported from writer type: long to reader type: long",
      );
    });
  });

  describe("sync APIs", () => {
    describe("readSync", () => {
      it("should read long synchronously from tap", () => {
        const buffer = new ArrayBuffer(10);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeLong(123n);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), 123n);
      });

      it("should read negative long synchronously from tap", () => {
        const buffer = new ArrayBuffer(10);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeLong(-456n);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), -456n);
      });

      it("should read zero synchronously from tap", () => {
        const buffer = new ArrayBuffer(1);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeLong(0n);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), 0n);
      });

      it("should read max long synchronously from tap", () => {
        const buffer = new ArrayBuffer(10);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeLong(maxLong);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), maxLong);
      });

      it("should read min long synchronously from tap", () => {
        const buffer = new ArrayBuffer(10);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeLong(minLong);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), minLong);
      });
    });

    describe("writeSync", () => {
      it("should write long synchronously to tap", () => {
        const buffer = new ArrayBuffer(10);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, 789n);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readLong(), 789n);
      });

      it("should write negative long synchronously to tap", () => {
        const buffer = new ArrayBuffer(10);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, -999n);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readLong(), -999n);
      });

      it("should write zero synchronously to tap", () => {
        const buffer = new ArrayBuffer(1);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, 0n);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readLong(), 0n);
      });

      it("should write max long synchronously to tap", () => {
        const buffer = new ArrayBuffer(10);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, maxLong);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readLong(), maxLong);
      });

      it("should write min long synchronously to tap", () => {
        const buffer = new ArrayBuffer(10);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, minLong);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readLong(), minLong);
      });

      it("should throw for out-of-range value", () => {
        const buffer = new ArrayBuffer(10);
        const tap = new SyncWritableTap(buffer);
        assertThrows(() => {
          type.writeSync(tap, maxLong + 1n);
        }, ValidationError);
      });

      it("should throw for invalid value", () => {
        const buffer = new ArrayBuffer(10);
        const tap = new SyncWritableTap(buffer);
        assertThrows(() => {
          type.writeSync(tap, 42 as unknown as bigint);
        }, ValidationError);
      });
    });

    describe("skipSync", () => {
      it("should skip long synchronously in tap", () => {
        const value = 42n;
        const size = calculateVarintSize(value);
        const buffer = new ArrayBuffer(size + 1);
        const tap = new SyncWritableTap(buffer);
        type.writeSync(tap, value);
        const posAfterWrite = tap.getPos();
        assertEquals(posAfterWrite, size);
        const readTap = new SyncReadableTap(buffer);
        type.skipSync(readTap);
        const posAfterSkip = readTap.getPos();
        assertEquals(posAfterSkip, size);
      });

      it("should skip large long synchronously in tap", () => {
        const value = maxLong;
        const size = calculateVarintSize(value);
        const buffer = new ArrayBuffer(size + 1);
        const tap = new SyncWritableTap(buffer);
        type.writeSync(tap, value);
        const posAfterWrite = tap.getPos();
        assertEquals(posAfterWrite, size);
        const readTap = new SyncReadableTap(buffer);
        type.skipSync(readTap);
        const posAfterSkip = readTap.getPos();
        assertEquals(posAfterSkip, size);
      });
    });

    describe("toSyncBuffer", () => {
      it("should convert long to buffer synchronously", () => {
        const value = 123n;
        const buffer = type.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(tap), value);
      });

      it("should convert negative long to buffer synchronously", () => {
        const value = -456n;
        const buffer = type.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(tap), value);
      });

      it("should convert zero to buffer synchronously", () => {
        const value = 0n;
        const buffer = type.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(tap), value);
      });

      it("should convert max long to buffer synchronously", () => {
        const buffer = type.toSyncBuffer(maxLong);
        const tap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(tap), maxLong);
      });

      it("should convert min long to buffer synchronously", () => {
        const buffer = type.toSyncBuffer(minLong);
        const tap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(tap), minLong);
      });

      it("should throw ValidationError for invalid value", () => {
        assertThrows(() => {
          type.toSyncBuffer(42 as unknown as bigint);
        }, ValidationError);
      });
    });

    describe("fromSyncBuffer", () => {
      it("should deserialize buffer synchronously", () => {
        const value = 123n;
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should deserialize negative long synchronously", () => {
        const value = -456n;
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should deserialize zero synchronously", () => {
        const value = 0n;
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should deserialize max long synchronously", () => {
        const buffer = type.toSyncBuffer(maxLong);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, maxLong);
      });

      it("should deserialize min long synchronously", () => {
        const buffer = type.toSyncBuffer(minLong);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, minLong);
      });

      it("should throw for insufficient data", () => {
        const buffer = new ArrayBuffer(0);
        assertThrows(
          () => {
            type.fromSyncBuffer(buffer);
          },
          Error,
          "Operation exceeds buffer bounds",
        );
      });

      it("should throw for extra data", () => {
        const value = 123n;
        const buffer = type.toSyncBuffer(value);
        const largeBuffer = new ArrayBuffer(buffer.byteLength + 1);
        new Uint8Array(largeBuffer).set(new Uint8Array(buffer), 0);
        assertThrows(
          () => {
            type.fromSyncBuffer(largeBuffer);
          },
          Error,
          "Insufficient data for type",
        );
      });
    });

    describe("matchSync", () => {
      it("should match encoded long buffers synchronously", () => {
        const buf1 = type.toSyncBuffer(1n);
        const buf2 = type.toSyncBuffer(2n);
        const buf3 = type.toSyncBuffer(1n);

        assertEquals(
          type.matchSync(new SyncReadableTap(buf1), new SyncReadableTap(buf2)),
          -1,
        ); // 1n < 2n
        assertEquals(
          type.matchSync(new SyncReadableTap(buf2), new SyncReadableTap(buf1)),
          1,
        ); // 2n > 1n
        assertEquals(
          type.matchSync(new SyncReadableTap(buf1), new SyncReadableTap(buf3)),
          0,
        ); // 1n == 1n
      });

      it("should handle edge cases synchronously", () => {
        const minBuf = type.toSyncBuffer(minLong);
        const maxBuf = type.toSyncBuffer(maxLong);

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

    describe("resolver sync functionality", () => {
      it("should create resolver for int type with readSync", () => {
        const resolver = type.createResolver(new IntType());
        const value = 123;
        const intType = new IntType();
        const buffer = intType.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap);
        assertEquals(result, BigInt(value));
      });

      it("should create resolver for int type with negative values", () => {
        const resolver = type.createResolver(new IntType());
        const value = -456;
        const intType = new IntType();
        const buffer = intType.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap);
        assertEquals(result, BigInt(value));
      });

      it("should create resolver for int type with zero", () => {
        const resolver = type.createResolver(new IntType());
        const value = 0;
        const intType = new IntType();
        const buffer = intType.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap);
        assertEquals(result, 0n);
      });

      it("should create resolver for int type with max int", () => {
        const resolver = type.createResolver(new IntType());
        const value = 2147483647;
        const intType = new IntType();
        const buffer = intType.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap);
        assertEquals(result, BigInt(value));
      });

      it("should create resolver for int type with min int", () => {
        const resolver = type.createResolver(new IntType());
        const value = -2147483648;
        const intType = new IntType();
        const buffer = intType.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap);
        assertEquals(result, BigInt(value));
      });
    });
  });
});
