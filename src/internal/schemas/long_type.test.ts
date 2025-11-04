import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { Tap } from "../serialization/tap.ts";
import { LongType } from "./long_type.ts";
import { IntType } from "./int_type.ts";
import { ValidationError } from "./error.ts";
import { calculateVarintSize } from "./varint.ts";

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
    it("should read long from tap", () => {
      const buffer = new ArrayBuffer(10);
      const writeTap = new Tap(buffer);
      writeTap.writeLong(123n);
      const readTap = new Tap(buffer);
      assertEquals(type.read(readTap), 123n);
    });
  });

  describe("write", () => {
    it("should write long to tap", () => {
      const buffer = new ArrayBuffer(10);
      const writeTap = new Tap(buffer);
      type.write(writeTap, 456n);
      const readTap = new Tap(buffer);
      assertEquals(readTap.readLong(), 456n);
    });

    it("should throw for out-of-range value", () => {
      const buffer = new ArrayBuffer(10);
      const tap = new Tap(buffer);
      assertThrows(() => {
        type.write(tap, maxLong + 1n);
      }, ValidationError);
    });

    it("should throw for invalid value", () => {
      const buffer = new ArrayBuffer(10);
      const tap = new Tap(buffer);
      assertThrows(() => {
        type.write(tap, 42 as unknown as bigint);
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip long in tap", () => {
      const value = 42n;
      const size = calculateVarintSize(value);
      const buffer = new ArrayBuffer(size + 1);
      const tap = new Tap(buffer);
      type.write(tap, value);
      const posAfterWrite = tap._testOnlyPos;
      assertEquals(posAfterWrite, size);
      tap.resetPos();
      type.skip(tap);
      const posAfterSkip = tap._testOnlyPos;
      assertEquals(posAfterSkip, size);
    });
  });

  describe("toBuffer", () => {
    it("should throw ValidationError for out-of-range value", () => {
      assertThrows(() => {
        type.toBuffer(maxLong + 1n);
      }, ValidationError);
    });

    it("should throw ValidationError for non-bigint value", () => {
      assertThrows(() => {
        type.toBuffer(1.5 as unknown as bigint);
      }, ValidationError);
    });
  });

  describe("compare", () => {
    it("should compare bigints correctly", () => {
      assertEquals(type.compare(1n, 2n), -1);
      assertEquals(type.compare(2n, 1n), 1);
      assertEquals(type.compare(1n, 1n), 0);
    });
  });

  describe("random", () => {
    it("should return a bigint", () => {
      const value = type.random();
      assert(typeof value === "bigint");
    });
  });

  describe("toJSON", () => {
    it('should return "long"', () => {
      assertEquals(type.toJSON(), "long");
    });
  });

  describe("createResolver", () => {
    it("should create resolver for same type", () => {
      const resolver = type.createResolver(type);
      const value = 789n;
      const buffer = type.toBuffer(value);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, value);
    });

    it("should create resolver for IntType writer", () => {
      const intType = new IntType();
      const resolver = type.createResolver(intType);
      const intValue = 123;
      const buffer = intType.toBuffer(intValue);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, 123n);
    });

    it("should throw error for unsupported type", () => {
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

  describe("match", () => {
    it("should match encoded long buffers correctly", () => {
      const val1 = 1n;
      const val2 = 1n;
      const val3 = 2n;

      const buf1 = type.toBuffer(val1);
      const buf2 = type.toBuffer(val2);
      const buf3 = type.toBuffer(val3);

      assertEquals(type.match(new Tap(buf1), new Tap(buf2)), 0); // 1n == 1n
      assertEquals(type.match(new Tap(buf1), new Tap(buf3)), -1); // 1n < 2n
      assertEquals(type.match(new Tap(buf3), new Tap(buf1)), 1); // 2n > 1n
    });
  });

  describe("inheritance from PrimitiveType and BaseType", () => {
    it("should clone bigint values", () => {
      assertEquals(type.clone(42n), 42n);
      assertEquals(type.clone(-42n), -42n);
      assertEquals(type.clone(maxLong), maxLong);
      assertEquals(type.clone(minLong), minLong);
    });

    it("should throw ValidationError for invalid clone", () => {
      assertThrows(() => {
        type.clone(42 as unknown as bigint);
      }, ValidationError);
    });

    it("should have toBuffer and fromBuffer", () => {
      const value = 123n;
      const buffer = type.toBuffer(value);
      const result = type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should have isValid", () => {
      assert(type.isValid(42n));
      assert(!type.isValid(42));
      assert(!type.isValid(maxLong + 1n));
    });
  });
});
