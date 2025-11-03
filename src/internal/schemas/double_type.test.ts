import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { Tap } from "../serialization/tap.ts";
import { DoubleType } from "./double_type.ts";
import { IntType } from "./int_type.ts";
import { LongType } from "./long_type.ts";
import { FloatType } from "./float_type.ts";
import { ValidationError } from "./error.ts";

describe("DoubleType", () => {
  const type = new DoubleType();

  describe("check", () => {
    it("should return true for numeric values including IEEE-754 specials", () => {
      assert(type.check(0));
      assert(type.check(42.5));
      assert(type.check(-42.5));
      assert(type.check(Number.NaN));
      assert(type.check(Number.POSITIVE_INFINITY));
      assert(type.check(Number.NEGATIVE_INFINITY));
    });

    it("should return false for non-number values", () => {
      assert(!type.check("42"));
      assert(!type.check(null));
      assert(!type.check(undefined));
      assert(!type.check({}));
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
    it("should read double from tap", () => {
      const buffer = new ArrayBuffer(8);
      const writeTap = new Tap(buffer);
      writeTap.writeDouble(123.5);
      const readTap = new Tap(buffer);
      assertEquals(type.read(readTap), 123.5);
    });

    it("should throw when insufficient data", () => {
      const buffer = new ArrayBuffer(4); // Less than 8 bytes needed for double
      const tap = new Tap(buffer);
      assertThrows(
        () => {
          type.read(tap);
        },
        Error,
        "Insufficient data for double",
      );
    });
  });

  describe("write", () => {
    it("should write double to tap", () => {
      const buffer = new ArrayBuffer(8);
      const writeTap = new Tap(buffer);
      type.write(writeTap, 456.0);
      const readTap = new Tap(buffer);
      assertEquals(readTap.readDouble(), 456.0);
    });

    it("should write and read Infinity without error", () => {
      const buffer = new ArrayBuffer(8);
      const writeTap = new Tap(buffer);
      type.write(writeTap, Infinity);
      const readTap = new Tap(buffer);
      assertEquals(readTap.readDouble(), Infinity);
    });

    it("should write and read NaN without error", () => {
      const buffer = new ArrayBuffer(8);
      const writeTap = new Tap(buffer);
      type.write(writeTap, NaN);
      const readTap = new Tap(buffer);
      const result = readTap.readDouble();
      assert(Number.isNaN(result));
    });

    it("should throw for invalid value", () => {
      const buffer = new ArrayBuffer(8);
      const tap = new Tap(buffer);
      assertThrows(() => {
        // deno-lint-ignore no-explicit-any
        (type as any).write(tap, "invalid");
      }, ValidationError);
    });
  });

  describe("sizeBytes", () => {
    it("should return 8", () => {
      assertEquals(type.sizeBytes(), 8);
    });
  });

  describe("compare", () => {
    it("should compare numbers correctly", () => {
      assertEquals(type.compare(1.0, 2.0), -1);
      assertEquals(type.compare(2.0, 1.0), 1);
      assertEquals(type.compare(1.0, 1.0), 0);
    });
  });

  describe("random", () => {
    it("should return a number", () => {
      const randomValue = type.random();
      assert(typeof randomValue === "number");
      assert(isFinite(randomValue));
    });
  });

  describe("toJSON", () => {
    it('should return "double"', () => {
      assertEquals(type.toJSON(), "double");
    });
  });

  describe("createResolver", () => {
    it("should create resolver for same type", () => {
      const resolver = type.createResolver(type);
      const value = 789.5;
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
      assertEquals(result, 123);
    });

    it("should create resolver for LongType writer", () => {
      const longType = new LongType();
      const resolver = type.createResolver(longType);
      const longValue = 123n;
      const buffer = longType.toBuffer(longValue);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, 123);
    });

    it("should create resolver for FloatType writer", () => {
      const floatType = new FloatType();
      const resolver = type.createResolver(floatType);
      const floatValue = 123.5;
      const buffer = floatType.toBuffer(floatValue);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, 123.5);
    });

    it("should throw when insufficient data in FloatType resolver", () => {
      const floatType = new FloatType();
      const resolver = type.createResolver(floatType);
      const buffer = new ArrayBuffer(2); // Less than 4 bytes needed for float
      const tap = new Tap(buffer);
      assertThrows(
        () => {
          resolver.read(tap);
        },
        Error,
        "Insufficient data for float",
      );
    });

    it("should demonstrate lossy promotion from long to double", () => {
      const longType = new LongType();
      const resolver = type.createResolver(longType);
      // Use a large bigint that loses precision when converted to double
      const largeLong = 2n ** 54n + 1n; // Larger than what double can represent exactly
      assert(largeLong > BigInt(Number.MAX_SAFE_INTEGER));
      const buffer = longType.toBuffer(largeLong);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      // The result should be rounded to the nearest representable double
      assertEquals(result, 2 ** 54); // Rounded down
    });

    it("should throw error for unsupported type", () => {
      class FakeType extends DoubleType {
        // Different class
      }
      const otherType = new FakeType();
      assertThrows(
        () => {
          type.createResolver(otherType);
        },
        Error,
        "Schema evolution not supported from writer type: double to reader type: double",
      );
    });
  });

  describe("inheritance from PrimitiveType and BaseType", () => {
    it("should clone number values", () => {
      assertEquals(type.clone(42.5), 42.5);
      assertEquals(type.clone(-42.5), -42.5);
    });

    it("should clone IEEE-754 special values", () => {
      const nanClone = type.clone(Number.NaN);
      assert(Number.isNaN(nanClone));
      assertEquals(
        type.clone(Number.POSITIVE_INFINITY),
        Number.POSITIVE_INFINITY,
      );
      assertEquals(
        type.clone(Number.NEGATIVE_INFINITY),
        Number.NEGATIVE_INFINITY,
      );
    });

    it("should throw ValidationError for invalid clone", () => {
      assertThrows(() => {
        // deno-lint-ignore no-explicit-any
        (type as any).clone("invalid");
      }, ValidationError);
    });

    it("should have toBuffer and fromBuffer", () => {
      const value = 123.5;
      const buffer = type.toBuffer(value);
      const result = type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should have isValid", () => {
      assert(type.isValid(42.5));
      assert(type.isValid(NaN));
      assert(type.isValid(Infinity));
      assert(type.isValid(-Infinity));
      assert(!type.isValid("invalid"));
    });
  });
});
