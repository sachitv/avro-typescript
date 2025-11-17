import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
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

  describe("inheritance from PrimitiveType and BaseType", () => {
    it("should clone int values", () => {
      assertEquals(type.clone(42), 42);
      assertEquals(type.clone(-42), -42);
    });

    it("should throw ValidationError for invalid clone", () => {
      assertThrows(() => {
        type.clone(2147483648 as unknown as number);
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
