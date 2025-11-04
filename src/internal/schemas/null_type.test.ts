import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { Tap } from "../serialization/tap.ts";
import { NullType } from "./null_type.ts";
import { ValidationError } from "./error.ts";

describe("NullType", () => {
  const type = new NullType();

  describe("check", () => {
    it("should return true for null", () => {
      assert(type.check(null));
    });

    it("should return false for non-null values", () => {
      assert(!type.check(undefined));
      assert(!type.check(0));
      assert(!type.check(""));
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
    it("should return null", () => {
      const buffer = new ArrayBuffer(0);
      const tap = new Tap(buffer);
      assertEquals(type.read(tap), null);
    });
  });

  describe("write", () => {
    it("should write null to tap", () => {
      const buffer = new ArrayBuffer(0);
      const tap = new Tap(buffer);
      type.write(tap, null);
      // Nothing to assert, just shouldn't throw
    });

    it("should throw for non-null value", () => {
      const buffer = new ArrayBuffer(0);
      const tap = new Tap(buffer);
      assertThrows(() => {
        // deno-lint-ignore no-explicit-any
        (type as any).write(tap, "invalid");
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip null in tap", () => {
      const buffer = new ArrayBuffer(1);
      const tap = new Tap(buffer);
      const posBefore = tap._testOnlyPos;
      type.skip(tap);
      const posAfter = tap._testOnlyPos;
      assertEquals(posAfter - posBefore, 0);
    });
  });

  describe("sizeBytes", () => {
    it("should return 0", () => {
      assertEquals(type.sizeBytes(), 0);
    });
  });

  describe("compare", () => {
    it("should always return 0", () => {
      assertEquals(type.compare(null, null), 0);
    });
  });

  describe("match", () => {
    it("should always return 0 for null buffers", () => {
      const buf1 = type.toBuffer(null);
      const buf2 = type.toBuffer(null);

      assertEquals(type.match(new Tap(buf1), new Tap(buf2)), 0);
    });
  });

  describe("random", () => {
    it("should return null", () => {
      assertEquals(type.random(), null);
    });
  });

  describe("toJSON", () => {
    it('should return "null"', () => {
      assertEquals(type.toJSON(), "null");
    });
  });

  describe("inheritance from FixedSizeBaseType and BaseType", () => {
    it("should clone null values", () => {
      assertEquals(type.clone(null), null);
    });

    it("should throw ValidationError for invalid clone", () => {
      assertThrows(() => {
        // deno-lint-ignore no-explicit-any
        (type as any).clone("invalid");
      }, ValidationError);
    });

    it("should have toBuffer and fromBuffer", () => {
      const value = null;
      const buffer = type.toBuffer(value);
      assertEquals(buffer.byteLength, 0);
      const result = type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should deserialize from an empty buffer", () => {
      const result = type.fromBuffer(new ArrayBuffer(0));
      assertEquals(result, null);
    });

    it("should have isValid", () => {
      assert(type.isValid(null));
      assert(!type.isValid(undefined));
    });
  });
});
