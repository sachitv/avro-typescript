import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import { NullType } from "../null_type.ts";
import { ValidationError } from "../../error.ts";

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
    it("should return null", async () => {
      const buffer = new ArrayBuffer(0);
      const tap = new Tap(buffer);
      assertEquals(await type.read(tap), null);
    });
  });

  describe("write", () => {
    it("should write null to tap", async () => {
      const buffer = new ArrayBuffer(0);
      const tap = new Tap(buffer);
      await type.write(tap, null);
      // Nothing to assert, just shouldn't throw
    });

    it("should throw for non-null value", async () => {
      const buffer = new ArrayBuffer(0);
      const tap = new Tap(buffer);
      await assertRejects(async () => {
        // deno-lint-ignore no-explicit-any
        await ((type as any).write(tap, "invalid"));
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip null in tap", async () => {
      const buffer = new ArrayBuffer(1);
      const tap = new Tap(buffer);
      const posBefore = tap.getPos();
      await type.skip(tap);
      const posAfter = tap.getPos();
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
    it("should always return 0 for null buffers", async () => {
      const buf1 = await type.toBuffer(null);
      const buf2 = await type.toBuffer(null);

      assertEquals(await type.match(new Tap(buf1), new Tap(buf2)), 0);
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

    it("should have toBuffer and fromBuffer", async () => {
      const value = null;
      const buffer = await type.toBuffer(value);
      assertEquals(buffer.byteLength, 0);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should deserialize from an empty buffer", async () => {
      const result = await type.fromBuffer(new ArrayBuffer(0));
      assertEquals(result, null);
    });

    it("should have isValid", () => {
      assert(type.isValid(null));
      assert(!type.isValid(undefined));
    });
  });
});
