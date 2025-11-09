import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { TestTap as Tap } from "../serialization/test_tap.ts";
import { BooleanType } from "./boolean_type.ts";
import { ValidationError } from "./error.ts";

describe("BooleanType", () => {
  const type = new BooleanType();

  describe("check", () => {
    it("should return true for boolean values", () => {
      assert(type.check(true));
      assert(type.check(false));
    });

    it("should return false for non-boolean values", () => {
      assert(!type.check(1));
      assert(!type.check("true"));
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
    it("should read boolean from tap", async () => {
      const buffer = new ArrayBuffer(1);
      const writeTap = new Tap(buffer);
      await writeTap.writeBoolean(true);
      const readTap = new Tap(buffer);
      assertEquals(await type.read(readTap), true);
    });
  });

  describe("write", () => {
    it("should write boolean to tap", async () => {
      const buffer = new ArrayBuffer(1);
      const writeTap = new Tap(buffer);
      await type.write(writeTap, true);
      const readTap = new Tap(buffer);
      assertEquals(await readTap.readBoolean(), true);
    });

    it("should throw for invalid value", async () => {
      const buffer = new ArrayBuffer(1);
      const tap = new Tap(buffer);
      await assertRejects(async () => {
        await type.write(tap, 123 as unknown as boolean);
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip boolean in tap", async () => {
      const buffer = new ArrayBuffer(2);
      const tap = new Tap(buffer);
      await tap.writeBoolean(true); // write something first
      const posBefore = tap._testOnlyPos;
      await type.skip(tap);
      const posAfter = tap._testOnlyPos;
      assertEquals(posAfter - posBefore, 1);
    });
  });

  describe("sizeBytes", () => {
    it("should return 1", () => {
      assertEquals(type.sizeBytes(), 1);
    });
  });

  describe("compare", () => {
    it("should compare booleans correctly", () => {
      assertEquals(type.compare(true, true), 0);
      assertEquals(type.compare(false, false), 0);
      assertEquals(type.compare(true, false), 1);
      assertEquals(type.compare(false, true), -1);
    });
  });

  describe("random", () => {
    it("should return a boolean", () => {
      const value = type.random();
      assert(typeof value === "boolean");
    });
  });

  describe("toJSON", () => {
    it("should return the schema name", () => {
      assertEquals(type.toJSON(), "boolean");
    });
  });

  describe("match", () => {
    it("should match encoded boolean buffers", async () => {
      const trueBuf = await type.toBuffer(true);
      const falseBuf = await type.toBuffer(false);

      assertEquals(await type.match(new Tap(falseBuf), new Tap(trueBuf)), -1);
      assertEquals(await type.match(new Tap(trueBuf), new Tap(falseBuf)), 1);
      assertEquals(
        await type.match(new Tap(trueBuf), new Tap(await type.toBuffer(true))),
        0,
      );
      assertEquals(
        await type.match(
          new Tap(falseBuf),
          new Tap(await type.toBuffer(false)),
        ),
        0,
      );
    });
  });

  describe("inheritance from PrimitiveType and BaseType", () => {
    it("should clone boolean values", () => {
      assertEquals(type.clone(true), true);
      assertEquals(type.clone(false), false);
    });

    it("should throw ValidationError for invalid clone", () => {
      assertThrows(() => {
        type.clone(123 as unknown as boolean);
      }, ValidationError);
    });

    it("should have toBuffer and fromBuffer", async () => {
      const value = true;
      const buffer = await type.toBuffer(value);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should throw for truncated buffer", async () => {
      const buffer = new ArrayBuffer(0);
      await assertRejects(
        () => type.fromBuffer(buffer),
        Error,
        "Insufficient data for type",
      );
    });

    it("should have isValid", () => {
      assert(type.isValid(true));
      assert(!type.isValid("true"));
    });

    it("should create resolver for same type", async () => {
      const resolver = type.createResolver(type);
      const value = false;
      const buffer = await type.toBuffer(value);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, value);
    });

    it("should throw error for different type", () => {
      // Create a fake different type
      class FakeType extends BooleanType {
        // Different class
      }
      const otherType = new FakeType();
      assertThrows(
        () => {
          type.createResolver(otherType);
        },
        Error,
        "Schema evolution not supported from writer type: boolean to reader type: boolean",
      );
    });
  });
});
