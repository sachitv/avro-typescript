import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { Tap } from "../serialization/tap.ts";
import { BytesType } from "./bytes_type.ts";
import { StringType } from "./string_type.ts";
import { ValidationError } from "./error.ts";

describe("BytesType", () => {
  const type = new BytesType();

  describe("check", () => {
    it("should return true for Uint8Array", () => {
      assert(type.check(new Uint8Array([1, 2, 3])));
      assert(type.check(new Uint8Array(0)));
    });

    it("should return false for non-Uint8Array", () => {
      assert(!type.check("bytes"));
      assert(!type.check(null));
      assert(!type.check(undefined));
      assert(!type.check({}));
      assert(!type.check(new ArrayBuffer(4)));
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
    it("should read bytes from tap", () => {
      const data = new Uint8Array([1, 2, 3, 4]);
      const buffer = new ArrayBuffer(10);
      const writeTap = new Tap(buffer);
      writeTap.writeBytes(data);
      const readTap = new Tap(buffer);
      const result = type.read(readTap);
      assertEquals(result, data);
    });

    it("should throw when insufficient data", () => {
      const buffer = new ArrayBuffer(0); // Empty buffer
      const tap = new Tap(buffer);
      assertThrows(
        () => {
          type.read(tap);
        },
        Error,
        "Insufficient data for bytes",
      );
    });
  });

  describe("write", () => {
    it("should write bytes to tap", () => {
      const data = new Uint8Array([5, 6, 7]);
      const buffer = new ArrayBuffer(10);
      const writeTap = new Tap(buffer);
      type.write(writeTap, data);
      const readTap = new Tap(buffer);
      const result = readTap.readBytes();
      assertEquals(result, data);
    });

    it("should throw for invalid value", () => {
      const buffer = new ArrayBuffer(10);
      const tap = new Tap(buffer);
      assertThrows(() => {
        // deno-lint-ignore no-explicit-any
        (type as any).write(tap, "invalid");
      }, ValidationError);
    });
  });

  describe("toBuffer", () => {
    it("should serialize bytes correctly", () => {
      const data = new Uint8Array([10, 20, 30]);
      const buffer = type.toBuffer(data);
      const result = type.fromBuffer(buffer);
      assertEquals(result, data);
    });

    it("should throw ValidationError for invalid value", () => {
      assertThrows(() => {
        type.toBuffer("invalid" as unknown as Uint8Array);
      }, ValidationError);
    });
  });

  describe("compare", () => {
    it("should compare byte arrays correctly", () => {
      const buf1 = new Uint8Array([1, 2, 3]);
      const buf2 = new Uint8Array([1, 2, 3]);
      const buf3 = new Uint8Array([1, 2, 4]);
      const buf4 = new Uint8Array([1, 2]);
      assertEquals(type.compare(buf1, buf2), 0);
      assertEquals(type.compare(buf1, buf3), -1);
      assertEquals(type.compare(buf3, buf1), 1);
      assertEquals(type.compare(buf1, buf4), 1);
      assertEquals(type.compare(buf4, buf1), -1);
    });
  });

  describe("random", () => {
    it("should return a Uint8Array", () => {
      const randomValue = type.random();
      assert(randomValue instanceof Uint8Array);
      assert(randomValue.length >= 0 && randomValue.length < 32);
    });
  });

  describe("toJSON", () => {
    it('should return "bytes"', () => {
      assertEquals(type.toJSON(), "bytes");
    });
  });

  describe("createResolver", () => {
    it("should create resolver for same type", () => {
      const resolver = type.createResolver(type);
      const value = new Uint8Array([1, 2, 3]);
      const buffer = type.toBuffer(value);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, value);
    });

    it("should create resolver for StringType writer", () => {
      const stringType = new StringType();
      const resolver = type.createResolver(stringType);
      const str = "hello";
      const buffer = stringType.toBuffer(str);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      const expected = new TextEncoder().encode(str);
      assertEquals(result, expected);
    });

    it("should throw when reading string with insufficient data in resolver", () => {
      const stringType = new StringType();
      const resolver = type.createResolver(stringType);
      const buf = new ArrayBuffer(5);
      const writeTap = new Tap(buf);
      writeTap.writeLong(10n);
      const readTap = new Tap(buf);
      assertThrows(
        () => resolver.read(readTap),
        Error,
        "Insufficient data for string",
      );
    });

    it("should throw error for unsupported type", () => {
      class FakeType extends BytesType {
        // Different class
      }
      const otherType = new FakeType();
      assertThrows(
        () => {
          type.createResolver(otherType);
        },
        Error,
        "Schema evolution not supported from writer type: bytes to reader type: bytes",
      );
    });
  });

  describe("inheritance from PrimitiveType and BaseType", () => {
    it("should clone Uint8Array values", () => {
      const original = new Uint8Array([1, 2, 3]);
      const cloned = type.clone(original);
      assertEquals(cloned, original);
      assert(cloned !== original); // Different instances
    });

    it("should throw ValidationError for invalid clone", () => {
      assertThrows(() => {
        // deno-lint-ignore no-explicit-any
        (type as any).clone("invalid");
      }, ValidationError);
    });

    it("should have fromBuffer", () => {
      const data = new Uint8Array([100, 101]);
      const buffer = type.toBuffer(data);
      const result = type.fromBuffer(buffer);
      assertEquals(result, data);
    });

    it("should have isValid", () => {
      assert(type.isValid(new Uint8Array([1])));
      assert(!type.isValid("invalid"));
    });
  });
});
