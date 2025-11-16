import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { TestTap as Tap } from "../serialization/test_tap.ts";
import type { ReadableTapLike } from "../serialization/tap.ts";
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
    it("should read bytes from tap", async () => {
      const data = new Uint8Array([1, 2, 3, 4]);
      const buffer = new ArrayBuffer(10);
      const writeTap = new Tap(buffer);
      await writeTap.writeBytes(data);
      const readTap = new Tap(buffer);
      const result = await type.read(readTap);
      assertEquals(result, data);
    });

    it("should throw when insufficient data", async () => {
      const buffer = new ArrayBuffer(0); // Empty buffer
      const tap = new Tap(buffer);
      await assertRejects(
        async () => {
          await type.read(tap);
        },
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });

    it("should throw 'Insufficient data for bytes' when tap.readBytes returns undefined", async () => {
      // Mock tap that returns undefined for readBytes
      const mockTap = {
        readBytes: () => Promise.resolve(undefined),
      } as unknown as ReadableTapLike;
      await assertRejects(
        async () => await type.read(mockTap),
        Error,
        "Insufficient data for bytes",
      );
    });
  });

  describe("write", () => {
    it("should write bytes to tap", async () => {
      const data = new Uint8Array([5, 6, 7]);
      const buffer = new ArrayBuffer(10);
      const writeTap = new Tap(buffer);
      await type.write(writeTap, data);
      const readTap = new Tap(buffer);
      const result = await readTap.readBytes();
      assertEquals(result, data);
    });

    it("should throw for invalid value", async () => {
      const buffer = new ArrayBuffer(10);
      const tap = new Tap(buffer);
      await assertRejects(async () => {
        // deno-lint-ignore no-explicit-any
        await (type as any).write(tap, "invalid");
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip bytes in tap", async () => {
      const data = new Uint8Array([1, 2, 3]);
      const buffer = await type.toBuffer(data);
      const tap = new Tap(buffer);
      const posBefore = tap.getPos();
      await type.skip(tap);
      const posAfter = tap.getPos();
      assertEquals(posAfter - posBefore, buffer.byteLength);
    });
  });

  describe("toBuffer", () => {
    it("should serialize bytes correctly", async () => {
      const data = new Uint8Array([10, 20, 30]);
      const buffer = await type.toBuffer(data);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, data);
    });

    it("should throw ValidationError for invalid value", async () => {
      await assertRejects(async () => {
        await type.toBuffer("invalid" as unknown as Uint8Array);
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
      assert(randomValue.length >= 1 && randomValue.length <= 32);
    });

    it("should generate random bytes with values 0-255", () => {
      let randomValue;
      do {
        randomValue = type.random();
      } while (randomValue.length === 0);
      for (const byte of randomValue) {
        assert(byte >= 0 && byte <= 255);
      }
    });
  });

  describe("toJSON", () => {
    it('should return "bytes"', () => {
      assertEquals(type.toJSON(), "bytes");
    });
  });

  describe("createResolver", () => {
    it("should create resolver for same type", async () => {
      const resolver = type.createResolver(type);
      const value = new Uint8Array([1, 2, 3]);
      const buffer = await type.toBuffer(value);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, value);
    });

    it("should create resolver for StringType writer", async () => {
      const stringType = new StringType();
      const resolver = type.createResolver(stringType);
      const str = "hello";
      const buffer = await stringType.toBuffer(str);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      const expected = new TextEncoder().encode(str);
      assertEquals(result, expected);
    });

    it("should throw when reading string with insufficient data in resolver", async () => {
      const stringType = new StringType();
      const resolver = type.createResolver(stringType);
      const buf = new ArrayBuffer(5);
      const writeTap = new Tap(buf);
      await writeTap.writeLong(10n);
      const readTap = new Tap(buf);
      await assertRejects(
        async () => await resolver.read(readTap),
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });

    it("should throw 'Insufficient data for string' when tap.readString returns undefined", async () => {
      const stringType = new StringType();
      const resolver = type.createResolver(stringType);
      // Mock tap that returns undefined for readString
      const mockTap = {
        readString: () => Promise.resolve(undefined),
      } as unknown as ReadableTapLike;
      await assertRejects(
        async () => await resolver.read(mockTap),
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

  describe("match", () => {
    it("should match encoded bytes buffers correctly", async () => {
      const val1 = new Uint8Array([1, 2, 3]);
      const val2 = new Uint8Array([1, 2, 3]);
      const val3 = new Uint8Array([1, 2, 4]);
      const val4 = new Uint8Array([1, 2]);

      const buf1 = await type.toBuffer(val1);
      const buf2 = await type.toBuffer(val2);
      const buf3 = await type.toBuffer(val3);
      const buf4 = await type.toBuffer(val4);

      assertEquals(await type.match(new Tap(buf1), new Tap(buf2)), 0); // equal
      assertEquals(await type.match(new Tap(buf1), new Tap(buf3)), -1); // [1,2,3] < [1,2,4]
      assertEquals(await type.match(new Tap(buf3), new Tap(buf1)), 1); // [1,2,4] > [1,2,3]
      assertEquals(await type.match(new Tap(buf1), new Tap(buf4)), 1); // longer > shorter
      assertEquals(await type.match(new Tap(buf4), new Tap(buf1)), -1); // shorter < longer
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

    it("should have fromBuffer", async () => {
      const data = new Uint8Array([100, 101]);
      const buffer = await type.toBuffer(data);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, data);
    });

    it("should have isValid", () => {
      assert(type.isValid(new Uint8Array([1])));
      assert(!type.isValid("invalid"));
    });
  });
});
