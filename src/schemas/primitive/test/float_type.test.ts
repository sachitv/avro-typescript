import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import { ReadableTap } from "../../../serialization/tap.ts";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../../serialization/tap_sync.ts";
import { FloatType } from "../float_type.ts";
import { IntType } from "../int_type.ts";
import { LongType } from "../long_type.ts";
import { ValidationError } from "../../error.ts";
import { BooleanType } from "../boolean_type.ts";
import { ReadBufferError } from "../../../serialization/buffers/buffer_sync.ts";

describe("FloatType", () => {
  const type = new FloatType();

  describe("check", () => {
    it("should return true for all numbers including NaN and infinity", () => {
      assert(type.check(0));
      assert(type.check(42.5));
      assert(type.check(-42.5));
      assert(type.check(NaN));
      assert(type.check(Infinity));
      assert(type.check(-Infinity));
    });

    it("should return false for non-numbers", () => {
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
    it("should read float from tap", async () => {
      const buffer = new ArrayBuffer(4);
      const writeTap = new Tap(buffer);
      await writeTap.writeFloat(123.5);
      const readTap = new Tap(buffer);
      assertEquals(await type.read(readTap), 123.5);
    });

    it("should handle NaN and infinity", async () => {
      const values = [NaN, Infinity, -Infinity];
      for (const val of values) {
        const buffer = await type.toBuffer(val);
        const result = await type.fromBuffer(buffer);
        if (isNaN(val)) {
          assert(isNaN(result));
        } else {
          assertEquals(result, val);
        }
      }
    });

    it("should throw when insufficient data", async () => {
      const buffer = new ArrayBuffer(2); // Less than 4 bytes needed for float
      const tap = new Tap(buffer);
      await assertRejects(
        async () => {
          await type.read(tap);
        },
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });

    // This test verifies that float type read failures throw RangeError, as the tap throws on buffer read failures instead of returning undefined.
    it("should throw when read fails", async () => {
      const mockBuffer = {
        read: (_offset: number, _size: number) => Promise.resolve(undefined),
        // This is unused here.
        canReadMore: (_offset: number) => Promise.resolve(false),
      };
      const tap = new ReadableTap(mockBuffer);
      await assertRejects(
        async () => {
          await type.read(tap);
        },
        RangeError,
        "Attempt to read beyond buffer bounds.",
      );
    });
  });

  describe("write", () => {
    it("should write float to tap", async () => {
      const buffer = new ArrayBuffer(4);
      const writeTap = new Tap(buffer);
      await type.write(writeTap, 456.0);
      const readTap = new Tap(buffer);
      assertEquals(await readTap.readFloat(), 456.0);
    });

    it("should throw for invalid value", async () => {
      const buffer = new ArrayBuffer(4);
      const tap = new Tap(buffer);
      await assertRejects(async () => {
        // deno-lint-ignore no-explicit-any
        await (type as any).write(tap, "invalid");
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip float in tap", async () => {
      const buffer = new ArrayBuffer(8);
      const tap = new Tap(buffer);
      await tap.writeFloat(123.5); // write something first
      const posBefore = tap.getPos();
      await type.skip(tap);
      const posAfter = tap.getPos();
      assertEquals(posAfter - posBefore, 4);
    });
  });

  describe("sizeBytes", () => {
    it("should return 4", () => {
      assertEquals(type.sizeBytes(), 4);
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
    it('should return "float"', () => {
      assertEquals(type.toJSON(), "float");
    });
  });

  describe("createResolver", () => {
    it("should create resolver for same type", async () => {
      const resolver = type.createResolver(type);
      const value = 789.5;
      const buffer = await type.toBuffer(value);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, value);
    });

    it("should create resolver for IntType writer", async () => {
      const intType = new IntType();
      const resolver = type.createResolver(intType);
      const intValue = 123;
      const buffer = await intType.toBuffer(intValue);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, 123);
    });

    it("should create resolver for LongType writer", async () => {
      const longType = new LongType();
      const resolver = type.createResolver(longType);
      const longValue = 123n;
      const buffer = await longType.toBuffer(longValue);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, 123);
    });

    it("should demonstrate lossy promotion from long to float", async () => {
      const longType = new LongType();
      const resolver = type.createResolver(longType);
      // Use a large bigint that loses precision when converted to float
      const largeLong = 2n ** 53n + 1n; // Larger than MAX_SAFE_INTEGER
      assert(largeLong > BigInt(Number.MAX_SAFE_INTEGER));
      const buffer = await longType.toBuffer(largeLong);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      // The result should be rounded to the nearest representable float
      assertEquals(result, 2 ** 53); // Rounded down to 2^53
    });

    it("should create resolver for IntType writer with sync read", async () => {
      const intType = new IntType();
      const resolver = type.createResolver(intType);
      const intValue = 123;
      const buffer = await intType.toBuffer(intValue);
      const tap = new SyncReadableTap(buffer);
      const result = resolver.readSync(tap);
      assertEquals(result, 123);
    });

    it("should create resolver for LongType writer with sync read", async () => {
      const longType = new LongType();
      const resolver = type.createResolver(longType);
      const longValue = 123n;
      const buffer = await longType.toBuffer(longValue);
      const tap = new SyncReadableTap(buffer);
      const result = resolver.readSync(tap);
      assertEquals(result, 123);
    });

    it("should demonstrate lossy promotion from long to float synchronously", async () => {
      const longType = new LongType();
      const resolver = type.createResolver(longType);
      // Use a large bigint that loses precision when converted to float
      const largeLong = 2n ** 53n + 1n; // Larger than MAX_SAFE_INTEGER
      assert(largeLong > BigInt(Number.MAX_SAFE_INTEGER));
      const buffer = await longType.toBuffer(largeLong);
      const tap = new SyncReadableTap(buffer);
      const result = resolver.readSync(tap);
      // The result should be rounded to the nearest representable float
      assertEquals(result, 2 ** 53); // Rounded down to 2^53
    });

    it("should throw error for unsupported type", () => {
      const otherType = new BooleanType();
      assertThrows(
        () => {
          type.createResolver(otherType);
        },
        Error,
        "Schema evolution not supported from writer type: boolean to reader type: float",
      );
    });
  });

  describe("match", () => {
    it("should match encoded float buffers correctly", async () => {
      const val1 = 1.0;
      const val2 = 1.0;
      const val3 = 2.0;

      const buf1 = await type.toBuffer(val1);
      const buf2 = await type.toBuffer(val2);
      const buf3 = await type.toBuffer(val3);

      assertEquals(await type.match(new Tap(buf1), new Tap(buf2)), 0); // 1.0 == 1.0
      assertEquals(await type.match(new Tap(buf1), new Tap(buf3)), -1); // 1.0 < 2.0
      assertEquals(await type.match(new Tap(buf3), new Tap(buf1)), 1); // 2.0 > 1.0
    });
  });

  describe("readSync", () => {
    it("should read float from sync tap", () => {
      const buffer = new ArrayBuffer(4);
      const writeTap = new SyncWritableTap(buffer);
      writeTap.writeFloat(123.5);
      const readTap = new SyncReadableTap(buffer);
      assertEquals(type.readSync(readTap), 123.5);
    });

    it("should handle NaN and infinity synchronously", () => {
      const values = [NaN, Infinity, -Infinity];
      for (const val of values) {
        const buffer = type.toSyncBuffer(val);
        const result = type.fromSyncBuffer(buffer);
        if (isNaN(val)) {
          assert(isNaN(result));
        } else {
          assertEquals(result, val);
        }
      }
    });

    it("should throw when insufficient data", () => {
      const buffer = new ArrayBuffer(2); // Less than 4 bytes needed for float
      const tap = new SyncReadableTap(buffer);
      assertThrows(
        () => {
          type.readSync(tap);
        },
        ReadBufferError,
      );
    });
  });

  describe("writeSync", () => {
    it("should write float to sync tap", () => {
      const buffer = new ArrayBuffer(4);
      const writeTap = new SyncWritableTap(buffer);
      type.writeSync(writeTap, 456.0);
      const readTap = new SyncReadableTap(buffer);
      assertEquals(readTap.readFloat(), 456.0);
    });

    it("should throw for invalid value", () => {
      const buffer = new ArrayBuffer(4);
      const tap = new SyncWritableTap(buffer);
      assertThrows(() => {
        // deno-lint-ignore no-explicit-any
        (type as any).writeSync(tap, "invalid");
      }, ValidationError);
    });
  });

  describe("matchSync", () => {
    it("should match encoded float buffers correctly synchronously", () => {
      const val1 = 1.0;
      const val2 = 1.0;
      const val3 = 2.0;

      const buf1 = type.toSyncBuffer(val1);
      const buf2 = type.toSyncBuffer(val2);
      const buf3 = type.toSyncBuffer(val3);

      assertEquals(
        type.matchSync(new SyncReadableTap(buf1), new SyncReadableTap(buf2)),
        0,
      ); // 1.0 == 1.0
      assertEquals(
        type.matchSync(new SyncReadableTap(buf1), new SyncReadableTap(buf3)),
        -1,
      ); // 1.0 < 2.0
      assertEquals(
        type.matchSync(new SyncReadableTap(buf3), new SyncReadableTap(buf1)),
        1,
      ); // 2.0 > 1.0
    });
  });

  describe("inheritance from PrimitiveType and BaseType", () => {
    it("should clone number values", () => {
      assertEquals(type.cloneFromValue(42.5), 42.5);
      assertEquals(type.cloneFromValue(-42.5), -42.5);
    });

    it("should throw ValidationError for invalid clone", () => {
      assertThrows(() => {
        // deno-lint-ignore no-explicit-any
        (type as any).cloneFromValue("invalid");
      }, ValidationError);
    });

    it("should have toBuffer and fromBuffer", async () => {
      const value = 123.5;
      const buffer = await type.toBuffer(value);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should have toSyncBuffer and fromSyncBuffer", () => {
      const value = 123.5;
      const buffer = type.toSyncBuffer(value);
      const result = type.fromSyncBuffer(buffer);
      assertEquals(result, value);
    });

    it("should have isValid", () => {
      assert(type.isValid(42.5));
      assert(type.isValid(NaN));
      assert(!type.isValid("invalid"));
    });
  });
});
