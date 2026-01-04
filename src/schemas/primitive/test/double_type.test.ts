import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { ReadBufferError } from "../../../serialization/buffers/buffer_error.ts";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import { ReadableTap } from "../../../serialization/tap.ts";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../../serialization/tap_sync.ts";
import { DoubleType } from "../double_type.ts";
import { IntType } from "../int_type.ts";
import { LongType } from "../long_type.ts";
import { FloatType } from "../float_type.ts";
import { ValidationError } from "../../error.ts";

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
    it("should read double from tap", async () => {
      const buffer = new ArrayBuffer(8);
      const writeTap = new Tap(buffer);
      await writeTap.writeDouble(123.5);
      const readTap = new Tap(buffer);
      assertEquals(await type.read(readTap), 123.5);
    });

    it("should throw when insufficient data", async () => {
      const buffer = new ArrayBuffer(4); // Less than 8 bytes needed for double
      const tap = new Tap(buffer);
      await assertRejects(
        async () => {
          await type.read(tap);
        },
        ReadBufferError,
        "Operation exceeds buffer bounds",
      );
    });

    // This test checks that double type read failures throw ReadBufferError, as the tap throws on buffer read failures instead of returning undefined.
    it("should throw when read fails", async () => {
      const mockBuffer = {
        read: (offset: number, size: number) =>
          Promise.reject(
            new ReadBufferError(
              "Operation exceeds buffer bounds",
              offset,
              size,
              0,
            ),
          ),
        // This is unused here.
        canReadMore: (_offset: number) => Promise.resolve(false),
      };
      const tap = new ReadableTap(mockBuffer);
      await assertRejects(
        async () => {
          await type.read(tap);
        },
        ReadBufferError,
        "Operation exceeds buffer bounds",
      );
    });
  });

  describe("write", () => {
    it("should write double to tap", async () => {
      const buffer = new ArrayBuffer(8);
      const writeTap = new Tap(buffer);
      await type.write(writeTap, 456.0);
      const readTap = new Tap(buffer);
      assertEquals(await readTap.readDouble(), 456.0);
    });

    it("should write and read Infinity without error", async () => {
      const buffer = new ArrayBuffer(8);
      const writeTap = new Tap(buffer);
      await type.write(writeTap, Infinity);
      const readTap = new Tap(buffer);
      assertEquals(await readTap.readDouble(), Infinity);
    });

    it("should write and read NaN without error", async () => {
      const buffer = new ArrayBuffer(8);
      const writeTap = new Tap(buffer);
      await type.write(writeTap, NaN);
      const readTap = new Tap(buffer);
      const result = await readTap.readDouble();
      assert(Number.isNaN(result));
    });

    it("should throw for invalid value", async () => {
      const buffer = new ArrayBuffer(8);
      const tap = new Tap(buffer);
      await assertRejects(async () => {
        // deno-lint-ignore no-explicit-any
        await (type as any).write(tap, "invalid");
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip double in tap", async () => {
      const buffer = new ArrayBuffer(16);
      const tap = new Tap(buffer);
      await tap.writeDouble(123.5); // write something first
      const posBefore = tap.getPos();
      await type.skip(tap);
      const posAfter = tap.getPos();
      assertEquals(posAfter - posBefore, 8);
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

    it("should create resolver for FloatType writer", async () => {
      const floatType = new FloatType();
      const resolver = type.createResolver(floatType);
      const floatValue = 123.5;
      const buffer = await floatType.toBuffer(floatValue);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, 123.5);
    });

    it("should throw when insufficient data in FloatType resolver", async () => {
      const floatType = new FloatType();
      const resolver = type.createResolver(floatType);
      const buffer = new ArrayBuffer(2); // Less than 4 bytes needed for float
      const tap = new Tap(buffer);
      await assertRejects(
        async () => {
          await resolver.read(tap);
        },
        ReadBufferError,
        "Operation exceeds buffer bounds",
      );
    });

    // This test checks that double type read failures throw ReadBufferError, as the tap throws on buffer read failures instead of returning undefined.
    it("should throw when read fails in FloatType resolver", async () => {
      const floatType = new FloatType();
      const resolver = type.createResolver(floatType);
      const mockBuffer = {
        read: (offset: number, size: number) =>
          Promise.reject(
            new ReadBufferError(
              "Operation exceeds buffer bounds",
              offset,
              size,
              0,
            ),
          ),
        // This is unused here.
        canReadMore: (_offset: number) => Promise.resolve(false),
      };
      const tap = new ReadableTap(mockBuffer);
      await assertRejects(
        async () => {
          await resolver.read(tap);
        },
        ReadBufferError,
        "Operation exceeds buffer bounds",
      );
    });

    it("should demonstrate lossy promotion from long to double", async () => {
      const longType = new LongType();
      const resolver = type.createResolver(longType);
      // Use a large bigint that loses precision when converted to double
      const largeLong = 2n ** 54n + 1n; // Larger than what double can represent exactly
      assert(largeLong > BigInt(Number.MAX_SAFE_INTEGER));
      const buffer = await longType.toBuffer(largeLong);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
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

  describe("match", () => {
    it("should match encoded double buffers correctly", async () => {
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

  describe("inheritance from PrimitiveType and BaseType", () => {
    it("should clone number values", () => {
      assertEquals(type.cloneFromValue(42.5), 42.5);
      assertEquals(type.cloneFromValue(-42.5), -42.5);
    });

    it("should clone IEEE-754 special values", () => {
      const nanClone = type.cloneFromValue(Number.NaN);
      assert(Number.isNaN(nanClone));
      assertEquals(
        type.cloneFromValue(Number.POSITIVE_INFINITY),
        Number.POSITIVE_INFINITY,
      );
      assertEquals(
        type.cloneFromValue(Number.NEGATIVE_INFINITY),
        Number.NEGATIVE_INFINITY,
      );
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

    it("should have isValid", () => {
      assert(type.isValid(42.5));
      assert(type.isValid(NaN));
      assert(type.isValid(Infinity));
      assert(type.isValid(-Infinity));
      assert(!type.isValid("invalid"));
    });
  });

  describe("synchronous API", () => {
    describe("readSync", () => {
      it("should read double from sync tap", () => {
        const buffer = new ArrayBuffer(8);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeDouble(123.5);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), 123.5);
      });

      it("should throw when insufficient data", () => {
        const buffer = new ArrayBuffer(4); // Less than 8 bytes needed for double
        const tap = new SyncReadableTap(buffer);
        assertThrows(
          () => {
            type.readSync(tap);
          },
          Error, // ReadBufferError
        );
      });
    });

    describe("writeSync", () => {
      it("should write double to sync tap", () => {
        const buffer = new ArrayBuffer(8);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, 456.0);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readDouble(), 456.0);
      });

      it("should write and read Infinity without error", () => {
        const buffer = new ArrayBuffer(8);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, Infinity);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readDouble(), Infinity);
      });

      it("should write and read NaN without error", () => {
        const buffer = new ArrayBuffer(8);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, NaN);
        const readTap = new SyncReadableTap(buffer);
        const result = readTap.readDouble();
        assert(Number.isNaN(result));
      });

      it("should throw for invalid value", () => {
        const buffer = new ArrayBuffer(8);
        const tap = new SyncWritableTap(buffer);
        assertThrows(() => {
          // deno-lint-ignore no-explicit-any
          (type as any).writeSync(tap, "invalid");
        }, ValidationError);
      });
    });

    describe("skipSync", () => {
      it("should skip double in sync tap", () => {
        const buffer = new ArrayBuffer(16);
        const tap = new SyncReadableTap(buffer);
        const posBefore = tap.getPos();
        type.skipSync(tap);
        const posAfter = tap.getPos();
        assertEquals(posAfter - posBefore, 8);
      });
    });

    describe("matchSync", () => {
      it("should match encoded double buffers correctly", () => {
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

    describe("createResolver sync", () => {
      it("should create resolver for same type sync", () => {
        const resolver = type.createResolver(type);
        const value = 789.5;
        const buffer = type.toSyncBuffer(value);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap);
        assertEquals(result, value);
      });

      it("should create resolver for IntType writer with sync read", async () => {
        const intType = new IntType();
        const resolver = type.createResolver(intType);
        const intValue = 42;
        const buffer = await intType.toBuffer(intValue);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap);
        assertEquals(result, intValue);
      });

      it("should create resolver for LongType writer with sync read", async () => {
        const longType = new LongType();
        const resolver = type.createResolver(longType);
        const longValue = 1234567890123n;
        const buffer = await longType.toBuffer(longValue);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap);
        assertEquals(result, Number(longValue));
      });

      it("should create resolver for FloatType writer with sync read", () => {
        const floatType = new FloatType();
        const resolver = type.createResolver(floatType);
        const floatValue = 3.14159;
        const buffer = floatType.toSyncBuffer(floatValue);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap) as number;
        // Float to double promotion may have precision differences
        assert(Math.abs(result - floatValue) < 0.00001);
      });
    });

    describe("sync buffer operations", () => {
      it("should have toSyncBuffer and fromSyncBuffer", () => {
        const value = 123.5;
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should handle IEEE-754 specials in sync buffer operations", () => {
        const values = [NaN, Infinity, -Infinity, 0, -0];
        for (const value of values) {
          const buffer = type.toSyncBuffer(value);
          const result = type.fromSyncBuffer(buffer);
          if (Number.isNaN(value)) {
            assert(Number.isNaN(result));
          } else {
            assertEquals(result, value);
          }
        }
      });
    });
  });
});
