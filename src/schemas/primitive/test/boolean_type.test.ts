import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../../serialization/tap_sync.ts";
import { ReadBufferError } from "../../../serialization/buffers/buffer_sync.ts";
import { BooleanType } from "../boolean_type.ts";
import { ValidationError } from "../../error.ts";

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
      const posBefore = tap.getPos();
      await type.skip(tap);
      const posAfter = tap.getPos();
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

  describe("sync APIs", () => {
    describe("readSync", () => {
      it("should read boolean synchronously from tap", () => {
        const buffer = new ArrayBuffer(1);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeBoolean(true);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), true);
      });

      it("should read false boolean synchronously from tap", () => {
        const buffer = new ArrayBuffer(1);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeBoolean(false);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(readTap), false);
      });
    });

    describe("writeSync", () => {
      it("should write boolean synchronously to tap", () => {
        const buffer = new ArrayBuffer(1);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, true);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readBoolean(), true);
      });

      it("should write false boolean synchronously to tap", () => {
        const buffer = new ArrayBuffer(1);
        const writeTap = new SyncWritableTap(buffer);
        type.writeSync(writeTap, false);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(readTap.readBoolean(), false);
      });

      it("should throw for invalid value", () => {
        const buffer = new ArrayBuffer(1);
        const tap = new SyncWritableTap(buffer);
        assertThrows(() => {
          type.writeSync(tap, 123 as unknown as boolean);
        }, ValidationError);
      });
    });

    describe("skipSync", () => {
      it("should skip boolean synchronously in tap", () => {
        const buffer = new ArrayBuffer(2);
        const tap = new SyncWritableTap(buffer);
        tap.writeBoolean(true); // write something first
        const readTap = new SyncReadableTap(buffer);
        const posBefore = readTap.getPos();
        type.skipSync(readTap);
        const posAfter = readTap.getPos();
        assertEquals(posAfter - posBefore, 1);
      });
    });

    describe("toSyncBuffer", () => {
      it("should serialize boolean to buffer synchronously", () => {
        const value = true;
        const buffer = type.toSyncBuffer(value);
        assertEquals(buffer.byteLength, 1);
        const tap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(tap), value);
      });

      it("should serialize false boolean to buffer synchronously", () => {
        const value = false;
        const buffer = type.toSyncBuffer(value);
        assertEquals(buffer.byteLength, 1);
        const tap = new SyncReadableTap(buffer);
        assertEquals(type.readSync(tap), value);
      });

      it("should throw ValidationError for invalid value", () => {
        assertThrows(() => {
          type.toSyncBuffer("invalid" as unknown as boolean);
        }, ValidationError);
      });
    });

    describe("fromSyncBuffer", () => {
      it("should deserialize boolean from buffer synchronously", () => {
        const value = true;
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should deserialize false boolean from buffer synchronously", () => {
        const value = false;
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should throw for truncated buffer", () => {
        const buffer = new ArrayBuffer(0);
        assertThrows(
          () => {
            type.fromSyncBuffer(buffer);
          },
          ReadBufferError,
          "Operation exceeds buffer bounds",
        );
      });
    });

    describe("matchSync", () => {
      it("should match encoded boolean buffers synchronously", () => {
        const trueBuf = type.toSyncBuffer(true);
        const falseBuf = type.toSyncBuffer(false);

        assertEquals(
          type.matchSync(
            new SyncReadableTap(falseBuf),
            new SyncReadableTap(trueBuf),
          ),
          -1,
        );
        assertEquals(
          type.matchSync(
            new SyncReadableTap(trueBuf),
            new SyncReadableTap(falseBuf),
          ),
          1,
        );
        assertEquals(
          type.matchSync(
            new SyncReadableTap(trueBuf),
            new SyncReadableTap(type.toSyncBuffer(true)),
          ),
          0,
        );
        assertEquals(
          type.matchSync(
            new SyncReadableTap(falseBuf),
            new SyncReadableTap(type.toSyncBuffer(false)),
          ),
          0,
        );
      });
    });
  });

  describe("inheritance from PrimitiveType and BaseType", () => {
    it("should clone boolean values", () => {
      assertEquals(type.cloneFromValue(true), true);
      assertEquals(type.cloneFromValue(false), false);
    });

    it("should throw ValidationError for invalid clone", () => {
      assertThrows(() => {
        type.cloneFromValue(123 as unknown as boolean);
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
        ReadBufferError,
        "Operation exceeds buffer bounds",
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
