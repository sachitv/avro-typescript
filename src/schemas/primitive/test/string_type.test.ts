import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { StringType } from "../string_type.ts";
import { BytesType } from "../bytes_type.ts";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import { ReadableTap } from "../../../serialization/tap.ts";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../../serialization/sync_tap.ts";
import { ReadBufferError } from "../../../serialization/buffers/sync_buffer.ts";
import { calculateVarintSize } from "../../../internal/varint.ts";
import { utf8ByteLength } from "../../../serialization/text_encoding.ts";

describe("StringType", () => {
  const type = new StringType();

  it("check should validate strings", () => {
    assertEquals(type.check("hello"), true);
    assertEquals(type.check(""), true);
    assertEquals(type.check(123), false);
    assertEquals(type.check(null), false);
    assertEquals(type.check(undefined), false);

    let errorCalled = false;
    const errorHook = () => {
      errorCalled = true;
    };
    assertEquals(type.check(123, errorHook), false);
    assertEquals(errorCalled, true);
  });

  it("toBuffer should allocate enough space for multi-byte strings", async () => {
    const value = "\u00e9".repeat(40);
    const buf = await type.toBuffer(value);
    assertEquals(buf.byteLength, 82);
    const tap = new Tap(buf);
    assertEquals(await type.read(tap), value);
  });

  it("toBuffer should allocate enough space for surrogate pairs", async () => {
    const value = "\ud83d\ude80".repeat(40);
    const buf = await type.toBuffer(value);
    const expectedLength = calculateVarintSize(utf8ByteLength(value)) +
      utf8ByteLength(value);
    assertEquals(buf.byteLength, expectedLength);
    const tap = new Tap(buf);
    assertEquals(await type.read(tap), value);
  });

  it("toBuffer should allocate enough space for unpaired surrogates", async () => {
    const value = "\ud800".repeat(40);
    const buf = await type.toBuffer(value);
    const expectedLength = calculateVarintSize(utf8ByteLength(value)) +
      utf8ByteLength(value);
    assertEquals(buf.byteLength, expectedLength);
    const tap = new Tap(buf);
    assertEquals(await type.read(tap), "\ufffd".repeat(40));
  });

  it("toBuffer should throw ValidationError for invalid value", async () => {
    await assertRejects(() => type.toBuffer(123 as unknown as string));
  });

  it("toBuffer and read should serialize and deserialize strings", async () => {
    const testStrings = ["hello", "", "test string", "🚀 emoji"];

    for (const str of testStrings) {
      const buf = await type.toBuffer(str);
      const tap = new Tap(buf);
      assertEquals(await type.read(tap), str);
    }
  });

  it("write should write strings to tap", async () => {
    const buf = new ArrayBuffer(100);
    const writeTap = new Tap(buf);
    await type.write(writeTap, "test");
    const readTap = new Tap(buf);
    assertEquals(await type.read(readTap), "test");
  });

  it("write should throw for non-strings", async () => {
    const buf = new ArrayBuffer(10);
    const tap = new Tap(buf);
    // deno-lint-ignore no-explicit-any
    const invalidValue: any = 123;
    await assertRejects(() => type.write(tap, invalidValue), Error);
  });

  it("skip should skip string in tap", async () => {
    const str = "test";
    const buffer = await type.toBuffer(str);
    const tap = new Tap(buffer);
    const posBefore = tap.getPos();
    await type.skip(tap);
    const posAfter = tap.getPos();
    assertEquals(posAfter - posBefore, buffer.byteLength);
  });

  it("read should throw for insufficient data", async () => {
    const buf = new ArrayBuffer(5);
    const writeTap = new Tap(buf);
    await writeTap.writeLong(10n);
    const readTap = new Tap(buf);
    await assertRejects(
      () => type.read(readTap),
      RangeError,
      "Operation exceeds buffer bounds",
    );
  });

  // This test ensures string type read failures throw RangeError, as the tap throws on buffer read failures instead of returning undefined.
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

  it("compare should compare strings lexicographically", () => {
    assertEquals(type.compare("a", "b"), -1);
    assertEquals(type.compare("b", "a"), 1);
    assertEquals(type.compare("a", "a"), 0);
    assertEquals(type.compare("", "a"), -1);
  });

  it("match should match encoded string buffers", async () => {
    const bufA = await type.toBuffer("a");
    const bufB = await type.toBuffer("b");
    const bufEmpty = await type.toBuffer("");

    assertEquals(await type.match(new Tap(bufA), new Tap(bufB)), -1);
    assertEquals(await type.match(new Tap(bufB), new Tap(bufA)), 1);
    assertEquals(
      await type.match(new Tap(bufA), new Tap(await type.toBuffer("a"))),
      0,
    );
    assertEquals(
      await type.match(new Tap(bufEmpty), new Tap(await type.toBuffer(""))),
      0,
    );
  });

  it("random should return a string", () => {
    const rand = type.random();
    assertEquals(typeof rand, "string");
    assertEquals(rand.length > 0, true);
  });

  it('toJSON should return "string"', () => {
    assertEquals(type.toJSON(), "string");
  });

  describe("createResolver", () => {
    it("should create resolver for same type", async () => {
      const resolver = type.createResolver(type);
      const str = "test";
      const buffer = await type.toBuffer(str);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, str);
    });

    it("should create resolver for BytesType writer", async () => {
      const bytesType = new BytesType();
      const resolver = type.createResolver(bytesType);
      const bytes = new Uint8Array([72, 101, 108, 108, 111]); // 'Hello'
      const buffer = await bytesType.toBuffer(bytes);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, "Hello");
    });

    it("should throw when reading bytes with insufficient data in resolver", async () => {
      const bytesType = new BytesType();
      const resolver = type.createResolver(bytesType);
      const buf = new ArrayBuffer(5);
      const writeTap = new Tap(buf);
      await writeTap.writeLong(10n);
      const readTap = new Tap(buf);
      await assertRejects(
        () => resolver.read(readTap),
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });

    // This test ensures string type read failures throw RangeError, as the tap throws on buffer read failures instead of returning undefined.
    it("should throw when read fails in resolver", async () => {
      const bytesType = new BytesType();
      const resolver = type.createResolver(bytesType);
      const mockBuffer = {
        read: (_offset: number, _size: number) => Promise.resolve(undefined),
        // This is unused here.
        canReadMore: (_offset: number) => Promise.resolve(false),
      };
      const tap = new ReadableTap(mockBuffer);
      await assertRejects(
        () => resolver.read(tap),
        RangeError,
        "Attempt to read beyond buffer bounds.",
      );
    });
  });

  describe("Sync API", () => {
    it("toSyncBuffer should allocate enough space for multi-byte strings", () => {
      const value = "\u00e9".repeat(40);
      const buf = type.toSyncBuffer(value);
      assertEquals(buf.byteLength, 82);
      const tap = new SyncReadableTap(buf);
      assertEquals(type.readSync(tap), value);
    });

    it("toSyncBuffer should throw ValidationError for invalid value", () => {
      // deno-lint-ignore no-explicit-any
      assertThrows(() => type.toSyncBuffer(123 as any));
    });

    it("toSyncBuffer and readSync should serialize and deserialize strings", () => {
      const testStrings = ["hello", "", "test string", "🚀 emoji"];

      for (const str of testStrings) {
        const buf = type.toSyncBuffer(str);
        const tap = new SyncReadableTap(buf);
        assertEquals(type.readSync(tap), str);
      }
    });

    it("writeSync should write strings to tap", () => {
      const buf = new ArrayBuffer(100);
      const writeTap = new SyncWritableTap(buf);
      type.writeSync(writeTap, "test");
      const readTap = new SyncReadableTap(buf);
      assertEquals(type.readSync(readTap), "test");
    });

    it("writeSync should throw for non-strings", () => {
      const buf = new ArrayBuffer(10);
      const tap = new SyncWritableTap(buf);
      // deno-lint-ignore no-explicit-any
      const invalidValue: any = 123;
      assertThrows(() => type.writeSync(tap, invalidValue), Error);
    });

    it("skipSync should skip string in tap", () => {
      const str = "test";
      const buffer = type.toSyncBuffer(str);
      const tap = new SyncReadableTap(buffer);
      const posBefore = tap.getPos();
      type.skipSync(tap);
      const posAfter = tap.getPos();
      assertEquals(posAfter - posBefore, buffer.byteLength);
    });

    it("readSync should throw for insufficient data", () => {
      const buf = new ArrayBuffer(5);
      const writeTap = new SyncWritableTap(buf);
      writeTap.writeLong(10n); // Need to write long first
      const readTap = new SyncReadableTap(buf);
      assertThrows(
        () => type.readSync(readTap),
        ReadBufferError,
        "Operation exceeds buffer bounds",
      );
    });

    it("matchSync should match encoded string buffers", () => {
      const bufA = type.toSyncBuffer("a");
      const bufB = type.toSyncBuffer("b");
      const bufEmpty = type.toSyncBuffer("");

      assertEquals(
        type.matchSync(new SyncReadableTap(bufA), new SyncReadableTap(bufB)),
        -1,
      );
      assertEquals(
        type.matchSync(new SyncReadableTap(bufB), new SyncReadableTap(bufA)),
        1,
      );
      assertEquals(
        type.matchSync(
          new SyncReadableTap(bufA),
          new SyncReadableTap(type.toSyncBuffer("a")),
        ),
        0,
      );
      assertEquals(
        type.matchSync(
          new SyncReadableTap(bufEmpty),
          new SyncReadableTap(type.toSyncBuffer("")),
        ),
        0,
      );
    });

    describe("createResolver sync", () => {
      it("should create resolver for same type sync", () => {
        const resolver = type.createResolver(type);
        const str = "test";
        const buffer = type.toSyncBuffer(str);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap);
        assertEquals(result, str);
      });

      it("should create resolver for BytesType writer sync", () => {
        const bytesType = new BytesType();
        const resolver = type.createResolver(bytesType);
        const bytes = new Uint8Array([72, 101, 108, 108, 111]); // 'Hello'
        const buffer = bytesType.toSyncBuffer(bytes);
        const tap = new SyncReadableTap(buffer);
        const result = resolver.readSync(tap);
        assertEquals(result, "Hello");
      });

      it("should throw when reading bytes with insufficient data in resolver sync", () => {
        const bytesType = new BytesType();
        const resolver = type.createResolver(bytesType);
        const buf = new ArrayBuffer(5);
        const writeTap = new SyncWritableTap(buf);
        writeTap.writeLong(10n);
        const readTap = new SyncReadableTap(buf);
        assertThrows(
          () => resolver.readSync(readTap),
          ReadBufferError,
          "Operation exceeds buffer bounds",
        );
      });
    });
  });
});
