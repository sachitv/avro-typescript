import { assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { StringType } from "./string_type.ts";
import { BytesType } from "./bytes_type.ts";
import { Tap } from "../serialization/tap.ts";

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
    const posBefore = tap._testOnlyPos;
    await type.skip(tap);
    const posAfter = tap._testOnlyPos;
    assertEquals(posAfter - posBefore, buffer.byteLength);
  });

  it("read should throw for insufficient data", async () => {
    const buf = new ArrayBuffer(5);
    const writeTap = new Tap(buf);
    await writeTap.writeLong(10n);
    const readTap = new Tap(buf);
    await assertRejects(
      () => type.read(readTap),
      Error,
      "Insufficient data for string",
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
        Error,
        "Insufficient data for bytes",
      );
    });
  });
});
