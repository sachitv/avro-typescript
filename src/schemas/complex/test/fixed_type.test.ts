import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { FixedType } from "../fixed_type.ts";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import { ReadableTap } from "../../../serialization/tap.ts";
import type { Type } from "../../type.ts";
import { ValidationError } from "../../error.ts";
import { resolveNames } from "../resolve_names.ts";

function createFixedType(
  name: string,
  size: number,
  namespace?: string,
  aliases?: string[],
): FixedType {
  const resolvedNames = resolveNames({ name, namespace, aliases });
  return new FixedType({ ...resolvedNames, size });
}

describe("FixedType", () => {
  it("creates FixedType with name, namespace, aliases and size", () => {
    const fixedType = createFixedType("MD5", 16, "org.example.types", [
      "Hash",
      "Checksum",
    ]);

    assertEquals(fixedType.getFullName(), "org.example.types.MD5");
    assertEquals(fixedType.getNamespace(), "org.example.types");
    assertEquals(fixedType.getAliases(), [
      "org.example.types.Hash",
      "org.example.types.Checksum",
    ]);
    assertEquals(fixedType.getSize(), 16);
    assertEquals(fixedType.sizeBytes(), 16);
  });

  it("validates size parameter in constructor", () => {
    assertThrows(
      () => createFixedType("Invalid", 0),
      Error,
      "Invalid fixed size: 0. Size must be a positive integer.",
    );

    assertThrows(
      () => createFixedType("Invalid", -5),
      Error,
      "Invalid fixed size: -5. Size must be a positive integer.",
    );

    assertThrows(
      () => createFixedType("Invalid", 3.14),
      Error,
      "Invalid fixed size: 3.14. Size must be a positive integer.",
    );
  });

  it("validates Uint8Array values of correct size", () => {
    const fixedType = createFixedType("Test", 4);

    // Valid values
    assertEquals(fixedType.check(new Uint8Array([1, 2, 3, 4])), true);
    assertEquals(fixedType.check(new Uint8Array([0, 0, 0, 0])), true);

    // Invalid values
    assertEquals(fixedType.check(new Uint8Array([1, 2, 3])), false); // Too short
    assertEquals(fixedType.check(new Uint8Array([1, 2, 3, 4, 5])), false); // Too long
    assertEquals(fixedType.check("not bytes"), false);
    assertEquals(fixedType.check(123), false);
    assertEquals(fixedType.check(null), false);
    assertEquals(fixedType.check(undefined), false);
  });

  it("calls error hook for invalid values", () => {
    const fixedType = createFixedType("Test", 4);
    let errorCalled = false;
    let errorPath: string[] = [];
    let errorValue: unknown;

    const errorHook = (path: string[], value: unknown) => {
      errorCalled = true;
      errorPath = path;
      errorValue = value;
    };

    // Test invalid value with error hook
    assertEquals(
      fixedType.check(new Uint8Array([1, 2, 3]), errorHook, ["test"]),
      false,
    );
    assertEquals(errorCalled, true);
    assertEquals(errorPath, ["test"]);
    assertEquals(errorValue, new Uint8Array([1, 2, 3]));
  });

  it("does not call error hook for valid values", () => {
    const fixedType = createFixedType("Test", 4);
    let errorCalled = false;

    const errorHook = () => {
      errorCalled = true;
    };

    // Test valid value with error hook (should not call hook)
    assertEquals(
      fixedType.check(new Uint8Array([1, 2, 3, 4]), errorHook, ["test"]),
      true,
    );
    assertEquals(errorCalled, false);
  });

  it("serializes and deserializes fixed-size byte arrays", async () => {
    const fixedType = createFixedType("Test", 4);
    const originalBytes = new Uint8Array([1, 2, 3, 4]);

    // Test write and read
    const buf = new ArrayBuffer(4);
    const tap = new Tap(buf);
    await fixedType.write(tap, originalBytes);

    const tap2 = new Tap(buf);
    const readBytes = await fixedType.read(tap2);

    assertEquals(readBytes, originalBytes);
  });

  it("throws error when writing invalid values", async () => {
    const fixedType = createFixedType("Test", 4);
    const buf = new ArrayBuffer(4);
    const tap = new Tap(buf);

    await assertRejects(
      async () => await fixedType.write(tap, new Uint8Array([1, 2, 3])),
      Error,
      "Invalid value",
    );

    await assertRejects(
      async () => await fixedType.write(tap, new Uint8Array([1, 2, 3, 4, 5])),
      Error,
      "Invalid value",
    );
  });

  it("throws error when reading insufficient data", async () => {
    const fixedType = createFixedType("Test", 4);
    const buf = new ArrayBuffer(2); // Not enough data
    const tap = new Tap(buf);

    await assertRejects(
      async () => await fixedType.read(tap),
      RangeError,
      "Operation exceeds buffer bounds",
    );
  });

  it("should throw when tap.readFixed returns undefined", async () => {
    const fixedType = createFixedType("Test", 4);
    const mockBuffer = {
      read: (_offset: number, _size: number) => Promise.resolve(undefined),
    };
    const tap = new ReadableTap(mockBuffer);
    await assertRejects(
      async () => {
        await fixedType.read(tap);
      },
      Error,
      "Insufficient data for fixed type",
    );
  });

  it("compares fixed-size byte arrays lexicographically", () => {
    const fixedType = createFixedType("Test", 4);
    const bytes1 = new Uint8Array([1, 2, 3, 4]);
    const bytes2 = new Uint8Array([1, 2, 3, 5]);
    const bytes3 = new Uint8Array([1, 2, 3, 4]);

    // bytes1 < bytes2 (differs at position 3: 4 < 5)
    assertEquals(fixedType.compare(bytes1, bytes2), -1);
    // bytes2 > bytes1 (differs at position 3: 5 > 4)
    assertEquals(fixedType.compare(bytes2, bytes1), 1);
    // bytes1 == bytes3 (identical arrays)
    assertEquals(fixedType.compare(bytes1, bytes3), 0);
  });

  it("throws error for non-Uint8Array inputs in comparison", () => {
    const fixedType = createFixedType("Test", 4);
    const validBytes = new Uint8Array([1, 2, 3, 4]);

    assertThrows(
      () => fixedType.compare("not bytes" as unknown as Uint8Array, validBytes),
      Error,
      "Fixed comparison requires Uint8Array values.",
    );

    assertThrows(
      () => fixedType.compare(validBytes, "not bytes" as unknown as Uint8Array),
      Error,
      "Fixed comparison requires Uint8Array values.",
    );
  });

  it("throws error for wrong size inputs in comparison", () => {
    const fixedType = createFixedType("Test", 4);
    const validBytes = new Uint8Array([1, 2, 3, 4]);

    assertThrows(
      () => fixedType.compare(new Uint8Array([1, 2, 3]), validBytes),
      Error,
      "Fixed values must be exactly 4 bytes.",
    );

    assertThrows(
      () => fixedType.compare(validBytes, new Uint8Array([1, 2, 3])),
      Error,
      "Fixed values must be exactly 4 bytes.",
    );
  });

  it("creates deep copy of fixed-size byte arrays when cloning", () => {
    const fixedType = createFixedType("Test", 4);
    const original = new Uint8Array([1, 2, 3, 4]);
    const cloned = fixedType.clone(original);

    assertEquals(cloned, original);
    assertEquals(cloned === original, false); // Different instances
  });

  it("clones JSON string defaults", () => {
    const fixedType = createFixedType("Test", 3);
    const cloned = fixedType.clone("\u0001\u0002\u0003");
    assertEquals([...cloned], [1, 2, 3]);
  });

  it("throws error when cloning invalid values", () => {
    const fixedType = createFixedType("Test", 4);

    assertThrows(
      () => fixedType.clone(new Uint8Array([1, 2, 3])),
      Error,
      "Invalid value",
    );
  });

  it("throws ValidationError when cloning unsupported default types", () => {
    const fixedType = createFixedType("Test", 4);
    assertThrows(() => {
      fixedType.clone(123 as unknown);
    }, ValidationError);
  });

  it("generates random fixed-size byte arrays", () => {
    const fixedType = createFixedType("Test", 8);
    const random = fixedType.random();

    assertEquals(random.length, 8);
    assertEquals(random instanceof Uint8Array, true);
  });

  it("when created with namespace and aliases, toJSON provides correct JSON representation", () => {
    const fixedType = createFixedType("MD5", 16, "org.example.types", [
      "Hash",
      "Checksum",
    ]);

    assertEquals(fixedType.toJSON(), {
      name: "org.example.types.MD5",
      type: "fixed",
      size: 16,
    });
  });

  it("when created without namespace, toJSON provides correct JSON representation", () => {
    const fixedType = createFixedType("Simple", 4);

    assertEquals(fixedType.toJSON(), {
      name: "Simple",
      type: "fixed",
      size: 4,
    });
  });

  it("converts values to ArrayBuffer", async () => {
    const fixedType = createFixedType("Test", 4);
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const buffer = await fixedType.toBuffer(bytes);

    assertEquals(buffer.byteLength, 4);
    const view = new Uint8Array(buffer);
    assertEquals(view, bytes);
  });

  it("validates values before converting to buffer", async () => {
    const fixedType = createFixedType("Test", 4);

    await assertRejects(
      async () => await fixedType.toBuffer(new Uint8Array([1, 2, 3])),
      Error,
      "Invalid value",
    );
  });

  it("skips fixed-size values in tap", async () => {
    const fixedType = createFixedType("Test", 4);
    const buf = new ArrayBuffer(8);
    const view = new Uint8Array(buf);
    view.set([1, 2, 3, 4, 5, 6, 7, 8]);

    const tap = new Tap(buf);
    assertEquals(tap.getPos(), 0);

    await fixedType.skip(tap);
    assertEquals(tap.getPos(), 4);

    await fixedType.skip(tap);
    assertEquals(tap.getPos(), 8);
  });

  it("matches encoded fixed-size buffers", async () => {
    const fixedType = createFixedType("Test", 4);
    const bytes1 = new Uint8Array([1, 2, 3, 4]);
    const bytes2 = new Uint8Array([1, 2, 3, 4]);
    const bytes3 = new Uint8Array([1, 2, 3, 5]);

    const buf1 = new ArrayBuffer(4);
    const buf2 = new ArrayBuffer(4);
    const buf3 = new ArrayBuffer(4);

    new Uint8Array(buf1).set(bytes1);
    new Uint8Array(buf2).set(bytes2);
    new Uint8Array(buf3).set(bytes3);

    const tapMatch1 = new Tap(buf1);
    const tapMatch2 = new Tap(buf2);
    assertEquals(await fixedType.match(tapMatch1, tapMatch2), 0); // Match

    const tapDiff1 = new Tap(buf1);
    const tapDiff2 = new Tap(buf3);
    assertEquals(await fixedType.match(tapDiff1, tapDiff2), -1); // bytes1 < bytes3
  });

  it("creates resolver for identical FixedType", () => {
    const readerType = createFixedType("MD5", 16, "org.example.types");
    const writerType = createFixedType("MD5", 16, "org.example.types");

    const resolver = readerType.createResolver(writerType);
    assertEquals(resolver.constructor.name, "FixedResolver");
  });

  it("creates resolver for FixedType with alias", () => {
    const readerType = createFixedType("Hash", 16, "org.example.types", [
      "MD5",
      "Checksum",
    ]);
    const writerType = createFixedType("MD5", 16, "org.example.types");

    const resolver = readerType.createResolver(writerType);
    assertEquals(resolver.constructor.name, "FixedResolver");
  });

  it("throws error for incompatible FixedType names", () => {
    const readerType = createFixedType("MD5", 16, "org.example.types");
    const writerType = createFixedType("SHA1", 16, "org.example.types");

    assertThrows(
      () => readerType.createResolver(writerType),
      Error,
      "Schema evolution not supported from writer type: org.example.types.SHA1 to reader type: org.example.types.MD5",
    );
  });

  it("throws error for different FixedType sizes", () => {
    const readerType = createFixedType("MD5", 16, "org.example.types");
    const writerType = createFixedType("MD5", 20, "org.example.types");

    assertThrows(
      () => readerType.createResolver(writerType),
      Error,
      "Cannot resolve fixed types with different sizes: writer has 20, reader has 16",
    );
  });

  it("reads values through FixedType resolver", async () => {
    const readerType = createFixedType("MD5", 16, "org.example.types");
    const writerType = createFixedType("MD5", 16, "org.example.types");

    const resolver = readerType.createResolver(writerType);
    const testData = new Uint8Array([
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
    ]);

    const buf = new ArrayBuffer(16);
    const tap = new Tap(buf);
    await tap.writeFixed(testData, 16);

    const tap2 = new Tap(buf);
    const readData = await resolver.read(tap2);

    assertEquals(readData, testData);
  });

  it("matches names including aliases", () => {
    const fixedType = createFixedType("MD5", 16, "org.example.types", [
      "Hash",
      "Checksum",
    ]);

    assertEquals(fixedType.matchesName("org.example.types.MD5"), true);
    assertEquals(fixedType.matchesName("MD5"), false); // Not fully qualified
    assertEquals(fixedType.matchesName("org.example.types.Hash"), true);
    assertEquals(fixedType.matchesName("org.example.types.Checksum"), true);
    assertEquals(fixedType.matchesName("org.example.types.Unknown"), false);
  });

  it("falls back to base resolver for non-FixedType", () => {
    const fixedType = createFixedType("MD5", 16, "org.example.types");

    // Create a mock type that's not FixedType
    const mockType = {
      constructor: { name: "mock.MockType" },
      getAliases: () => [],
      toJSON: () => "mock",
    } as unknown as Type;

    // This should fall back to the base class implementation which throws an error
    assertThrows(
      () => fixedType.createResolver(mockType),
      Error,
      `Schema evolution not supported from writer type: mock to reader type: 
{
  "name": "org.example.types.MD5",
  "type": "fixed",
  "size": 16
}
`,
    );
  });
});
