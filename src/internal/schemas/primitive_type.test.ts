import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { TestTap as Tap } from "../serialization/test_tap.ts";
import { PrimitiveType } from "./primitive_type.ts";
import { JSONType, Type } from "./type.ts";
import { throwInvalidError, ValidationError } from "./error.ts";
import { encode } from "../serialization/text_encoding.ts";

/**
 * A simple concrete implementation of PrimitiveType for testing purposes.
 * Handles number values.
 */
class TestPrimitiveType extends PrimitiveType<number> {
  public override check(
    value: unknown,
    errorHook?: (
      path: string[],
      invalidValue: unknown,
      schemaType: PrimitiveType<number>,
    ) => void,
    path: string[] = [],
  ): boolean {
    const isValid = typeof value === "number" && Number.isInteger(value) &&
      value >= 0 && value <= 100;
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  public override async toBuffer(value: number): Promise<ArrayBuffer> {
    // Allocate 5 bytes (max size for 32-bit int varint)
    const buf = new ArrayBuffer(5);
    const tap = new Tap(buf);
    await this.write(tap, value);
    const result = await tap.getValue();
    return (result.buffer as ArrayBuffer).slice(
      result.byteOffset,
      result.byteOffset + result.byteLength,
    );
  }

  public override async read(tap: Tap): Promise<number> {
    return await tap.readInt();
  }

  public override async write(tap: Tap, value: number): Promise<void> {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    await tap.writeInt(value);
  }

  public override async skip(tap: Tap): Promise<void> {
    await tap.skipInt();
  }

  public override toJSON(): JSONType {
    return "test";
  }

  public override random(): number {
    return Math.floor(Math.random() * 101);
  }

  public override async match(tap1: Tap, tap2: Tap): Promise<number> {
    return await tap1.matchInt(tap2);
  }
}

/**
 * Another fake primitive type for testing different constructors.
 */
class FakePrimitiveType extends PrimitiveType<string> {
  public override check(
    value: unknown,
    errorHook?: (
      path: string[],
      invalidValue: unknown,
      schemaType: Type,
    ) => void,
    path: string[] = [],
  ): boolean {
    const isValid = typeof value === "string";
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  public override async toBuffer(value: string): Promise<ArrayBuffer> {
    const strBytes = encode(value);
    const buf = new ArrayBuffer(5 + strBytes.length);
    const tap = new Tap(buf);
    await tap.writeString(value);
    const result = await tap.getValue();
    return (result.buffer as ArrayBuffer).slice(
      result.byteOffset,
      result.byteOffset + result.byteLength,
    );
  }

  public override async read(tap: Tap): Promise<string> {
    return (await tap.readString())!;
  }

  public override async write(tap: Tap, value: string): Promise<void> {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    await tap.writeString(value);
  }

  public override async skip(tap: Tap): Promise<void> {
    await tap.skipString();
  }

  public override toJSON(): JSONType {
    return "fake";
  }

  public override random(): string {
    return "fake";
  }

  public override async match(tap1: Tap, tap2: Tap): Promise<number> {
    return await tap1.matchString(tap2);
  }
}

describe("PrimitiveType", () => {
  const type = new TestPrimitiveType();

  describe("clone", () => {
    it("should clone a valid number value", () => {
      const value = 42;
      const cloned = type.clone(value);
      assertEquals(cloned, value);
      assert(cloned !== value || typeof value !== "object"); // Primitives are immutable
    });

    it("should throw ValidationError for invalid values", () => {
      assertThrows(() => {
        type.clone(150); // Invalid since > 100
      }, ValidationError);
    });
  });

  describe("compare", () => {
    it("should compare numbers correctly", () => {
      assertEquals(type.compare(1, 2), -1);
      assertEquals(type.compare(2, 1), 1);
      assertEquals(type.compare(1, 1), 0);
    });

    it("should handle edge cases", () => {
      assertEquals(type.compare(0, 100), -1);
      assertEquals(type.compare(100, 0), 1);
    });
  });

  describe("match", () => {
    it("should match encoded buffers", async () => {
      const buf1 = await type.toBuffer(1);
      const buf2 = await type.toBuffer(2);

      assertEquals(await type.match(new Tap(buf1), new Tap(buf2)), -1);
      assertEquals(await type.match(new Tap(buf2), new Tap(buf1)), 1);
      assertEquals(
        await type.match(new Tap(buf1), new Tap(await type.toBuffer(1))),
        0,
      );
    });
  });

  describe("inheritance from BaseType", () => {
    it("should have toBuffer and fromBuffer from BaseType", async () => {
      const value = 50;
      const buffer = await type.toBuffer(value);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should have isValid from BaseType", () => {
      assert(type.isValid(50));
      assert(!type.isValid(150));
    });

    it("should create resolver for same type", async () => {
      const resolver = type.createResolver(type);
      const value = 42;
      const buffer = await type.toBuffer(value);
      const tap = new Tap(buffer);
      const result = await resolver.read(tap);
      assertEquals(result, value);
    });

    it("should throw error for different type", () => {
      const otherType = new FakePrimitiveType();
      assertThrows(
        () => {
          type.createResolver(otherType);
        },
        Error,
        "Schema evolution not supported from writer type: fake to reader type: test",
      );
    });
  });
});
