import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { Tap } from "../serialization/tap.ts";
import { JSONType, Type } from "./type.ts";
import { Resolver } from "./resolver.ts";
import { BaseType } from "./base_type.ts";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";

/**
 * A simple concrete implementation of Type for testing purposes.
 * Handles string values.
 */
class TestType extends BaseType<string> {
  public override toJSON(): JSONType {
    return "test";
  }

  public override toBuffer(value: string): ArrayBuffer {
    const buf = new ArrayBuffer(value.length + 10); // extra space for length encoding
    const tap = new Tap(buf);
    this.write(tap, value);
    return buf;
  }

  public override write(tap: Tap, value: string): void {
    tap.writeString(value);
  }

  public override read(tap: Tap): string {
    const val = tap.readString();
    return val ?? "";
  }

  public override skip(tap: Tap): void {
    tap.skipString();
  }

  public override check(
    value: unknown,
    errorHook?: (
      path: string[],
      invalidValue: unknown,
      schemaType: Type,
    ) => void,
  ): boolean {
    const isValid = typeof value === "string";
    if (!isValid && errorHook) {
      errorHook([], value, this);
    }
    return isValid;
  }

  public override clone(value: string): string {
    if (!this.check(value)) {
      throw new Error(`Invalid value for clone: ${JSON.stringify(value)}`);
    }
    return value; // Simple clone for strings
  }

  public override compare(a: string, b: string): number {
    return a.localeCompare(b);
  }

  public override random(): string {
    return Math.random().toString(36).substring(2);
  }
}

/**
 * Another type for testing incompatible resolvers.
 */
class OtherType extends FixedSizeBaseType<number> {
  public override toJSON(): JSONType {
    return "other";
  }

  // FixedSizeBaseType requires a sizeBytes implementation; a double is 8 bytes.
  public override sizeBytes(): number {
    return 8;
  }

  public override write(tap: Tap, value: number): void {
    tap.writeDouble(value);
  }

  public override read(tap: Tap): number {
    const val = tap.readDouble();
    if (val === undefined) {
      throw new Error("Insufficient data");
    }
    return val;
  }

  public override skip(tap: Tap): void {
    tap.skipDouble();
  }

  public override check(value: unknown): boolean {
    return typeof value === "number" && isFinite(value);
  }

  public override clone(value: number): number {
    if (!this.check(value)) {
      throw new Error(`Invalid value for clone: ${JSON.stringify(value)}`);
    }
    return value;
  }

  public override compare(a: number, b: number): number {
    return a < b ? -1 : a > b ? 1 : 0;
  }

  public override random(): number {
    return Math.random();
  }
}

describe("Type", () => {
  const type = new TestType();

  describe("toBuffer and fromBuffer", () => {
    it("should serialize and deserialize a string", () => {
      const value = "hello world";
      const buffer = type.toBuffer(value);
      const result = type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should handle empty string", () => {
      const value = "";
      const buffer = type.toBuffer(value);
      const result = type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should handle unicode strings", () => {
      const value = "héllo wörld 🌍";
      const buffer = type.toBuffer(value);
      const result = type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should throw for truncated buffers", () => {
      const buffer = new ArrayBuffer(0);
      assertThrows(
        () => type.fromBuffer(buffer),
        Error,
        "Insufficient data for type",
      );
    });
  });

  describe("isValid", () => {
    it("should return true for valid strings", () => {
      assert(type.isValid("hello"));
      assert(type.isValid(""));
      assert(type.isValid("123"));
    });

    it("should return false for invalid values", () => {
      assert(!type.isValid(123));
      assert(!type.isValid(null));
      assert(!type.isValid(undefined));
      assert(!type.isValid({}));
      assert(!type.isValid([]));
    });

    it("should call errorHook for invalid values", () => {
      let called = false;
      let capturedPath: string[] = [];
      let capturedValue: unknown;
      let capturedType: Type | undefined;

      type.isValid(123, {
        errorHook: (path, value, schemaType) => {
          called = true;
          capturedPath = path;
          capturedValue = value;
          capturedType = schemaType;
        },
      });

      assert(called);
      assertEquals(capturedPath, []);
      assertEquals(capturedValue, 123);
      assertEquals(capturedType, type);
    });

    it("should not call errorHook for valid values", () => {
      let called = false;

      type.isValid("valid", {
        errorHook: () => {
          called = true;
        },
      });

      assert(!called);
    });
  });

  describe("createResolver", () => {
    it("should return a resolver for the same type", () => {
      const resolver = type.createResolver(type);
      assert(resolver instanceof Resolver);
      assert(typeof resolver.read === "function");
    });

    it("should resolve data correctly", () => {
      const resolver = type.createResolver(type);
      const value = "test";
      const buffer = type.toBuffer(value);
      const tap = new Tap(buffer);
      const resolved = resolver.read(tap);
      assertEquals(resolved, value);
    });

    it("should throw for incompatible types", () => {
      const otherType = new OtherType();
      assertThrows(
        () => {
          type.createResolver(otherType);
        },
        Error,
        "Schema evolution not supported from writer type: other to reader type: test",
      );
    });
  });

  describe("clone", () => {
    it("should clone a string value", () => {
      const value = "test";
      const cloned = type.clone(value);
      assertEquals(cloned, value);
      // Strings are immutable, so reference equality is fine
    });

    it("should throw for invalid values", () => {
      assertThrows(() => {
        // deno-lint-ignore no-explicit-any
        (type as any).clone(123);
      });
    });
  });

  describe("compare", () => {
    it("should compare strings lexicographically", () => {
      assertEquals(type.compare("a", "b"), -1);
      assertEquals(type.compare("b", "a"), 1);
      assertEquals(type.compare("a", "a"), 0);
    });
  });

  describe("random", () => {
    it("should generate a random string", () => {
      const randomValue = type.random();
      assert(typeof randomValue === "string");
      assert(randomValue.length > 0);
    });
  });
});
