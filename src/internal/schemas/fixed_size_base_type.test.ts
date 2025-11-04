import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";
import { Tap } from "../serialization/tap.ts";
import { ErrorHook, ValidationError } from "./error.ts";
import { JSONType } from "./type.ts";

// Simple concrete implementation for testing
class TestFixedSizeType extends FixedSizeBaseType<number> {
  public sizeBytes(): number {
    return 4;
  }

  public check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = typeof value === "number";
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  public read(tap: Tap): number {
    return tap.readInt() || 0;
  }

  public write(tap: Tap, value: number): void {
    tap.writeInt(value);
  }

  public clone(value: number): number {
    return value;
  }

  public compare(val1: number, val2: number): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  public random(): number {
    return Math.floor(Math.random() * 100);
  }

  public override toJSON(): JSONType {
    return "test";
  }
}

describe("FixedSizeBaseType", () => {
  const type = new TestFixedSizeType();

  describe("toBuffer", () => {
    it("should serialize value using fixed size", () => {
      const value = 42;
      const buffer = type.toBuffer(value);
      assertEquals(buffer.byteLength, 4);
      const tap = new Tap(buffer);
      assertEquals(type.read(tap), value);
    });

    it("should throw ValidationError for invalid value", () => {
      assertThrows(() => {
        type.toBuffer("invalid" as unknown as number);
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip fixed-size value using base class implementation", () => {
      const value = 42;
      const buffer = type.toBuffer(value);
      const tap = new Tap(buffer);
      const posBefore = tap._testOnlyPos;
      type.skip(tap);
      const posAfter = tap._testOnlyPos;
      assertEquals(posAfter - posBefore, 4); // sizeBytes() returns 4
    });
  });
});
