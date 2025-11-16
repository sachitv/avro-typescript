import { assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";
import { TestTap as Tap } from "../serialization/test_tap.ts";
import { type ErrorHook, ValidationError } from "./error.ts";
import type { JSONType } from "./type.ts";

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

  public async read(tap: Tap): Promise<number> {
    return (await tap.readInt()) || 0;
  }

  public async write(tap: Tap, value: number): Promise<void> {
    await tap.writeInt(value);
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

  public override async match(tap1: Tap, tap2: Tap): Promise<number> {
    return await tap1.matchInt(tap2);
  }
}

describe("FixedSizeBaseType", () => {
  const type = new TestFixedSizeType();

  describe("toBuffer", () => {
    it("should serialize value using fixed size", async () => {
      const value = 42;
      const buffer = await type.toBuffer(value);
      assertEquals(buffer.byteLength, 4);
      const tap = new Tap(buffer);
      assertEquals(await type.read(tap), value);
    });

    it("should throw ValidationError for invalid value", () => {
      assertRejects(async () => {
        await type.toBuffer("invalid" as unknown as number);
      }, ValidationError);
    });
  });

  describe("skip", () => {
    it("should skip fixed-size value using base class implementation", async () => {
      const value = 42;
      const buffer = await type.toBuffer(value);
      const tap = new Tap(buffer);
      const posBefore = tap.getPos();
      await type.skip(tap);
      const posAfter = tap.getPos();
      assertEquals(posAfter - posBefore, 4); // sizeBytes() returns 4
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
});
