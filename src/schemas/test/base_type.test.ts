import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { TestTap as Tap } from "../../serialization/test/test_tap.ts";
import { SyncReadableTap } from "../../serialization/tap_sync.ts";
import type { WritableTapLike } from "../../serialization/tap.ts";
import type { SyncWritableTapLike } from "../../serialization/tap_sync.ts";
import { ReadBufferError } from "../../serialization/buffers/buffer_sync.ts";
import type { JSONType, Type } from "../type.ts";
import { Resolver } from "../resolver.ts";
import { BaseType } from "../base_type.ts";
import { FixedSizeBaseType } from "../primitive/fixed_size_base_type.ts";

/**
 * A simple concrete implementation of Type for testing purposes.
 * Handles string values.
 */
class TestType extends BaseType<string> {
  constructor() {
    super();
  }

  public override toJSON(): JSONType {
    return "test";
  }

  public override async writeUnchecked(
    tap: WritableTapLike,
    value: string,
  ): Promise<void> {
    await tap.writeString(value);
  }

  public override async read(tap: Tap): Promise<string> {
    return await tap.readString();
  }

  public override async skip(tap: Tap): Promise<void> {
    await tap.skipString();
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

  public override cloneFromValue(value: string): string {
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

  public override async match(tap1: Tap, tap2: Tap): Promise<number> {
    return await tap1.matchString(tap2);
  }

  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: string,
  ): void {
    tap.writeString(value);
  }

  public override readSync(tap: SyncReadableTap): string {
    return tap.readString();
  }

  public override skipSync(tap: SyncReadableTap): void {
    tap.skipString();
  }

  public override matchSync(
    tap1: SyncReadableTap,
    tap2: SyncReadableTap,
  ): number {
    return tap1.matchString(tap2);
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

  public override async writeUnchecked(
    tap: WritableTapLike,
    value: number,
  ): Promise<void> {
    await tap.writeDouble(value);
  }

  public override async read(tap: Tap): Promise<number> {
    return await tap.readDouble();
  }

  public override async skip(tap: Tap): Promise<void> {
    await tap.skipDouble();
  }

  public override check(value: unknown): boolean {
    return typeof value === "number" && isFinite(value);
  }

  public override cloneFromValue(value: number): number {
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

  public override async match(tap1: Tap, tap2: Tap): Promise<number> {
    return await tap1.matchDouble(tap2);
  }

  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: number,
  ): void {
    tap.writeDouble(value);
  }

  public override readSync(tap: SyncReadableTap): number {
    return tap.readDouble();
  }

  public override skipSync(tap: SyncReadableTap): void {
    tap.skipDouble();
  }

  public override matchSync(
    tap1: SyncReadableTap,
    tap2: SyncReadableTap,
  ): number {
    return tap1.matchDouble(tap2);
  }
}

describe("Type", () => {
  const type = new TestType();

  describe("toBuffer and fromBuffer", () => {
    it("should serialize and deserialize a string", async () => {
      const value = "hello world";
      const buffer = await type.toBuffer(value);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should handle empty string", async () => {
      const value = "";
      const buffer = await type.toBuffer(value);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should handle unicode strings", async () => {
      const value = "héllo wörld 🌍";
      const buffer = await type.toBuffer(value);
      const result = await type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("should throw for truncated buffers", async () => {
      const buffer = new ArrayBuffer(0);
      await assertRejects(
        () => type.fromBuffer(buffer),
        ReadBufferError,
        "Operation exceeds buffer bounds",
      );
    });

    it("should throw for extra data in buffer", async () => {
      const value = "hello";
      const buffer = await type.toBuffer(value);
      // Append extra data
      const extraBuffer = new ArrayBuffer(buffer.byteLength + 1);
      new Uint8Array(extraBuffer).set(new Uint8Array(buffer), 0);
      // Add a byte at the end
      new Uint8Array(extraBuffer)[buffer.byteLength] = 0;
      await assertRejects(
        () => type.fromBuffer(extraBuffer),
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
        errorHook: (
          path: string[],
          value: unknown,
          schemaType: Type,
        ) => {
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
      assert(typeof resolver.readSync === "function");
    });

    it("should resolve data correctly", async () => {
      const resolver = type.createResolver(type);
      const value = "test";
      const buffer = await type.toBuffer(value);
      const tap = new Tap(buffer);
      const resolved = await resolver.read(tap);
      assertEquals(resolved, value);
    });

    it("should resolve data correctly synchronously", () => {
      const resolver = type.createResolver(type);
      const value = "test";
      const buffer = type.toSyncBuffer(value);
      const tap = new SyncReadableTap(buffer);
      const resolved = resolver.readSync(tap);
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
      const cloned = type.cloneFromValue(value);
      assertEquals(cloned, value);
      // Strings are immutable, so reference equality is fine
    });

    it("should throw for invalid values", () => {
      assertThrows(() => {
        // deno-lint-ignore no-explicit-any
        (type as any).cloneFromValue(123);
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

  describe("match", () => {
    it("should match encoded buffers correctly", async () => {
      const value1 = "apple";
      const value2 = "banana";
      const value3 = "apple";

      const buffer1 = await type.toBuffer(value1);
      const buffer2 = await type.toBuffer(value2);
      const buffer3 = await type.toBuffer(value3);

      // apple < banana
      assertEquals(await type.match(new Tap(buffer1), new Tap(buffer2)), -1);
      // banana > apple
      assertEquals(await type.match(new Tap(buffer2), new Tap(buffer1)), 1);
      // apple == apple
      assertEquals(await type.match(new Tap(buffer1), new Tap(buffer3)), 0);
    });

    it("should handle empty strings", async () => {
      const emptyBuf = await type.toBuffer("");
      const nonEmptyBuf = await type.toBuffer("a");

      assertEquals(
        await type.match(new Tap(emptyBuf), new Tap(nonEmptyBuf)),
        -1,
      );
      assertEquals(
        await type.match(new Tap(nonEmptyBuf), new Tap(emptyBuf)),
        1,
      );
      assertEquals(
        await type.match(new Tap(emptyBuf), new Tap(await type.toBuffer(""))),
        0,
      );
    });

    it("should handle unicode strings", async () => {
      const value1 = "héllo";
      const value2 = "wörld";

      const buffer1 = await type.toBuffer(value1);
      const buffer2 = await type.toBuffer(value2);

      // Compare based on encoded byte order
      const result = await type.match(new Tap(buffer1), new Tap(buffer2));
      assert(typeof result === "number");
      assert(result !== 0); // They should be different
    });
  });

  describe("sync APIs", () => {
    describe("toSyncBuffer and fromSyncBuffer", () => {
      it("should serialize and deserialize a string synchronously", () => {
        const value = "hello world";
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should handle empty string", () => {
        const value = "";
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should handle unicode strings", () => {
        const value = "héllo wörld 🌍";
        const buffer = type.toSyncBuffer(value);
        const result = type.fromSyncBuffer(buffer);
        assertEquals(result, value);
      });

      it("should throw for truncated buffers", () => {
        const buffer = new ArrayBuffer(0);
        assertThrows(
          () => type.fromSyncBuffer(buffer),
          ReadBufferError,
        );
      });

      it("should throw for extra data in buffer", () => {
        const value = "hello";
        const buffer = type.toSyncBuffer(value);
        // Append extra data
        const extraBuffer = new ArrayBuffer(buffer.byteLength + 1);
        new Uint8Array(extraBuffer).set(new Uint8Array(buffer), 0);
        // Add a byte at the end
        new Uint8Array(extraBuffer)[buffer.byteLength] = 0;
        assertThrows(
          () => type.fromSyncBuffer(extraBuffer),
          Error,
          "Insufficient data for type",
        );
      });
    });

    describe("matchSync", () => {
      it("should match encoded buffers correctly", () => {
        const value1 = "apple";
        const value2 = "banana";
        const value3 = "apple";

        const buffer1 = type.toSyncBuffer(value1);
        const buffer2 = type.toSyncBuffer(value2);
        const buffer3 = type.toSyncBuffer(value3);

        // apple < banana
        assertEquals(
          type.matchSync(
            new SyncReadableTap(buffer1),
            new SyncReadableTap(buffer2),
          ),
          -1,
        );
        // banana > apple
        assertEquals(
          type.matchSync(
            new SyncReadableTap(buffer2),
            new SyncReadableTap(buffer1),
          ),
          1,
        );
        // apple == apple
        assertEquals(
          type.matchSync(
            new SyncReadableTap(buffer1),
            new SyncReadableTap(buffer3),
          ),
          0,
        );
      });

      it("should handle empty strings", () => {
        const emptyBuf = type.toSyncBuffer("");
        const nonEmptyBuf = type.toSyncBuffer("a");

        assertEquals(
          type.matchSync(
            new SyncReadableTap(emptyBuf),
            new SyncReadableTap(nonEmptyBuf),
          ),
          -1,
        );
        assertEquals(
          type.matchSync(
            new SyncReadableTap(nonEmptyBuf),
            new SyncReadableTap(emptyBuf),
          ),
          1,
        );
        assertEquals(
          type.matchSync(
            new SyncReadableTap(emptyBuf),
            new SyncReadableTap(type.toSyncBuffer("")),
          ),
          0,
        );
      });

      it("should handle unicode strings", () => {
        const value1 = "héllo";
        const value2 = "wörld";

        const buffer1 = type.toSyncBuffer(value1);
        const buffer2 = type.toSyncBuffer(value2);

        // Compare based on encoded byte order
        const result = type.matchSync(
          new SyncReadableTap(buffer1),
          new SyncReadableTap(buffer2),
        );
        assert(typeof result === "number");
        assert(result !== 0); // They should be different
      });
    });
  });
});
