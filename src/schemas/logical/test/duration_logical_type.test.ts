import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  DurationLogicalType,
  type DurationValue,
} from "../duration_logical_type.ts";
import { FixedType } from "../../complex/fixed_type.ts";
import { resolveNames } from "../../complex/resolve_names.ts";
import { ValidationError } from "../../error.ts";

function createFixed(name: string): FixedType {
  const names = resolveNames({ name });
  return new FixedType({ ...names, size: 12 });
}

describe("DurationLogicalType", () => {
  const type = new DurationLogicalType(createFixed("DurationFixed"));

  it("validates duration objects", () => {
    const value: DurationValue = { months: 1, days: 2, millis: 3 };
    assert(type.isValid(value));
    assert(!type.isValid({ months: -1, days: 2, millis: 3 }));
  });

  it("round-trips duration values", async () => {
    const value: DurationValue = { months: 5, days: 12, millis: 9000 };
    const buffer = await type.toBuffer(value);
    const decoded = await type.fromBuffer(buffer);
    assertEquals(decoded, value);
  });

  it("rejects invalid objects on serialization", async () => {
    await assertRejects(
      async () => {
        // deno-lint-ignore no-explicit-any
        await type.toBuffer({ months: -1 } as any);
      },
      ValidationError,
    );
  });

  it("provides duration JSON schema", () => {
    assertEquals(type.toJSON(), {
      name: "DurationFixed",
      type: "fixed",
      size: 12,
      logicalType: "duration",
    });
  });

  it("throws on invalid fixed size", () => {
    const names = resolveNames({ name: "test" });
    assertThrows(
      () => new DurationLogicalType(new FixedType({ ...names, size: 10 })),
      Error,
      "Duration logical type requires fixed size of 12 bytes.",
    );
  });

  it("throws on invalid buffer length", async () => {
    const buffer = new ArrayBuffer(10);
    await assertRejects(() => type.fromBuffer(buffer), Error);
  });

  it("rejects non-objects", () => {
    assert(!type.isValid(null));
    assert(!type.isValid("string"));
    assert(!type.isValid(123));
  });

  describe("canReadFromLogical", () => {
    it("returns true for DurationLogicalType", () => {
      const otherType = new DurationLogicalType(createFixed("OtherDuration"));
      // Access protected method through type assertion for testing
      const canRead = (type as unknown as {
        canReadFromLogical: (writer: unknown) => boolean;
      }).canReadFromLogical(otherType);
      assert(canRead);
    });

    it("returns false for other logical types", () => {
      const mockLogicalType = {
        constructor: { name: "OtherLogicalType" },
      };
      // Access protected method through type assertion for testing
      const canRead = (type as unknown as {
        canReadFromLogical: (writer: unknown) => boolean;
      }).canReadFromLogical(mockLogicalType);
      assert(!canRead);
    });
  });

  it("throws on invalid duration value during serialization", async () => {
    // Test the error path in toUnderlying method (lines 29-31)
    await assertRejects(
      async () => {
        // deno-lint-ignore no-explicit-any
        await type.toBuffer({ months: -1, days: 2, millis: 3 } as any);
      },
      ValidationError,
    );
  });

  it("throws on invalid buffer length during deserialization", () => {
    // Test the specific error path in fromUnderlying method (lines 41-43)
    // Call fromUnderlying directly with a Uint8Array of wrong length
    const invalidUint8Array = new Uint8Array(10); // Wrong length
    assertThrows(
      () =>
        (type as unknown as {
          fromUnderlying: (value: Uint8Array) => DurationValue;
        }).fromUnderlying(invalidUint8Array),
      Error,
      "Duration bytes must be 12 bytes long.",
    );
  });

  describe("compare", () => {
    it("compares equal durations", () => {
      const a: DurationValue = { months: 5, days: 10, millis: 1000 };
      const b: DurationValue = { months: 5, days: 10, millis: 1000 };
      assertEquals(type.compare(a, b), 0);
    });

    it("compares durations by months", () => {
      const a: DurationValue = { months: 3, days: 10, millis: 1000 };
      const b: DurationValue = { months: 5, days: 10, millis: 1000 };
      assertEquals(type.compare(a, b), -1);
      assertEquals(type.compare(b, a), 1);
    });

    it("compares durations by days when months equal", () => {
      const a: DurationValue = { months: 5, days: 5, millis: 1000 };
      const b: DurationValue = { months: 5, days: 10, millis: 1000 };
      assertEquals(type.compare(a, b), -1);
      assertEquals(type.compare(b, a), 1);
    });

    it("compares durations by millis when months and days equal", () => {
      const a: DurationValue = { months: 5, days: 10, millis: 500 };
      const b: DurationValue = { months: 5, days: 10, millis: 1000 };
      assertEquals(type.compare(a, b), -1);
      assertEquals(type.compare(b, a), 1);
    });
  });

  describe("random", () => {
    it("generates valid duration values", () => {
      const value = type.random();
      assert(type.isValid(value));
      assert(typeof value.months === "number");
      assert(typeof value.days === "number");
      assert(typeof value.millis === "number");
    });

    it("generates values within expected ranges", () => {
      const value = type.random();
      assert(value.months >= 0 && value.months < 1200);
      assert(value.days >= 0 && value.days < 365);
      assert(value.millis >= 0 && value.millis < 86_400_000);
    });

    it("generates different values on multiple calls", () => {
      const values = new Set();
      for (let i = 0; i < 10; i++) {
        const value = type.random();
        values.add(`${value.months}-${value.days}-${value.millis}`);
      }
      // Should generate multiple different values (very high probability)
      assert(values.size > 1);
    });
  });
});
