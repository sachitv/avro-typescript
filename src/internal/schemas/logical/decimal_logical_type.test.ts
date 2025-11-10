import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { DecimalLogicalType } from "./decimal_logical_type.ts";
import { BytesType } from "../bytes_type.ts";
import { FixedType } from "../fixed_type.ts";
import { resolveNames } from "../resolve_names.ts";
import { ValidationError } from "../error.ts";

function createFixed(name: string, size: number): FixedType {
  const names = resolveNames({ name });
  return new FixedType({ ...names, size });
}

describe("DecimalLogicalType", () => {
  describe("serialization", () => {
    it("round-trips bigint values using bytes", async () => {
      const type = new DecimalLogicalType(new BytesType(), {
        precision: 10,
        scale: 2,
      });
      const value = 12345n;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("round-trips bigint values using fixed", async () => {
      const fixed = createFixed("FixedDecimal", 8);
      const type = new DecimalLogicalType(fixed, { precision: 18, scale: 4 });
      const value = -999999n;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("validates precision when serializing", async () => {
      const type = new DecimalLogicalType(new BytesType(), {
        precision: 4,
        scale: 2,
      });
      await assertRejects(
        async () => {
          await type.toBuffer(123456n);
        },
        ValidationError,
      );
    });
  });

  describe("validation", () => {
    it("throws when precision exceeds fixed capacity", () => {
      const fixed = createFixed("SmallFixed", 2);
      assertThrows(() => {
        new DecimalLogicalType(fixed, { precision: 20, scale: 2 });
      });
    });

    it("throws when underlying type is invalid", () => {
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => new DecimalLogicalType({} as any, { precision: 1 }),
        Error,
        "Decimal logical type requires bytes or fixed underlying type.",
      );
    });

    it("throws when precision is not positive integer", () => {
      assertThrows(
        () => new DecimalLogicalType(new BytesType(), { precision: 0 }),
        Error,
        "Decimal logical type requires a positive precision.",
      );
      assertThrows(
        () => new DecimalLogicalType(new BytesType(), { precision: 1.5 }),
        Error,
        "Decimal logical type requires a positive precision.",
      );
    });

    it("throws when scale is invalid", () => {
      assertThrows(
        () =>
          new DecimalLogicalType(new BytesType(), { precision: 1, scale: -1 }),
        Error,
        "Decimal logical type requires a valid scale (0 <= scale <= precision).",
      );
      assertThrows(
        () =>
          new DecimalLogicalType(new BytesType(), { precision: 1, scale: 2 }),
        Error,
        "Decimal logical type requires a valid scale (0 <= scale <= precision).",
      );
      assertThrows(
        () =>
          new DecimalLogicalType(new BytesType(), { precision: 1, scale: 1.5 }),
        Error,
        "Decimal logical type requires a valid scale (0 <= scale <= precision).",
      );
    });

    it("throws when decimal value does not fit within fixed size", async () => {
      const fixed = createFixed("SmallFixed", 2);
      const type = new DecimalLogicalType(fixed, { precision: 4, scale: 0 });
      // This value requires more than 2 bytes to store (max for 2 bytes is ±32767)
      const largeValue = 123456n;
      await assertRejects(
        async () => {
          await type.toBuffer(largeValue);
        },
        ValidationError,
      );
    });

    it("throws when decimal value exceeds declared precision for fixed type", async () => {
      const fixed = createFixed("SmallFixed", 2);
      const type = new DecimalLogicalType(fixed, { precision: 4, scale: 0 });
      // Value 20000 has five digits which exceeds the declared precision of 4.
      const value = 20000n;
      await assertRejects(
        async () => {
          await type.toBuffer(value);
        },
        Error,
        `Invalid value: '20000' for type: \n{\n  "name": "SmallFixed",\n  "type": "fixed",\n  "size": 2,\n  "logicalType": "decimal",\n  "precision": 4\n}\n`,
      );
    });
  });

  describe("JSON schema", () => {
    it("produces JSON schema with logical attributes", () => {
      const type = new DecimalLogicalType(new BytesType(), {
        precision: 8,
        scale: 3,
      });
      assertEquals(type.toJSON(), {
        type: "bytes",
        logicalType: "decimal",
        precision: 8,
        scale: 3,
      });
    });

    it("produces JSON schema without scale when zero", () => {
      const type = new DecimalLogicalType(new BytesType(), { precision: 8 });
      assertEquals(type.toJSON(), {
        type: "bytes",
        logicalType: "decimal",
        precision: 8,
      });
    });
  });

  describe("comparison", () => {
    it("compares decimal values via underlying bigints", () => {
      const type = new DecimalLogicalType(new BytesType(), { precision: 6 });
      assertEquals(type.compare(10n, 10n), 0);
      assertEquals(type.compare(10n, 11n), -1);
      assertEquals(type.compare(-5n, -6n), 1);
    });
  });

  describe("random value generation", () => {
    it("creates resolver when reading from bytes writer", () => {
      const reader = new DecimalLogicalType(new BytesType(), {
        precision: 6,
        scale: 2,
      });
      const writer = new DecimalLogicalType(new BytesType(), {
        precision: 6,
        scale: 2,
      });
      const resolver = reader.createResolver(writer);
      assert(resolver !== undefined);
    });
  });

  describe("getters", () => {
    it("returns precision and scale via getters", () => {
      const type = new DecimalLogicalType(new BytesType(), {
        precision: 10,
        scale: 3,
      });
      assertEquals(type.getPrecision(), 10);
      assertEquals(type.getScale(), 3);
    });

    it("returns default scale when not specified", () => {
      const type = new DecimalLogicalType(new BytesType(), { precision: 8 });
      assertEquals(type.getPrecision(), 8);
      assertEquals(type.getScale(), 0);
    });
  });

  describe("internal methods", () => {
    it("decodeBigInt handles empty bytes", () => {
      class TestDecimal extends DecimalLogicalType {
        public testDecode(bytes: Uint8Array): bigint {
          return super.fromUnderlying(bytes);
        }
      }
      const type = new TestDecimal(new BytesType(), { precision: 1 });
      assertEquals(type.testDecode(new Uint8Array(0)), 0n);
    });

    it("throws when toUnderlying detects value too large for fixed size", () => {
      class TestDecimal extends DecimalLogicalType {
        public testToUnderlying(value: bigint): Uint8Array {
          return super.toUnderlying(value);
        }
      }
      const fixed = createFixed("TinyFixed", 1);
      const type = new TestDecimal(fixed, { precision: 2, scale: 0 });
      // Value 128 requires 2 bytes but fixed size is 1
      assertThrows(
        () => type.testToUnderlying(128n),
        Error,
        "Decimal value: 128 exceeds declared precision: 2",
      );
    });
  });
});
