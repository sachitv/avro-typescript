import { assertEquals, assertInstanceOf, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { createType } from "./mod.ts";
import { BytesType } from "../schemas/bytes_type.ts";
import { DecimalLogicalType } from "../schemas/logical/decimal_logical_type.ts";
import { DurationLogicalType } from "../schemas/logical/duration_logical_type.ts";
import {
  DateLogicalType,
  LocalTimestampMicrosLogicalType,
  LocalTimestampMillisLogicalType,
  LocalTimestampNanosLogicalType,
  TimeMicrosLogicalType,
  TimeMillisLogicalType,
  TimestampMicrosLogicalType,
  TimestampMillisLogicalType,
  TimestampNanosLogicalType,
} from "../schemas/logical/temporal_logical_types.ts";
import { UuidLogicalType } from "../schemas/logical/uuid_logical_type.ts";
import { IntType } from "../schemas/int_type.ts";
import { LongType } from "../schemas/long_type.ts";
import { RecordType } from "../schemas/record_type.ts";
import { Type } from "../schemas/type.ts";

describe("createType", () => {
  describe("invalid schema error handling", () => {
    it("throws for invalid primitive type", () => {
      assertThrows(
        () => createType("invalid_type"),
        Error,
        "Undefined Avro type reference",
      );
    });

    it("throws for unsupported schema types", () => {
      // Test null
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => createType(null as any),
        Error,
        "Unsupported Avro schema",
      );

      // Test number
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => createType(42 as any),
        Error,
        "Unsupported Avro schema",
      );

      // Test boolean
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => createType(true as any),
        Error,
        "Unsupported Avro schema",
      );

      // Test undefined
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => createType(undefined as any),
        Error,
        "Unsupported Avro schema",
      );
    });

    describe("logical type support", () => {
      it("creates date logical type", () => {
        const type = createType({ type: "int", logicalType: "date" });
        assertInstanceOf(type, DateLogicalType);
      });

      it("creates time-millis logical type", () => {
        const type = createType({ type: "int", logicalType: "time-millis" });
        assertInstanceOf(type, TimeMillisLogicalType);
      });

      it("creates timestamp-millis logical type", () => {
        const type = createType({
          type: "long",
          logicalType: "timestamp-millis",
        });
        assertInstanceOf(type, TimestampMillisLogicalType);
      });

      it("creates decimal logical type from bytes", () => {
        const type = createType({
          type: "bytes",
          logicalType: "decimal",
          precision: 6,
          scale: 2,
        });
        assertInstanceOf(type, DecimalLogicalType);
      });

      it("falls back to bytes when decimal precision missing", () => {
        const type = createType({
          type: "bytes",
          logicalType: "decimal",
        });
        assertInstanceOf(type, BytesType);
      });

      it("creates uuid logical type for string", () => {
        const type = createType({ type: "string", logicalType: "uuid" });
        assertInstanceOf(type, UuidLogicalType);
      });

      it("creates uuid logical type for fixed", () => {
        const schema = {
          type: "fixed",
          name: "Id16",
          size: 16,
          logicalType: "uuid",
        } as const;
        const registry = new Map<string, Type>();
        const type = createType(schema, { registry });
        assertInstanceOf(type, UuidLogicalType);

        const registryType = createType({ type: "Id16" }, { registry });
        assertInstanceOf(registryType, UuidLogicalType);
      });

      it("creates duration logical type and registers it", () => {
        const schema = {
          type: "fixed",
          name: "DurationFixed",
          size: 12,
          logicalType: "duration",
        } as const;
        const registry = new Map<string, Type>();
        const type = createType(schema, { registry });
        assertInstanceOf(type, DurationLogicalType);

        const resolved = createType({ type: "DurationFixed" }, { registry });
        assertInstanceOf(resolved, DurationLogicalType);
      });

      it("creates local timestamp logical type", () => {
        const type = createType({
          type: "long",
          logicalType: "local-timestamp-millis",
        });
        assertInstanceOf(type, LocalTimestampMillisLogicalType);
      });

      it("creates time-micros logical type", () => {
        const type = createType({ type: "long", logicalType: "time-micros" });
        assertInstanceOf(type, TimeMicrosLogicalType);
      });

      it("creates timestamp-micros logical type", () => {
        const type = createType({
          type: "long",
          logicalType: "timestamp-micros",
        });
        assertInstanceOf(type, TimestampMicrosLogicalType);
      });

      it("creates timestamp-nanos logical type", () => {
        const type = createType({
          type: "long",
          logicalType: "timestamp-nanos",
        });
        assertInstanceOf(type, TimestampNanosLogicalType);
      });

      it("creates local-timestamp-micros logical type", () => {
        const type = createType({
          type: "long",
          logicalType: "local-timestamp-micros",
        });
        assertInstanceOf(type, LocalTimestampMicrosLogicalType);
      });

      it("creates local-timestamp-nanos logical type", () => {
        const type = createType({
          type: "long",
          logicalType: "local-timestamp-nanos",
        });
        assertInstanceOf(type, LocalTimestampNanosLogicalType);
      });

      it("ignores unknown logical type", () => {
        const type = createType({ type: "int", logicalType: "unknown-type" });
        assertInstanceOf(type, IntType);
      });

      it("ignores non-string logicalType", () => {
        const type = createType({ type: "int", logicalType: 123 });
        assertInstanceOf(type, IntType);
      });

      it("falls back to underlying when decimal precision is not a number", () => {
        const type = createType({
          type: "bytes",
          logicalType: "decimal",
          precision: "10",
        });
        assertInstanceOf(type, BytesType);
      });

      it("falls back to underlying when decimal scale is not a number", () => {
        const type = createType({
          type: "bytes",
          logicalType: "decimal",
          precision: 10,
          scale: "2",
        });
        assertInstanceOf(type, BytesType);
      });

      it("falls back to underlying when decimal precision is invalid", () => {
        const type = createType({
          type: "bytes",
          logicalType: "decimal",
          precision: 0,
        });
        assertInstanceOf(type, BytesType);
      });

      it("ignores date logical type for non-int underlying", () => {
        const type = createType({ type: "long", logicalType: "date" });
        assertInstanceOf(type, LongType);
      });

      it("ignores time-millis logical type for non-int underlying", () => {
        const type = createType({ type: "long", logicalType: "time-millis" });
        assertInstanceOf(type, LongType);
      });

      it("ignores time-micros logical type for non-long underlying", () => {
        const type = createType({ type: "int", logicalType: "time-micros" });
        assertInstanceOf(type, IntType);
      });

      it("ignores timestamp-millis logical type for non-long underlying", () => {
        const type = createType({
          type: "int",
          logicalType: "timestamp-millis",
        });
        assertInstanceOf(type, IntType);
      });

      it("ignores timestamp-micros logical type for non-long underlying", () => {
        const type = createType({
          type: "int",
          logicalType: "timestamp-micros",
        });
        assertInstanceOf(type, IntType);
      });

      it("ignores timestamp-nanos logical type for non-long underlying", () => {
        const type = createType({
          type: "int",
          logicalType: "timestamp-nanos",
        });
        assertInstanceOf(type, IntType);
      });

      it("ignores local-timestamp-millis logical type for non-long underlying", () => {
        const type = createType({
          type: "int",
          logicalType: "local-timestamp-millis",
        });
        assertInstanceOf(type, IntType);
      });

      it("ignores local-timestamp-micros logical type for non-long underlying", () => {
        const type = createType({
          type: "int",
          logicalType: "local-timestamp-micros",
        });
        assertInstanceOf(type, IntType);
      });

      it("ignores local-timestamp-nanos logical type for non-long underlying", () => {
        const type = createType({
          type: "int",
          logicalType: "local-timestamp-nanos",
        });
        assertInstanceOf(type, IntType);
      });

      it("ignores duration logical type for non-fixed underlying", () => {
        const type = createType({ type: "bytes", logicalType: "duration" });
        assertInstanceOf(type, BytesType);
      });

      it("ignores uuid logical type for non-string/non-fixed underlying", () => {
        const type = createType({ type: "int", logicalType: "uuid" });
        assertInstanceOf(type, IntType);
      });

      it("ignores decimal logical type for non-bytes/non-fixed underlying", () => {
        const type = createType({
          type: "int",
          logicalType: "decimal",
          precision: 10,
        });
        assertInstanceOf(type, IntType);
      });
    });

    it("throws for malformed record schema", () => {
      const schema = {
        type: "record",
        // Missing name
        fields: [{ name: "field", type: "string" }],
      };
      assertThrows(
        () => createType(schema),
        Error,
      );
    });

    it("throws for record with non-array fields", () => {
      const schema = {
        type: "record",
        name: "TestRecord",
        fields: "not an array",
      };
      assertThrows(
        () => createType(schema),
        Error,
        "Record schema requires a fields array",
      );
    });

    it("throws for malformed enum schema", () => {
      const schema = {
        type: "enum",
        name: "TestEnum",
        // Missing symbols
      };
      assertThrows(
        () => createType(schema),
        Error,
      );
    });

    it("throws for malformed fixed schema", () => {
      const schema = {
        type: "fixed",
        name: "TestFixed",
        // Missing size
      };
      assertThrows(
        () => createType(schema),
        Error,
      );
    });

    describe("record schema validation", () => {
      it("throws for record with duplicate field names", () => {
        /**
         * Tests that records with duplicate field names are rejected.
         * Schema contains: Record with two fields named "id".
         * Expected: Error should be thrown for duplicate field names.
         * Note: This validation may happen during field access rather than creation.
         */
        const schema = {
          type: "record",
          name: "TestRecord",
          fields: [
            { name: "id", type: "string" },
            { name: "id", type: "int" }, // Duplicate field name
            { name: "name", type: "string" },
          ],
        } as const;

        // Try creating the type (might succeed)
        const recordType = createType(schema);
        assertInstanceOf(recordType, RecordType);

        // Validation might happen when accessing fields
        assertThrows(
          () => recordType.getFields(),
          Error,
          "Duplicate record field name",
        );
      });

      it("throws for record with invalid field types", () => {
        /**
         * Tests that records with invalid field types are rejected.
         * Schema contains: Record with field having invalid type reference.
         * Expected: Error should be thrown for undefined type reference.
         * Note: This validation may happen during field access rather than creation.
         */
        const schema = {
          type: "record",
          name: "TestRecord",
          fields: [
            { name: "id", type: "string" },
            { name: "invalidField", type: "NonExistentType" }, // Invalid type
            { name: "name", type: "string" },
          ],
        } as const;

        // Try creating the type (might succeed)
        const recordType = createType(schema);
        assertInstanceOf(recordType, RecordType);

        // Validation might happen when accessing fields
        assertThrows(
          () => recordType.getFields(),
          Error,
          "Undefined Avro type reference",
        );
      });

      it("throws for record with invalid field definitions", () => {
        /**
         * Tests that records with invalid field definitions (null or non-object) are rejected.
         * Schema contains: Record with null field.
         * Expected: Error should be thrown when accessing fields.
         */
        const schema = {
          type: "record",
          name: "TestRecord",
          fields: [
            { name: "id", type: "string" },
            null, // Invalid field
          ],
        } as const;

        const recordType = createType(schema);
        assertInstanceOf(recordType, RecordType);

        assertThrows(
          () => recordType.getFields(),
          Error,
          "Invalid record field definition",
        );
      });

      it("throws for record with invalid field names", () => {
        /**
         * Tests that records with invalid field names are rejected.
         * Schema contains: Record with field having non-string or empty name.
         * Expected: Error should be thrown when accessing fields.
         */
        const schema = {
          type: "record",
          name: "TestRecord",
          fields: [
            { name: "valid", type: "string" },
            { name: "", type: "int" }, // Empty name
          ],
        } as const;

        const recordType = createType(schema);
        assertInstanceOf(recordType, RecordType);

        assertThrows(
          () => recordType.getFields(),
          Error,
          "Record field requires a non-empty name",
        );
      });

      it("throws for record with fields missing type", () => {
        /**
         * Tests that records with fields missing type are rejected.
         * Schema contains: Record with field missing type property.
         * Expected: Error should be thrown when accessing fields.
         */
        const schema = {
          type: "record",
          name: "TestRecord",
          fields: [
            { name: "valid", type: "string" },
            { name: "invalid" }, // Missing type
          ],
        } as const;

        const recordType = createType(schema);
        assertInstanceOf(recordType, RecordType);

        assertThrows(
          () => recordType.getFields(),
          Error,
          'Record field "invalid" is missing a type definition',
        );
      });

      it("throws for record with invalid namespace format", () => {
        /**
         * Tests that records with invalid namespace formats are rejected.
         * Schema contains: Record with namespace containing invalid characters.
         * Expected: Error should be thrown for invalid namespace.
         */
        const schema = {
          type: "record",
          name: "TestRecord",
          namespace: "invalid.namespace.with spaces", // Invalid namespace with spaces
          fields: [
            { name: "id", type: "string" },
          ],
        } as const;

        // Note: This might not throw an error depending on implementation
        // If it doesn't throw, we might need to adjust the test or implementation
        assertThrows(
          () => createType(schema),
          Error,
        );
      });
    });

    describe("enum schema validation", () => {
      it("throws for enum with empty symbols array", () => {
        /**
         * Tests that enums with empty symbols arrays are rejected.
         * Schema contains: Enum with no symbols defined.
         * Expected: Error should be thrown for empty symbols.
         */
        const schema = {
          type: "enum",
          name: "EmptyEnum",
          symbols: [], // Empty symbols array
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });

      it("throws for enum with duplicate symbols", () => {
        /**
         * Tests that enums with duplicate symbols are rejected.
         * Schema contains: Enum with repeated symbol values.
         * Expected: Error should be thrown for duplicate symbols.
         */
        const schema = {
          type: "enum",
          name: "DuplicateEnum",
          symbols: ["A", "B", "A", "C"], // Duplicate "A"
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });

      it("throws for enum with non-string symbols", () => {
        /**
         * Tests that enums with non-string symbols are rejected.
         * Schema contains: Enum with numeric symbols.
         * Expected: Error should be thrown for invalid symbol types.
         */
        const schema = {
          type: "enum",
          name: "InvalidSymbolsEnum",
          symbols: ["A", 123, "C"], // Mixed types including number
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });
    });

    describe("fixed schema validation", () => {
      it("throws for fixed with invalid size", () => {
        /**
         * Tests that fixed types with invalid sizes are rejected.
         * Schema contains: Fixed type with negative size.
         * Expected: Error should be thrown for invalid size values.
         */
        const schema = {
          type: "fixed",
          name: "InvalidSizeFixed",
          size: -1, // Negative size
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });

      it("throws for fixed with zero size", () => {
        /**
         * Tests that fixed types with zero size are rejected.
         * Schema contains: Fixed type with size 0.
         * Expected: Error should be thrown for zero size.
         */
        const schema = {
          type: "fixed",
          name: "ZeroSizeFixed",
          size: 0, // Zero size
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });

      it("throws for fixed with non-integer size", () => {
        /**
         * Tests that fixed types with non-integer sizes are rejected.
         * Schema contains: Fixed type with decimal size.
         * Expected: Error should be thrown for non-integer size.
         */
        const schema = {
          type: "fixed",
          name: "NonIntegerSizeFixed",
          size: 16.5, // Non-integer size
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });
    });

    describe("union schema validation", () => {
      it("throws for union with empty types array", () => {
        /**
         * Tests that unions with empty types arrays are rejected.
         * Schema contains: Union with no types defined.
         * Expected: Error should be thrown for empty union.
         */
        // deno-lint-ignore no-explicit-any
        const schema = [] as any; // Empty union

        assertThrows(
          () => createType(schema),
          Error,
        );
      });

      it("throws for union with duplicate types", () => {
        /**
         * Tests that unions with duplicate types are rejected.
         * Schema contains: Union with repeated type values.
         * Expected: Error should be thrown for duplicate types.
         */
        // deno-lint-ignore no-explicit-any
        const schema = ["string", "int", "string"] as any; // Duplicate "string"

        assertThrows(
          () => createType(schema),
          Error,
        );
      });
    });

    describe("array schema validation", () => {
      it("throws for array with missing items", () => {
        /**
         * Tests that arrays with missing items are rejected.
         * Schema contains: Array type without items specification.
         * Expected: Error should be thrown for missing items.
         */
        const schema = {
          type: "array",
          // Missing items
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });

      it("throws for array with invalid item type", () => {
        /**
         * Tests that arrays with invalid item types are rejected.
         * Schema contains: Array with undefined type reference as items.
         * Expected: Error should be thrown for invalid item type.
         */
        const schema = {
          type: "array",
          items: "NonExistentType", // Invalid type
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });
    });

    describe("map schema validation", () => {
      it("throws for map with missing values", () => {
        /**
         * Tests that maps with missing values are rejected.
         * Schema contains: Map type without values specification.
         * Expected: Error should be thrown for missing values.
         */
        const schema = {
          type: "map",
          // Missing values
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });

      it("throws for map with invalid value type", () => {
        /**
         * Tests that maps with invalid value types are rejected.
         * Schema contains: Map with undefined type reference as values.
         * Expected: Error should be thrown for invalid value type.
         */
        const schema = {
          type: "map",
          values: "NonExistentType", // Invalid type
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });
    });

    describe("type name validation", () => {
      it("throws for record with empty name", () => {
        /**
         * Tests that records with empty names are rejected.
         * Schema contains: Record with empty string as name.
         * Expected: Error should be thrown for empty name.
         */
        const schema = {
          type: "record",
          name: "", // Empty name
          fields: [
            { name: "field", type: "string" },
          ],
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });

      it("throws for record with invalid name characters", () => {
        /**
         * Tests that records with invalid name characters are rejected.
         * Schema contains: Record with name containing spaces.
         * Expected: Error should be thrown for invalid characters.
         */
        const schema = {
          type: "record",
          name: "invalid name", // Invalid: contains space
          fields: [
            { name: "field", type: "string" },
          ],
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });

      it("throws for enum with empty name", () => {
        /**
         * Tests that enums with empty names are rejected.
         * Schema contains: Enum with empty string as name.
         * Expected: Error should be thrown for empty name.
         */
        const schema = {
          type: "enum",
          name: "", // Empty name
          symbols: ["A", "B", "C"],
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });

      it("throws for fixed with empty name", () => {
        /**
         * Tests that fixed types with empty names are rejected.
         * Schema contains: Fixed type with empty string as name.
         * Expected: Error should be thrown for empty name.
         */
        const schema = {
          type: "fixed",
          name: "", // Empty name
          size: 16,
        } as const;

        assertThrows(
          () => createType(schema),
          Error,
        );
      });
    });

    it("throws for schema missing valid type property", () => {
      // Test schema with no type field
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => createType({ name: "Test" } as any),
        Error,
        'Schema is missing a valid "type" property',
      );

      // Test schema with null type field
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => createType({ type: null } as any),
        Error,
        'Schema is missing a valid "type" property',
      );

      // Test schema with undefined type field
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => createType({ type: undefined } as any),
        Error,
        'Schema is missing a valid "type" property',
      );

      // Test schema with invalid type field (number)
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => createType({ type: 42 } as any),
        Error,
        'Schema is missing a valid "type" property',
      );
    });
  });

  it("throws when referencing undefined named types", () => {
    assertThrows(
      () => createType("UnknownType"),
      Error,
      "Undefined Avro type reference",
    );
  });

  it("round-trips recursive record values", async () => {
    const schema = {
      type: "record",
      name: "Node",
      fields: [
        { name: "value", type: "long" },
        { name: "next", type: ["null", "Node"] },
      ],
    } as const;

    const nodeType = createType(schema);
    assertInstanceOf(nodeType, RecordType);

    const sampleValue = {
      value: 1n,
      next: {
        Node: {
          value: 2n,
          next: null,
        },
      },
    };

    const buffer = await nodeType.toBuffer(sampleValue);
    const decoded = await nodeType.fromBuffer(buffer);

    assertEquals(decoded, sampleValue);
  });

  it("round-trips recursive array-based records", async () => {
    const schema = {
      type: "record",
      name: "Employee",
      fields: [
        { name: "name", type: "string" },
        {
          name: "reports",
          type: {
            type: "array",
            items: "Employee",
          },
        },
      ],
    } as const;

    const employeeType = createType(schema);
    assertInstanceOf(employeeType, RecordType);

    const sample = {
      name: "Alice",
      reports: [
        {
          name: "Bob",
          reports: [],
        },
        {
          name: "Claire",
          reports: [
            {
              name: "Dave",
              reports: [],
            },
          ],
        },
      ],
    };

    const encoded = await employeeType.toBuffer(sample);
    const decoded = await employeeType.fromBuffer(encoded);
    assertEquals(decoded, sample);
  });
});
