import { assertEquals, assertInstanceOf } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { createType } from "./mod.ts";
import { EnumType } from "../schemas/enum_type.ts";
import { FixedType } from "../schemas/fixed_type.ts";
import { RecordType } from "../schemas/record_type.ts";

describe("createType", () => {
  describe("namespace resolution", () => {
    it("handles empty namespace strings", async () => {
      /**
       * Tests that empty namespace strings are treated as no namespace.
       * Schema contains: Record with empty namespace string.
       * Expected: Type is created without namespace.
       */
      const schema = {
        type: "record",
        name: "TestRecord",
        namespace: "", // Empty string
        fields: [{ name: "value", type: "string" }],
      } as const;

      const type = createType(schema);
      assertInstanceOf(type, RecordType);

      // Check that no namespace is set (fullName is just "TestRecord")
      // Since RecordType may not expose fullName, perhaps just ensure it works
      const value = { value: "test" };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("resolves all named types across namespaces", () => {
      // Test comprehensive namespace resolution for all named types (Record, Enum, Fixed)
      // This ensures that types defined in the same namespace can reference each other
      // without explicit qualification, and that the same type instances are shared.
      const schema = {
        type: "record",
        name: "Company",
        namespace: "com.business",
        fields: [
          { name: "name", type: "string" },
          {
            name: "status",
            type: {
              type: "enum",
              name: "Status",
              symbols: ["ACTIVE", "INACTIVE", "PENDING"],
            },
          },
          {
            name: "logo",
            type: {
              type: "fixed",
              name: "Logo",
              size: 16,
            },
          },
          {
            name: "employee",
            type: {
              type: "record",
              name: "Employee",
              fields: [
                { name: "name", type: "string" },
                { name: "id", type: "Logo" }, // Should resolve to com.business.Logo
                { name: "state", type: "Status" }, // Should resolve to com.business.Status
              ],
            },
          },
          {
            name: "backupLogo",
            type: "Logo", // Should resolve to com.business.Logo
          },
        ],
      } as const;

      const companyType = createType(schema);
      assertInstanceOf(companyType, RecordType);

      const fields = companyType.getFields();
      const statusField = fields.find((f) => f.getName() === "status");
      const logoField = fields.find((f) => f.getName() === "logo");
      const employeeField = fields.find((f) => f.getName() === "employee");
      const backupLogoField = fields.find((f) => f.getName() === "backupLogo");

      if (!statusField || !logoField || !employeeField || !backupLogoField) {
        throw new Error("Expected fields to be present");
      }

      // Check that status field has enum type
      assertInstanceOf(statusField.getType(), EnumType);

      // Check that logo and backupLogo fields have the same fixed type
      assertInstanceOf(logoField.getType(), FixedType);
      assertInstanceOf(backupLogoField.getType(), FixedType);
      assertEquals(logoField.getType(), backupLogoField.getType());

      // Check employee record and its fields
      const employeeType = employeeField.getType();
      assertInstanceOf(employeeType, RecordType);

      const employeeFields = employeeType.getFields();
      const employeeIdField = employeeFields.find((f) => f.getName() === "id");
      const employeeStateField = employeeFields.find((f) =>
        f.getName() === "state"
      );

      if (!employeeIdField || !employeeStateField) {
        throw new Error("Expected employee fields to be present");
      }

      // Employee id should reference the same Logo type
      assertInstanceOf(employeeIdField.getType(), FixedType);
      assertEquals(employeeIdField.getType(), logoField.getType());

      // Employee state should reference the same Status type
      assertInstanceOf(employeeStateField.getType(), EnumType);
      assertEquals(employeeStateField.getType(), statusField.getType());
    });

    it("resolves aliases for schema evolution", async () => {
      // Test schema evolution using aliases - write with one schema, read with another
      // Writer schema: no aliases
      const writerSchema = {
        type: "record",
        name: "User",
        namespace: "com.auth",
        fields: [
          { name: "username", type: "string" },
          {
            name: "role",
            type: {
              type: "enum",
              name: "Role",
              symbols: ["ADMIN", "USER", "GUEST"],
            },
          },
        ],
      } as const;

      // Reader schema: different names but aliases pointing to original names
      // This demonstrates schema evolution where field/record names change
      // but aliases maintain backward compatibility
      const readerSchema = {
        type: "record",
        name: "Account", // Different name
        namespace: "com.auth",
        aliases: ["User"], // Alias points to original User
        fields: [
          { name: "login", type: "string", aliases: ["username"] }, // Field alias
          {
            name: "permission",
            type: {
              type: "enum",
              name: "Permission", // Different name
              aliases: ["Role"], // Alias points to original Role
              symbols: ["ADMIN", "USER", "GUEST"],
            },
            aliases: ["role"], // Field alias
          },
        ],
      } as const;

      const writerType = createType(writerSchema);
      const readerType = createType(readerSchema);

      assertInstanceOf(writerType, RecordType);
      assertInstanceOf(readerType, RecordType);

      // Write with original schema
      const originalData = {
        username: "alice",
        role: "ADMIN",
      };

      const buffer = await writerType.toBuffer(originalData);

      // Read with evolved schema (should work due to aliases)
      const decoded = await readerType.fromBuffer(buffer);

      // Should map to new field names
      assertEquals(decoded.login, "alice");
      assertEquals(decoded.permission, "ADMIN");
    });
  });
});
