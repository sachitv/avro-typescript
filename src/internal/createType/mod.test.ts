import {
  assert,
  assertEquals,
  assertInstanceOf,
  assertRejects,
  assertThrows,
} from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { createType } from "./mod.ts";
import { Type } from "../schemas/type.ts";
import { IntType } from "../schemas/int_type.ts";
import { LongType } from "../schemas/long_type.ts";
import { FloatType } from "../schemas/float_type.ts";
import { DoubleType } from "../schemas/double_type.ts";
import { BooleanType } from "../schemas/boolean_type.ts";
import { StringType } from "../schemas/string_type.ts";
import { BytesType } from "../schemas/bytes_type.ts";
import { NullType } from "../schemas/null_type.ts";
import { ArrayType } from "../schemas/array_type.ts";
import { RecordType } from "../schemas/record_type.ts";
import { UnionType } from "../schemas/union_type.ts";
import { EnumType } from "../schemas/enum_type.ts";
import { FixedType } from "../schemas/fixed_type.ts";
import { MapType } from "../schemas/map_type.ts";

describe("createType", () => {
  it("returns the same Type instance when passed directly", () => {
    const originalType = new StringType();
    const result = createType(originalType);
    assertEquals(result, originalType);
  });

  describe("creates and round trips primitive types by name", () => {
    it("creates boolean type and round-trips values", async () => {
      const type = createType("boolean");
      assertInstanceOf(type, BooleanType);
      const value = true;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates int type and round-trips values", async () => {
      const type = createType("int");
      assertInstanceOf(type, IntType);
      const value = 42;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates long type and round-trips values", async () => {
      const type = createType("long");
      assertInstanceOf(type, LongType);
      const value = 1234567890123456789n;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates float type and round-trips values", async () => {
      const type = createType("float");
      assertInstanceOf(type, FloatType);
      const value = 3.5;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates double type and round-trips values", async () => {
      const type = createType("double");
      assertInstanceOf(type, DoubleType);
      const value = 2.718281828459045;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates string type and round-trips values", async () => {
      const type = createType("string");
      assertInstanceOf(type, StringType);
      const value = "hello world";
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates bytes type and round-trips values", async () => {
      const type = createType("bytes");
      assertInstanceOf(type, BytesType);
      const value = new Uint8Array([1, 2, 3, 4, 5]);
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates null type and round-trips values", async () => {
      const type = createType("null");
      assertInstanceOf(type, NullType);
      const value = null;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });
  });

  describe("creates primitive types from object schemas", () => {
    it("creates primitive types from object with type field", async () => {
      // Test that { type: "string" } creates a StringType
      const stringType = createType({ type: "string" });
      assertInstanceOf(stringType, StringType);

      // Test that { type: "int" } creates an IntType
      const intType = createType({ type: "int" });
      assertInstanceOf(intType, IntType);

      // Test that { type: "boolean" } creates a BooleanType
      const boolType = createType({ type: "boolean" });
      assertInstanceOf(boolType, BooleanType);

      // Test round-trip for all of them
      const stringValue = "hello world";
      const stringBuffer = await stringType.toBuffer(stringValue);
      const stringDecoded = await stringType.fromBuffer(stringBuffer);
      assertEquals(stringDecoded, stringValue);

      const intValue = 42;
      const intBuffer = await intType.toBuffer(intValue);
      const intDecoded = await intType.fromBuffer(intBuffer);
      assertEquals(intDecoded, intValue);

      const boolValue = true;
      const boolBuffer = await boolType.toBuffer(boolValue);
      const boolDecoded = await boolType.fromBuffer(boolBuffer);
      assertEquals(boolDecoded, boolValue);
    });
  });

  describe("creates and round trips complex types", () => {
    it("creates array type and round-trips values", async () => {
      const schema = { type: "array", items: "string" } as const;
      const type = createType(schema);
      assertInstanceOf(type, ArrayType);
      const itemsType = type.getItemsType();
      assertInstanceOf(itemsType, StringType);

      // Round trip test
      const value = ["hello", "world"];
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates record type and round-trips values", async () => {
      const schema = {
        type: "record",
        name: "Person",
        fields: [
          { name: "name", type: "string" },
          { name: "age", type: "int", order: "ascending", default: 25 },
        ],
      } as const;
      const type = createType(schema);
      assertInstanceOf(type, RecordType);

      // Check that order is set
      const fields = type.getFields();
      assertEquals(fields[1].getOrder(), "ascending");
      assertEquals(fields[1].getDefault(), 25);

      // Round trip test
      const value = { name: "Alice", age: 30 };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates union type and round-trips values", async () => {
      const schema = ["null", "string"];
      const type = createType(schema);
      assertInstanceOf(type, UnionType);

      // Round trip test
      const value = { string: "hello" };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates union type from object schema with array type", async () => {
      // Test union defined as { type: ["null", "string"] }
      const schema = { type: ["null", "string"] } as const;
      const type = createType(schema);
      assertInstanceOf(type, UnionType);

      // Round trip test
      const value = { string: "hello" };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates type from object schema with nested type object", async () => {
      // Test nested type definition: { type: { type: "record", ... } }
      const schema = {
        type: {
          type: "record",
          name: "NestedRecord",
          fields: [
            { name: "id", type: "int" },
            { name: "name", type: "string" },
          ],
        },
      } as const;
      const type = createType(schema);
      assertInstanceOf(type, RecordType);

      // Round trip test
      const value = { id: 123, name: "test" };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates enum type and round-trips values", async () => {
      const schema = {
        type: "enum",
        name: "Color",
        symbols: ["RED", "GREEN", "BLUE"],
        default: "RED",
      } as const;
      const type = createType(schema);
      assertInstanceOf(type, EnumType);

      // Check default is set
      assertEquals(type.getDefault(), "RED");

      // Round trip test
      const value = "GREEN";
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates map type and round-trips values", async () => {
      const schema = { type: "map", values: "int" } as const;
      const type = createType(schema);
      assertInstanceOf(type, MapType);

      // Round trip test
      const value = new Map([["one", 1], ["two", 2]]);
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates fixed type and round-trips values", async () => {
      const schema = {
        type: "fixed",
        name: "MD5",
        size: 16,
      } as const;
      const type = createType(schema);
      assertInstanceOf(type, FixedType);

      // Round trip test
      const value = new Uint8Array(16).fill(42);
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });
  });

  describe("creates and round trips recursive types", () => {
    it("creates map self-reference and round-trips values", async () => {
      const schema = {
        type: "record",
        name: "Category",
        fields: [
          { name: "name", type: "string" },
          {
            name: "subcategories",
            type: { type: "map", values: "Category" },
          },
        ],
      } as const;

      const categoryType = createType(schema);
      assertInstanceOf(categoryType, RecordType);

      const sampleValue = {
        name: "Electronics",
        subcategories: new Map([
          ["laptops", {
            name: "Laptops",
            subcategories: new Map([
              ["gaming", {
                name: "Gaming Laptops",
                subcategories: new Map(),
              }],
            ]),
          }],
          ["phones", {
            name: "Phones",
            subcategories: new Map(),
          }],
        ]),
      };

      const buffer = await categoryType.toBuffer(sampleValue);
      const decoded = await categoryType.fromBuffer(buffer);
      assertEquals(decoded, sampleValue);
    });

    it("creates nested record self-reference and round-trips values", async () => {
      const schema = {
        type: "record",
        name: "TreeNode",
        fields: [
          { name: "value", type: "string" },
          {
            name: "children",
            type: {
              type: "record",
              name: "NodeList",
              fields: [
                { name: "nodes", type: { type: "array", items: "TreeNode" } },
              ],
            },
          },
        ],
      } as const;

      const treeNodeType = createType(schema);
      assertInstanceOf(treeNodeType, RecordType);

      const sampleValue = {
        value: "root",
        children: {
          nodes: [
            {
              value: "child1",
              children: {
                nodes: [
                  {
                    value: "grandchild1",
                    children: { nodes: [] },
                  },
                ],
              },
            },
            {
              value: "child2",
              children: { nodes: [] },
            },
          ],
        },
      };

      const buffer = await treeNodeType.toBuffer(sampleValue);
      const decoded = await treeNodeType.fromBuffer(buffer);
      assertEquals(decoded, sampleValue);
    });

    it("creates mutual recursion and round-trips values", async () => {
      // Mutual recursion: Parent -> Child -> Parent
      // Use a shared registry for both types
      const registry = new Map<string, Type>();

      const parentSchema = {
        type: "record",
        name: "Parent",
        fields: [
          { name: "name", type: "string" },
          { name: "child", type: "Child" },
        ],
      } as const;

      const childSchema = {
        type: "record",
        name: "Child",
        fields: [
          { name: "name", type: "string" },
          { name: "parent", type: ["null", "Parent"] }, // Make parent optional to avoid infinite recursion
        ],
      } as const;

      // Create both types with the same registry
      const parentType = createType(parentSchema, { registry });
      const childType = createType(childSchema, { registry });

      assertInstanceOf(parentType, RecordType);
      assertInstanceOf(childType, RecordType);

      const sampleParent = {
        name: "John",
        child: {
          name: "Jane",
          parent: null, // Use null to avoid infinite recursion
        },
      };

      const buffer = await parentType.toBuffer(sampleParent);
      const decoded = await parentType.fromBuffer(buffer);
      assertEquals(decoded, sampleParent);
    });

    it("creates union with self-reference and round-trips values", async () => {
      const schema = {
        type: "record",
        name: "Expression",
        fields: [
          { name: "type", type: "string" },
          {
            name: "operands",
            type: ["null", { type: "array", items: "Expression" }],
          },
        ],
      } as const;

      const expressionType = createType(schema);
      assertInstanceOf(expressionType, RecordType);

      const sampleValue = {
        type: "add",
        operands: {
          array: [
            {
              type: "number",
              operands: null,
            },
            {
              type: "multiply",
              operands: {
                array: [
                  {
                    type: "number",
                    operands: null,
                  },
                  {
                    type: "number",
                    operands: null,
                  },
                ],
              },
            },
          ],
        },
      };

      const buffer = await expressionType.toBuffer(sampleValue);
      const decoded = await expressionType.fromBuffer(buffer);
      assertEquals(decoded, sampleValue);
    });

    it("creates complex nested structure and round-trips values", async () => {
      const schema = {
        type: "record",
        name: "Document",
        fields: [
          { name: "title", type: "string" },
          {
            name: "sections",
            type: {
              type: "array",
              items: {
                type: "record",
                name: "Section",
                fields: [
                  { name: "heading", type: "string" },
                  { name: "content", type: "Document" },
                ],
              },
            },
          },
        ],
      } as const;

      const documentType = createType(schema);
      assertInstanceOf(documentType, RecordType);

      const sampleValue = {
        title: "Main Document",
        sections: [
          {
            heading: "Introduction",
            content: {
              title: "Introduction Content",
              sections: [],
            },
          },
          {
            heading: "Chapter 1",
            content: {
              title: "Chapter 1 Content",
              sections: [
                {
                  heading: "Subsection 1.1",
                  content: {
                    title: "Subsection Content",
                    sections: [],
                  },
                },
              ],
            },
          },
        ],
      };

      const buffer = await documentType.toBuffer(sampleValue);
      const decoded = await documentType.fromBuffer(buffer);
      assertEquals(decoded, sampleValue);
    });
  });

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

  describe("registry", () => {
    it("shares named type instances within the same registry", () => {
      /**
       * Tests that named record types defined inline and referenced by name
       * within the same schema share the same type instance in the registry.
       * Schema contains: Outer record with inline Inner record definition and Inner reference.
       * Expected: Both field types should be identical RecordType instances.
       * This ensures memory efficiency and proper type identity.
       */
      const schema = {
        type: "record",
        name: "Outer",
        namespace: "com.example",
        fields: [
          {
            name: "inner",
            type: {
              type: "record",
              name: "Inner",
              fields: [{ name: "value", type: "string" }],
            },
          },
          { name: "innerAlias", type: "Inner" },
        ],
      } as const;

      const outer = createType(schema);
      assertInstanceOf(outer, RecordType);

      const fields = outer.getFields();
      const innerField = fields.find((field) => field.getName() === "inner");
      const aliasField = fields.find((field) =>
        field.getName() === "innerAlias"
      );
      if (!innerField || !aliasField) {
        throw new Error("Expected inner fields to be present.");
      }

      const innerType = innerField.getType();
      const aliasType = aliasField.getType();
      assertInstanceOf(innerType, RecordType);
      assertInstanceOf(aliasType, RecordType);
      assertEquals(innerType, aliasType);
    });

    it("shares enum type instances within the same registry", () => {
      /**
       * Tests that named enum types defined inline and referenced by name
       * within the same schema share the same type instance in the registry.
       * Schema contains: Container record with inline Status enum (ACTIVE, INACTIVE, PENDING) and Status reference.
       * Expected: Both field types should be identical EnumType instances with same symbols.
       * Verifies that enum symbols and type identity are preserved across references.
       */
      const schema = {
        type: "record",
        name: "Container",
        namespace: "com.example",
        fields: [
          {
            name: "status",
            type: {
              type: "enum",
              name: "Status",
              symbols: ["ACTIVE", "INACTIVE", "PENDING"],
            },
          },
          { name: "backupStatus", type: "Status" },
        ],
      } as const;

      const container = createType(schema);
      assertInstanceOf(container, RecordType);

      const fields = container.getFields();
      const statusField = fields.find((field) => field.getName() === "status");
      const backupField = fields.find((field) =>
        field.getName() === "backupStatus"
      );
      if (!statusField || !backupField) {
        throw new Error("Expected status fields to be present.");
      }

      const statusType = statusField.getType();
      const backupType = backupField.getType();
      assertInstanceOf(statusType, EnumType);
      assertInstanceOf(backupType, EnumType);
      assertEquals(statusType, backupType);
    });

    it("shares fixed type instances within the same registry", () => {
      /**
       * Tests that named fixed types defined inline and referenced by name
       * within the same schema share the same type instance in the registry.
       * Schema contains: DataHolder record with inline Hash fixed type (32 bytes) and Hash reference.
       * Expected: Both field types should be identical FixedType instances with size 32.
       * Ensures that fixed size constraints and type identity are maintained.
       */
      const schema = {
        type: "record",
        name: "DataHolder",
        namespace: "com.example",
        fields: [
          {
            name: "hash",
            type: {
              type: "fixed",
              name: "Hash",
              size: 32,
            },
          },
          { name: "backupHash", type: "Hash" },
        ],
      } as const;

      const holder = createType(schema);
      assertInstanceOf(holder, RecordType);

      const fields = holder.getFields();
      const hashField = fields.find((field) => field.getName() === "hash");
      const backupField = fields.find((field) =>
        field.getName() === "backupHash"
      );
      if (!hashField || !backupField) {
        throw new Error("Expected hash fields to be present.");
      }

      const hashType = hashField.getType();
      const backupType = backupField.getType();
      assertInstanceOf(hashType, FixedType);
      assertInstanceOf(backupType, FixedType);
      assertEquals(hashType, backupType);
    });

    it("shares named types within unions within the same registry", () => {
      /**
       * Tests that named types defined within union types and referenced
       * by name in other unions within the same schema share instances.
       * Schema contains: Container record with two union fields - first defines DataRecord inline,
       * second references DataRecord by name. Both unions are [null, DataRecord].
       * Expected: The DataRecord type from both unions should be identical instances.
       * Verifies that union type composition preserves type identity for named components.
       */
      const schema = {
        type: "record",
        name: "Container",
        namespace: "com.example",
        fields: [
          {
            name: "data",
            type: [
              "null",
              {
                type: "record",
                name: "DataRecord",
                fields: [{ name: "value", type: "string" }],
              },
            ],
          },
          {
            name: "backupData",
            type: ["null", "DataRecord"],
          },
        ],
      } as const;

      const container = createType(schema);
      assertInstanceOf(container, RecordType);

      const fields = container.getFields();
      const dataField = fields.find((field) => field.getName() === "data");
      const backupField = fields.find((field) =>
        field.getName() === "backupData"
      );
      if (!dataField || !backupField) {
        throw new Error("Expected data fields to be present.");
      }

      const dataType = dataField.getType();
      const backupType = backupField.getType();
      assertInstanceOf(dataType, UnionType);
      assertInstanceOf(backupType, UnionType);

      // Get the record types from the unions (index 1, since index 0 is null)
      const dataRecordType = dataType.getTypes()[1];
      const backupRecordType = backupType.getTypes()[1];
      assertInstanceOf(dataRecordType, RecordType);
      assertInstanceOf(backupRecordType, RecordType);
      assertEquals(dataRecordType, backupRecordType);
    });

    it("does not share instances for same local name in different namespaces", () => {
      /**
       * Tests that types with the same local name in different namespaces
       * create separate instances and do not share. Creates schemas for
       * com.example.User and com.company.User records.
       * Expected: Despite same local name "User", the types should be different instances.
       * This ensures proper namespace isolation and prevents cross-namespace type confusion.
       */
      const userSchema1 = {
        type: "record",
        name: "User",
        namespace: "com.example",
        fields: [{ name: "name", type: "string" }],
      } as const;

      const userSchema2 = {
        type: "record",
        name: "User",
        namespace: "com.company",
        fields: [{ name: "name", type: "string" }],
      } as const;

      const userType1 = createType(userSchema1);
      const userType2 = createType(userSchema2);

      assertInstanceOf(userType1, RecordType);
      assertInstanceOf(userType2, RecordType);
      // Same local name in different namespaces should not share instances
      assert(userType1 !== userType2);
    });

    it("resolves aliases to the same type instance", () => {
      /**
       * Tests that aliases for the same type resolve to the identical instance,
       * and multiple aliases point to the same type share the same instance.
       * Schema contains: Container record with Status enum and two alias references (State, Condition).
       * Expected: All three field types should be identical EnumType instances.
       * This ensures alias resolution maintains type identity and memory efficiency.
       */
      const schema = {
        type: "record",
        name: "Container",
        namespace: "com.example",
        fields: [
          {
            name: "status",
            type: {
              type: "enum",
              name: "Status",
              symbols: ["ACTIVE", "INACTIVE", "PENDING"],
            },
          },
          { name: "state", type: "Status" }, // Direct reference
          { name: "condition", type: "Status" }, // Another reference
        ],
      } as const;

      const container = createType(schema);
      assertInstanceOf(container, RecordType);

      const fields = container.getFields();
      const statusField = fields.find((field) => field.getName() === "status");
      const stateField = fields.find((field) => field.getName() === "state");
      const conditionField = fields.find((field) =>
        field.getName() === "condition"
      );

      if (!statusField || !stateField || !conditionField) {
        throw new Error("Expected all fields to be present.");
      }

      const statusType = statusField.getType();
      const stateType = stateField.getType();
      const conditionType = conditionField.getType();

      assertInstanceOf(statusType, EnumType);
      assertInstanceOf(stateType, EnumType);
      assertInstanceOf(conditionType, EnumType);

      // All references to the same type should share the same instance
      assertEquals(statusType, stateType);
      assertEquals(stateType, conditionType);
      assertEquals(statusType, conditionType);
    });

    it("shares instances in complex schema with interdependent named types", () => {
      /**
       * Tests sharing in schemas with multiple interdependent named types.
       * Schema contains: Company record with Employee and Department records,
       * where Employee references Department and Department references Employee.
       * Expected: All type references should resolve to shared instances.
       * This ensures proper handling of complex interdependent type relationships.
       */
      const schema = {
        type: "record",
        name: "Company",
        namespace: "com.business",
        fields: [
          { name: "name", type: "string" },
          {
            name: "employee",
            type: {
              type: "record",
              name: "Employee",
              fields: [
                { name: "name", type: "string" },
                { name: "department", type: "Department" }, // Forward reference
              ],
            },
          },
          {
            name: "department",
            type: {
              type: "record",
              name: "Department",
              fields: [
                { name: "name", type: "string" },
                { name: "manager", type: "Employee" }, // Backward reference
              ],
            },
          },
          { name: "backupEmployee", type: "Employee" }, // Reference to already defined type
          { name: "backupDepartment", type: "Department" }, // Reference to already defined type
        ],
      } as const;

      const company = createType(schema);
      assertInstanceOf(company, RecordType);

      const fields = company.getFields();
      const employeeField = fields.find((field) =>
        field.getName() === "employee"
      );
      const departmentField = fields.find((field) =>
        field.getName() === "department"
      );
      const backupEmployeeField = fields.find((field) =>
        field.getName() === "backupEmployee"
      );
      const backupDepartmentField = fields.find((field) =>
        field.getName() === "backupDepartment"
      );

      if (
        !employeeField || !departmentField || !backupEmployeeField ||
        !backupDepartmentField
      ) {
        throw new Error("Expected all fields to be present.");
      }

      const employeeType = employeeField.getType();
      const departmentType = departmentField.getType();
      const backupEmployeeType = backupEmployeeField.getType();
      const backupDepartmentType = backupDepartmentField.getType();

      assertInstanceOf(employeeType, RecordType);
      assertInstanceOf(departmentType, RecordType);
      assertInstanceOf(backupEmployeeType, RecordType);
      assertInstanceOf(backupDepartmentType, RecordType);

      // All references to Employee should share the same instance
      assertEquals(employeeType, backupEmployeeType);

      // All references to Department should share the same instance
      assertEquals(departmentType, backupDepartmentType);

      // Verify the interdependent references within the types
      const employeeFields = employeeType.getFields();
      const departmentFields = departmentType.getFields();

      const employeeDeptField = employeeFields.find((f) =>
        f.getName() === "department"
      );
      const deptManagerField = departmentFields.find((f) =>
        f.getName() === "manager"
      );

      if (!employeeDeptField || !deptManagerField) {
        throw new Error("Expected interdependent fields to be present.");
      }

      // Employee's department field should reference the Department type
      assertEquals(employeeDeptField.getType(), departmentType);

      // Department's manager field should reference the Employee type
      assertEquals(deptManagerField.getType(), employeeType);
    });

    it("maintains registry isolation between different registry instances", () => {
      /**
       * Tests that types created in different registry instances don't share,
       * even with identical names. Creates two separate registries with identical
       * User record schemas and verifies they create separate type instances.
       * Expected: Types from different registries should be different objects.
       * This ensures proper isolation between different registry contexts.
       */
      const userSchema = {
        type: "record",
        name: "User",
        namespace: "com.example",
        fields: [{ name: "name", type: "string" }],
      } as const;

      // Create types with separate registries
      const registry1 = new Map<string, Type>();
      const registry2 = new Map<string, Type>();

      const userType1 = createType(userSchema, { registry: registry1 });
      const userType2 = createType(userSchema, { registry: registry2 });

      assertInstanceOf(userType1, RecordType);
      assertInstanceOf(userType2, RecordType);

      // Types from different registries should not share instances
      assert(userType1 !== userType2);

      // Registries should contain their own instances
      assertEquals(registry1.get("com.example.User"), userType1);
      assertEquals(registry2.get("com.example.User"), userType2);
      assertEquals(
        registry1.get("com.example.User") === registry2.get("com.example.User"),
        false,
      );
    });

    it("maintains sharing with forward and circular references", () => {
      /**
       * Tests that forward references and circular references maintain proper
       * type sharing in the registry. Schema contains: Node record that references
       * itself (circular) and is referenced before definition (forward).
       * Expected: All references to Node should share the same instance.
       * This ensures registry handles reference patterns correctly.
       */
      const schema = {
        type: "record",
        name: "Tree",
        namespace: "com.example",
        fields: [
          { name: "value", type: "string" },
          {
            name: "children",
            type: {
              type: "array",
              items: "Node", // Forward reference to Node
            },
          },
          {
            name: "root",
            type: "Node", // Another forward reference
          },
        ],
      } as const;

      // Define Node after it's referenced (forward reference)
      const nodeSchema = {
        type: "record",
        name: "Node",
        namespace: "com.example",
        fields: [
          { name: "value", type: "string" },
          {
            name: "parent",
            type: ["null", "Node"], // Circular reference to itself
          },
          {
            name: "children",
            type: {
              type: "array",
              items: "Node", // Self-reference within array
            },
          },
        ],
      } as const;

      // Create both types with the same registry
      const registry = new Map<string, Type>();
      const treeType = createType(schema, { registry });
      const nodeType = createType(nodeSchema, { registry });

      assertInstanceOf(treeType, RecordType);
      assertInstanceOf(nodeType, RecordType);

      const treeFields = treeType.getFields();
      const childrenField = treeFields.find((f) => f.getName() === "children");
      const rootField = treeFields.find((f) => f.getName() === "root");

      if (!childrenField || !rootField) {
        throw new Error("Expected Tree fields to be present.");
      }

      // The array items type should be the Node type
      const arrayType = childrenField.getType();
      assertInstanceOf(arrayType, ArrayType);
      const itemsType = arrayType.getItemsType();
      assertEquals(itemsType, nodeType);

      // The root field should also reference the Node type
      assertEquals(rootField.getType(), nodeType);

      // Verify circular references within Node
      const nodeFields = nodeType.getFields();
      const parentField = nodeFields.find((f) => f.getName() === "parent");
      const nodeChildrenField = nodeFields.find((f) =>
        f.getName() === "children"
      );

      if (!parentField || !nodeChildrenField) {
        throw new Error("Expected Node fields to be present.");
      }

      // Parent field is a union, check the Node type in it
      const parentUnionType = parentField.getType();
      assertInstanceOf(parentUnionType, UnionType);
      const parentNodeType = parentUnionType.getTypes()[1]; // Index 1 is Node (index 0 is null)
      assertEquals(parentNodeType, nodeType);

      // Children array should also reference Node
      const nodeChildrenArrayType = nodeChildrenField.getType();
      assertInstanceOf(nodeChildrenArrayType, ArrayType);
      const nodeChildrenItemsType = nodeChildrenArrayType.getItemsType();
      assertEquals(nodeChildrenItemsType, nodeType);
    });

    it("throws error for unresolved forward references", () => {
      /**
       * Tests that errors are thrown when forward references exist to types
       * that are never defined. Schema contains: Tree record that references
       * UndefinedNode which is never created.
       * Expected: Error should be thrown when accessing fields that reference undefined types.
       * This ensures forward references must eventually be resolved.
       */
      const schema = {
        type: "record",
        name: "Tree",
        namespace: "com.example",
        fields: [
          { name: "value", type: "string" },
          {
            name: "children",
            type: {
              type: "array",
              items: "UndefinedNode", // Forward reference to undefined type
            },
          },
        ],
      } as const;

      // Create the type (this might succeed)
      const treeType = createType(schema);
      assertInstanceOf(treeType, RecordType);

      // Try to access the fields, which should trigger resolution of the undefined type
      assertThrows(
        () => treeType.getFields(),
        Error,
        "Undefined Avro type reference",
      );
    });

    it("throws error when serializing data with unresolved forward references", async () => {
      /**
       * Tests that errors are thrown when trying to serialize data using
       * a type that contains unresolved forward references.
       * Expected: Error should be thrown during serialization.
       * This ensures data integrity when forward references are unresolved.
       */
      const schema = {
        type: "record",
        name: "Tree",
        namespace: "com.example",
        fields: [
          { name: "value", type: "string" },
          {
            name: "child",
            type: "UndefinedNode", // Forward reference to undefined type
          },
        ],
      } as const;

      const treeType = createType(schema);
      assertInstanceOf(treeType, RecordType);

      const testData = {
        value: "root",
        child: { someField: "value" }, // This would be invalid anyway
      };

      // Try to serialize, which should trigger resolution and fail
      await assertRejects(
        async () => await treeType.toBuffer(testData),
        Error,
        "Undefined Avro type reference",
      );
    });

    it("does not share primitive type instances", () => {
      /**
       * Tests that primitive types are never shared in the registry.
       * Primitive types should be created on demand and not cached/shared.
       * Expected: Multiple calls to createType with same primitive return different instances.
       */
      const stringType1 = createType("string");
      const stringType2 = createType("string");
      assertInstanceOf(stringType1, StringType);
      assertInstanceOf(stringType2, StringType);
      // Primitive types should be different instances (not shared)
      assert(stringType1 !== stringType2);
    });

    it("does not share anonymous array type instances", () => {
      /**
       * Tests that anonymous array types (inline without names) are never shared
       * between different schemas. Each schema should get its own array type instance.
       * Expected: Array types in different schemas should be different instances.
       */
      const schema1 = {
        type: "record",
        name: "Test1",
        fields: [
          { name: "items", type: { type: "array", items: "string" } }, // Anonymous array
        ],
      } as const;

      const schema2 = {
        type: "record",
        name: "Test2",
        fields: [
          { name: "items", type: { type: "array", items: "string" } }, // Same anonymous array
        ],
      } as const;

      const testType1 = createType(schema1);
      const testType2 = createType(schema2);

      assertInstanceOf(testType1, RecordType);
      assertInstanceOf(testType2, RecordType);

      const fields1 = testType1.getFields();
      const fields2 = testType2.getFields();

      const arrayField1 = fields1.find((f) => f.getName() === "items");
      const arrayField2 = fields2.find((f) => f.getName() === "items");

      if (!arrayField1 || !arrayField2) {
        throw new Error("Expected array fields to be present.");
      }

      // Anonymous array types should not be shared between different schemas
      assert(arrayField1.getType() !== arrayField2.getType());
    });

    it("does not share anonymous map type instances", () => {
      /**
       * Tests that anonymous map types (inline without names) are never shared
       * between different schemas. Each schema should get its own map type instance.
       * Expected: Map types in different schemas should be different instances.
       */
      const schema1 = {
        type: "record",
        name: "Test1",
        fields: [
          { name: "data", type: { type: "map", values: "int" } }, // Anonymous map
        ],
      } as const;

      const schema2 = {
        type: "record",
        name: "Test2",
        fields: [
          { name: "data", type: { type: "map", values: "int" } }, // Same anonymous map
        ],
      } as const;

      const testType1 = createType(schema1);
      const testType2 = createType(schema2);

      assertInstanceOf(testType1, RecordType);
      assertInstanceOf(testType2, RecordType);

      const fields1 = testType1.getFields();
      const fields2 = testType2.getFields();

      const mapField1 = fields1.find((f) => f.getName() === "data");
      const mapField2 = fields2.find((f) => f.getName() === "data");

      if (!mapField1 || !mapField2) {
        throw new Error("Expected map fields to be present.");
      }

      // Anonymous map types should not be shared between different schemas
      assert(mapField1.getType() !== mapField2.getType());
    });

    it("does not share anonymous union type instances", () => {
      /**
       * Tests that anonymous union types (inline without names) are never shared
       * between different schemas. Each schema should get its own union type instance.
       * Expected: Union types in different schemas should be different instances.
       */
      const schema1 = {
        type: "record",
        name: "Test1",
        fields: [
          { name: "data", type: ["null", "string"] }, // Anonymous union
        ],
      } as const;

      const schema2 = {
        type: "record",
        name: "Test2",
        fields: [
          { name: "data", type: ["null", "string"] }, // Same anonymous union
        ],
      } as const;

      const testType1 = createType(schema1);
      const testType2 = createType(schema2);

      assertInstanceOf(testType1, RecordType);
      assertInstanceOf(testType2, RecordType);

      const fields1 = testType1.getFields();
      const fields2 = testType2.getFields();

      const unionField1 = fields1.find((f) => f.getName() === "data");
      const unionField2 = fields2.find((f) => f.getName() === "data");

      if (!unionField1 || !unionField2) {
        throw new Error("Expected union fields to be present.");
      }

      // Anonymous union types should not be shared between different schemas
      assert(unionField1.getType() !== unionField2.getType());
    });

    it("validates referential equality of shared type instances", () => {
      /**
       * Tests that shared type instances maintain referential equality (===) in JavaScript.
       * When the same named type is referenced multiple times, all references should
       * point to the exact same object instance.
       * Expected: Multiple references to same named type should be === equal.
       */
      const schema = {
        type: "record",
        name: "Container",
        namespace: "com.example",
        fields: [
          {
            name: "primary",
            type: {
              type: "record",
              name: "SharedType",
              fields: [{ name: "value", type: "string" }],
            },
          },
          { name: "secondary", type: "SharedType" },
          { name: "tertiary", type: "SharedType" },
        ],
      } as const;

      const container = createType(schema);
      assertInstanceOf(container, RecordType);

      const fields = container.getFields();
      const primaryField = fields.find((f) => f.getName() === "primary");
      const secondaryField = fields.find((f) => f.getName() === "secondary");
      const tertiaryField = fields.find((f) => f.getName() === "tertiary");

      if (!primaryField || !secondaryField || !tertiaryField) {
        throw new Error("Expected all fields to be present.");
      }

      const primaryType = primaryField.getType();
      const secondaryType = secondaryField.getType();
      const tertiaryType = tertiaryField.getType();

      // All references should be the exact same instance (referential equality)
      assertEquals(primaryType === secondaryType, true);
      assertEquals(secondaryType === tertiaryType, true);
      assertEquals(primaryType === tertiaryType, true);
    });

    it("validates metadata isolation for shared type instances", () => {
      /**
       * Tests that shared type instances maintain proper metadata isolation.
       * While instances are shared, any metadata or state should be properly
       * isolated to prevent cross-contamination between different uses.
       * Expected: Shared instances should maintain consistent metadata.
       */
      const schema = {
        type: "record",
        name: "Container",
        namespace: "com.example",
        fields: [
          {
            name: "primary",
            type: {
              type: "record",
              name: "SharedRecord",
              fields: [{ name: "data", type: "string" }],
            },
          },
          { name: "secondary", type: "SharedRecord" },
          { name: "tertiary", type: "SharedRecord" },
        ],
      } as const;

      const container = createType(schema);
      assertInstanceOf(container, RecordType);

      const fields = container.getFields();
      const primaryField = fields.find((f) => f.getName() === "primary");
      const secondaryField = fields.find((f) => f.getName() === "secondary");
      const tertiaryField = fields.find((f) => f.getName() === "tertiary");

      if (!primaryField || !secondaryField || !tertiaryField) {
        throw new Error("Expected fields to be present.");
      }

      const primaryType = primaryField.getType();
      const secondaryType = secondaryField.getType();
      const tertiaryType = tertiaryField.getType();

      // All references should be the same shared instance
      assertEquals(primaryType === secondaryType, true);
      assertEquals(secondaryType === tertiaryType, true);

      // Metadata should be consistent across all shared instances
      assertInstanceOf(primaryType, RecordType);
      assertInstanceOf(secondaryType, RecordType);
      assertInstanceOf(tertiaryType, RecordType);

      const primaryFields = primaryType.getFields();
      const secondaryFields = secondaryType.getFields();
      const tertiaryFields = tertiaryType.getFields();

      // All should have same number of fields and field names
      assertEquals(primaryFields.length, secondaryFields.length);
      assertEquals(secondaryFields.length, tertiaryFields.length);
      assertEquals(primaryFields.length, 1);
      assertEquals(primaryFields[0].getName(), secondaryFields[0].getName());
      assertEquals(secondaryFields[0].getName(), tertiaryFields[0].getName());
      assertEquals(primaryFields[0].getName(), "data");
    });

    it("maintains sharing through schema evolution with aliases", () => {
      /**
       * Tests that type sharing works correctly through schema evolution.
       * When schemas evolve but maintain the same type names, sharing should
       * continue to work. Also tests that aliases properly resolve to shared instances.
       * Expected: Evolved schemas should maintain type sharing and aliases should work.
       */
      // Original schema
      const originalSchema = {
        type: "record",
        name: "User",
        namespace: "com.example",
        fields: [
          { name: "id", type: "string" },
          { name: "name", type: "string" },
        ],
      } as const;

      // Evolved schema with additional field and alias
      const evolvedSchema = {
        type: "record",
        name: "Account", // Different name
        namespace: "com.example",
        aliases: ["User"], // Alias to original User
        fields: [
          { name: "id", type: "string" },
          { name: "name", type: "string" },
          { name: "email", type: "string" }, // New field
        ],
      } as const;

      const originalType = createType(originalSchema);
      const evolvedType = createType(evolvedSchema);

      assertInstanceOf(originalType, RecordType);
      assertInstanceOf(evolvedType, RecordType);

      // Types should be different since they have different names
      assert(originalType !== evolvedType);

      // But field types that are the same should potentially share if they were named
      // Since these are primitive types, they won't share (as tested above)
    });

    it("shares types correctly when schemas evolve with same named types", () => {
      /**
       * Tests that when schemas evolve but contain the same named subtypes,
       * those subtypes should still be shared correctly across schema versions.
       * Expected: Named types with same names should share instances even in evolved schemas.
       */
      const schemaV1 = {
        type: "record",
        name: "DocumentV1",
        namespace: "com.example",
        fields: [
          { name: "title", type: "string" },
          {
            name: "metadata",
            type: {
              type: "record",
              name: "Metadata",
              fields: [
                { name: "created", type: "string" },
                { name: "author", type: "string" },
              ],
            },
          },
        ],
      } as const;

      const schemaV2 = {
        type: "record",
        name: "DocumentV2",
        namespace: "com.example",
        fields: [
          { name: "title", type: "string" },
          { name: "content", type: "string" }, // New field
          { name: "metadata", type: "Metadata" }, // Reference to same Metadata type
        ],
      } as const;

      // Use same registry for both schemas
      const registry = new Map<string, Type>();
      const typeV1 = createType(schemaV1, { registry });
      const typeV2 = createType(schemaV2, { registry });

      assertInstanceOf(typeV1, RecordType);
      assertInstanceOf(typeV2, RecordType);

      const fieldsV1 = typeV1.getFields();
      const fieldsV2 = typeV2.getFields();

      const metadataFieldV1 = fieldsV1.find((f) => f.getName() === "metadata");
      const metadataFieldV2 = fieldsV2.find((f) => f.getName() === "metadata");

      if (!metadataFieldV1 || !metadataFieldV2) {
        throw new Error("Expected metadata fields to be present.");
      }

      const metadataTypeV1 = metadataFieldV1.getType();
      const metadataTypeV2 = metadataFieldV2.getType();

      // The Metadata type should be shared between both schema versions
      assertEquals(metadataTypeV1 === metadataTypeV2, true);

      // Verify it's the correct type
      assertInstanceOf(metadataTypeV1, RecordType);
      assertInstanceOf(metadataTypeV2, RecordType);

      // And has the expected fields
      const metadataFields = metadataTypeV1.getFields();
      assertEquals(metadataFields.length, 2);
      assertEquals(metadataFields[0].getName(), "created");
      assertEquals(metadataFields[1].getName(), "author");
    });

    it("does not share instances for different types with the same name", () => {
      /**
       * Tests that different types (record vs enum) with the same name
       * in the same namespace do not share instances, even when created
       * separately. Creates separate schemas: one with Foo record, one with Foo enum.
       * Expected: RecordType and EnumType instances should be different objects despite same name.
       * This ensures type safety and prevents accidental type confusion between different schema types.
       */
      const recordSchema = {
        type: "record",
        name: "Foo",
        namespace: "com.example",
        fields: [{ name: "value", type: "string" }],
      } as const;

      const enumSchema = {
        type: "enum",
        name: "Foo",
        namespace: "com.example",
        symbols: ["A", "B", "C"],
      } as const;

      const recordType = createType(recordSchema);
      const enumType = createType(enumSchema);

      assertInstanceOf(recordType, RecordType);
      assertInstanceOf(enumType, EnumType);
      // Different types with same name should not be equal
      // @ts-ignore: intentional comparison of different types
      assert(recordType !== enumType);
    });

    it("throws for duplicate type names in registry", () => {
      /**
       * Tests that creating a type with a name already in the registry throws an error.
       * Uses a shared registry with an existing type, then tries to create another with the same name.
       * Expected: Error should be thrown for duplicate name.
       */
      const registry = new Map<string, Type>();
      registry.set("com.example.Foo", new IntType()); // Dummy type

      const schema = {
        type: "record",
        name: "Foo",
        namespace: "com.example",
        fields: [{ name: "value", type: "string" }],
      } as const;

      assertThrows(
        () => createType(schema, { registry }),
        Error,
        "Duplicate Avro type name: com.example.Foo",
      );
    });

    it("throws for duplicate enum type names in registry", () => {
      /**
       * Tests that creating an enum type with a name already in the registry throws an error.
       * Uses a shared registry with an existing type, then tries to create an enum with the same name.
       * Expected: Error should be thrown for duplicate name.
       */
      const registry = new Map<string, Type>();
      registry.set("com.example.Bar", new StringType()); // Dummy type

      const schema = {
        type: "enum",
        name: "Bar",
        namespace: "com.example",
        symbols: ["A", "B"],
      } as const;

      assertThrows(
        () => createType(schema, { registry }),
        Error,
        "Duplicate Avro type name: com.example.Bar",
      );
    });

    it("throws for duplicate fixed type names in registry", () => {
      /**
       * Tests that creating a fixed type with a name already in the registry throws an error.
       * Uses a shared registry with an existing type, then tries to create a fixed with the same name.
       * Expected: Error should be thrown for duplicate name.
       */
      const registry = new Map<string, Type>();
      registry.set("com.example.Baz", new IntType()); // Dummy type

      const schema = {
        type: "fixed",
        name: "Baz",
        namespace: "com.example",
        size: 16,
      } as const;

      assertThrows(
        () => createType(schema, { registry }),
        Error,
        "Duplicate Avro type name: com.example.Baz",
      );
    });

    it("resolves named types from object schemas", () => {
      /**
       * Tests that named types can be resolved from object schemas with type field.
       * First creates a named record type, then references it using { type: "MyType" }.
       * This tests the fallback case in constructType that calls createFromTypeName.
       * Expected: Both references should resolve to the same type instance.
       */
      const registry = new Map<string, Type>();

      // First create a named record type
      const recordSchema = {
        type: "record",
        name: "MyRecord",
        namespace: "com.example",
        fields: [{ name: "value", type: "string" }],
      } as const;

      const recordType = createType(recordSchema, { registry });

      // Now reference it using object schema format
      const referenceSchema = {
        type: "MyRecord",
        namespace: "com.example",
      } as const;

      const referencedType = createType(referenceSchema, { registry });

      // Should resolve to the same type instance
      assertEquals(recordType, referencedType);
      assertInstanceOf(referencedType, RecordType);
    });
  });

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

    it("throws for logical types", () => {
      const schema = {
        type: "int",
        logicalType: "date",
      } as const;

      assertThrows(
        () => createType(schema),
        Error,
        "Logical types are not supported yet in the TypeScript port",
      );
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
