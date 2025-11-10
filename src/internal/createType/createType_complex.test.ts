import { assertEquals, assertInstanceOf } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { createType } from "./mod.ts";
import { ArrayType } from "../schemas/array_type.ts";
import { EnumType } from "../schemas/enum_type.ts";
import { FixedType } from "../schemas/fixed_type.ts";
import { MapType } from "../schemas/map_type.ts";
import { RecordType } from "../schemas/record_type.ts";
import { StringType } from "../schemas/string_type.ts";
import { Type } from "../schemas/type.ts";
import { UnionType } from "../schemas/union_type.ts";

describe("createType", () => {
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
});
