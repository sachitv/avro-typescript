import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { RecordField } from "../record_field.ts";
import { IntType } from "../../primitive/int_type.ts";
import { StringType } from "../../primitive/string_type.ts";
import { NullType } from "../../primitive/null_type.ts";
import { UnionType } from "../union_type.ts";
import type { Type } from "../../type.ts";
import { createRecord } from "./record_test_utils.ts";

describe("RecordField", () => {
  describe("constructor validation", () => {
    it("rejects invalid field names", () => {
      assertThrows(
        () =>
          new RecordField({
            name: 123 as unknown as string,
            type: new IntType(),
          }),
        Error,
        "Invalid record field name",
      );
      assertThrows(
        () =>
          new RecordField({
            name: "123invalid",
            type: new IntType(),
          }),
        Error,
        "Invalid record field name",
      );
      assertThrows(
        () =>
          new RecordField({
            name: "",
            type: new IntType(),
          }),
        Error,
        "Invalid record field name",
      );
    });

    it("rejects invalid field types", () => {
      assertThrows(
        () =>
          new RecordField({
            name: "id",
            type: "not a type" as unknown as Type,
          }),
        Error,
        "Invalid field type",
      );
    });

    it("rejects invalid field orders", () => {
      assertThrows(
        () =>
          new RecordField({
            name: "id",
            type: new IntType(),
            order: "invalid" as "ascending",
          }),
        Error,
        "Invalid record field order",
      );
    });

    it("rejects invalid field aliases", () => {
      assertThrows(
        () =>
          new RecordField({
            name: "id",
            type: new IntType(),
            aliases: [123 as unknown as string],
          }),
        Error,
        "Invalid record field alias",
      );
      assertThrows(
        () =>
          new RecordField({
            name: "id",
            type: new IntType(),
            aliases: ["123invalid"],
          }),
        Error,
        "Invalid record field alias",
      );
    });

    it("deduplicates aliases", () => {
      const field = new RecordField({
        name: "id",
        type: new IntType(),
        aliases: ["alias1", "alias1", "alias2"],
      });
      assertEquals(field.getAliases(), ["alias1", "alias2"]);
    });
  });

  describe("getName", () => {
    it("returns the field name", () => {
      const field = new RecordField({
        name: "myField",
        type: new IntType(),
      });
      assertEquals(field.getName(), "myField");
    });
  });

  describe("getType", () => {
    it("returns the field type", () => {
      const intType = new IntType();
      const field = new RecordField({
        name: "id",
        type: intType,
      });
      assertEquals(field.getType(), intType);
    });
  });

  describe("getAliases", () => {
    it("returns a copy of aliases", () => {
      const field = new RecordField({
        name: "id",
        type: new IntType(),
        aliases: ["alias1", "alias2"],
      });
      const aliases = field.getAliases();
      assertEquals(aliases, ["alias1", "alias2"]);
      // Verify it's a copy
      aliases.push("alias3");
      assertEquals(field.getAliases(), ["alias1", "alias2"]);
    });

    it("returns empty array when no aliases", () => {
      const field = new RecordField({
        name: "id",
        type: new IntType(),
      });
      assertEquals(field.getAliases(), []);
    });
  });

  describe("getOrder", () => {
    it("defaults to ascending", () => {
      const field = new RecordField({
        name: "id",
        type: new IntType(),
      });
      assertEquals(field.getOrder(), "ascending");
    });

    it("returns specified order", () => {
      const descField = new RecordField({
        name: "id",
        type: new IntType(),
        order: "descending",
      });
      assertEquals(descField.getOrder(), "descending");

      const ignoreField = new RecordField({
        name: "id",
        type: new IntType(),
        order: "ignore",
      });
      assertEquals(ignoreField.getOrder(), "ignore");
    });
  });

  describe("hasDefault", () => {
    it("returns false when no default", () => {
      const field = new RecordField({
        name: "id",
        type: new IntType(),
      });
      assertEquals(field.hasDefault(), false);
    });

    it("returns true when default is provided", () => {
      const field = new RecordField({
        name: "id",
        type: new IntType(),
        default: 42,
      });
      assertEquals(field.hasDefault(), true);
    });

    it("returns true even when default is undefined", () => {
      // For nullable unions, null is a valid default
      const field = new RecordField({
        name: "id",
        type: new UnionType({ types: [new NullType(), new IntType()] }),
        default: null,
      });
      // null is a valid default value for a nullable union
      assertEquals(field.hasDefault(), true);
      assertEquals(field.getDefault(), null);
    });
  });

  describe("getDefault", () => {
    it("returns the default value", () => {
      const field = new RecordField({
        name: "id",
        type: new IntType(),
        default: 42,
      });
      assertEquals(field.getDefault(), 42);
    });

    it("returns a cloned default value", () => {
      const field = new RecordField({
        name: "name",
        type: new StringType(),
        default: "hello",
      });
      const default1 = field.getDefault();
      const default2 = field.getDefault();
      assertEquals(default1, default2);
    });

    it("throws when field has no default", () => {
      const field = new RecordField({
        name: "id",
        type: new IntType(),
      });
      assertThrows(
        () => field.getDefault(),
        Error,
        "Field 'id' has no default",
      );
    });
  });

  describe("nameMatches", () => {
    it("matches field name", () => {
      const field = new RecordField({
        name: "id",
        type: new IntType(),
      });
      assertEquals(field.nameMatches("id"), true);
      assertEquals(field.nameMatches("other"), false);
    });

    it("matches field aliases", () => {
      const field = new RecordField({
        name: "id",
        type: new IntType(),
        aliases: ["identifier", "key"],
      });
      assertEquals(field.nameMatches("id"), true);
      assertEquals(field.nameMatches("identifier"), true);
      assertEquals(field.nameMatches("key"), true);
      assertEquals(field.nameMatches("other"), false);
    });
  });
});

describe("RecordType field access", () => {
  it("gets fields by name", () => {
    const type = createRecord({
      name: "example.Person",
      fields: [
        { name: "id", type: new IntType() },
        { name: "name", type: new StringType() },
      ],
    });

    const idField = type.getField("id");
    assertEquals(idField?.getName(), "id");

    const nameField = type.getField("name");
    assertEquals(nameField?.getName(), "name");

    const missingField = type.getField("age");
    assertEquals(missingField, undefined);
  });

  it("returns all fields", () => {
    const type = createRecord({
      name: "example.Person",
      fields: [
        { name: "id", type: new IntType() },
        { name: "name", type: new StringType() },
      ],
    });

    const fields = type.getFields();
    assertEquals(fields.length, 2);
    assertEquals(fields[0].getName(), "id");
    assertEquals(fields[1].getName(), "name");
  });

  it("rejects duplicate field names", () => {
    assertThrows(
      () =>
        createRecord({
          name: "example.Person",
          fields: [
            { name: "id", type: new IntType() },
            { name: "id", type: new StringType() },
          ],
        }),
      Error,
      "Duplicate record field name",
    );
  });
});
