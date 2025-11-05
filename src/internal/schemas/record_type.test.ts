import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { Tap } from "../serialization/tap.ts";
import { IntType } from "./int_type.ts";
import { StringType } from "./string_type.ts";
import { BytesType } from "./bytes_type.ts";
import { RecordType } from "./record_type.ts";
import { resolveNames } from "./resolve_names.ts";
import { Type } from "./type.ts";
import { ValidationError } from "./error.ts";

interface FieldSpec {
  name: string;
  type: Type;
  aliases?: string[];
  order?: "ascending" | "descending" | "ignore";
  default?: unknown;
}

function createRecord(params: {
  name: string;
  namespace?: string;
  aliases?: string[];
  fields: FieldSpec[];
}): RecordType {
  const { fields, ...names } = params;
  const resolved = resolveNames(names);
  return new RecordType({
    ...resolved,
    fields,
  });
}

describe("RecordType", () => {
  describe("Constructor", () => {
    it("requires a fields array", () => {
      const names = resolveNames({ name: "example.Empty" });
      assertThrows(() =>
        new RecordType({
          ...names,
          fields: undefined as unknown as FieldSpec[],
        })
      );
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

    it("rejects invalid field names", () => {
      assertThrows(
        () =>
          createRecord({
            name: "example.Person",
            fields: [{ name: 123 as unknown as string, type: new IntType() }],
          }),
        Error,
        "Invalid record field name",
      );
      assertThrows(
        () =>
          createRecord({
            name: "example.Person",
            fields: [{ name: "123invalid", type: new IntType() }],
          }),
        Error,
        "Invalid record field name",
      );
    });

    it("rejects invalid field types", () => {
      assertThrows(
        () =>
          createRecord({
            name: "example.Person",
            fields: [{ name: "id", type: "not a type" as unknown as Type }],
          }),
        Error,
        "Invalid field type",
      );
    });

    it("rejects invalid field orders", () => {
      assertThrows(
        () =>
          createRecord({
            name: "example.Person",
            fields: [{
              name: "id",
              type: new IntType(),
              order: "invalid" as unknown as "ascending",
            }],
          }),
        Error,
        "Invalid record field order",
      );
    });

    it("rejects invalid field aliases", () => {
      assertThrows(
        () =>
          createRecord({
            name: "example.Person",
            fields: [{
              name: "id",
              type: new IntType(),
              aliases: ["valid", 123 as unknown as string],
            }],
          }),
        Error,
        "Invalid record field alias",
      );
      assertThrows(
        () =>
          createRecord({
            name: "example.Person",
            fields: [{
              name: "id",
              type: new IntType(),
              aliases: ["123invalid"],
            }],
          }),
        Error,
        "Invalid record field alias",
      );
    });
  });

  describe("check method", () => {
    it("validates objects and reports missing required fields", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      assert(type.check({ id: 1, name: "Ann" }));
      assert(type.check({ id: 2 }));
      assert(!type.check({ name: "Ann" }));

      // We want to verify that the error is reported in the errorHook.
      const errors: Array<{ path: string[]; value: unknown }> = [];
      const result = type.check({ name: "Ann" }, (path, value, schema) => {
        errors.push({ path, value });
        assertEquals(schema, type);
      });
      assert(!result); // should return false when validation fails, even with errorHook
      assertEquals(errors, [{ path: ["id"], value: undefined }]);
    });

    it("validates objects and reports invalid field values with errorHook", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      // Valid record
      assert(type.check({ id: 1, name: "Ann" }));

      // Invalid record without errorHook should return false
      assert(!type.check({ id: "not a number", name: "Ann" }));

      // Invalid record with errorHook should return true (error handled)
      const errors: Array<{ path: string[]; value: unknown; schema: Type }> =
        [];
      const result = type.check(
        { id: "not a number", name: "Ann" },
        (path, value, schema) => {
          errors.push({ path, value, schema });
        },
      );
      assert(result); // should return true when errorHook handles errors
      assertEquals(errors.length, 1);
      assertEquals(errors[0].path, ["id"]);
      assertEquals(errors[0].value, "not a number");
      assert(errors[0].schema instanceof IntType); // schema is the field's type
    });

    it("rejects non-record values in check", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [{ name: "id", type: new IntType() }],
      });

      assert(!type.check("string"));
      assert(!type.check(42));
      assert(!type.check(null));
      assert(!type.check([1, 2, 3]));
    });

    it("reports errors for non-record values in check", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [{ name: "id", type: new IntType() }],
      });

      const errors: Array<{ path: string[]; value: unknown }> = [];
      type.check("invalid", (path, value, schema) => {
        errors.push({ path, value });
        assertEquals(schema, type);
      });
      assertEquals(errors, [{ path: [], value: "invalid" }]);
    });

    it("reports errors for invalid nested values in check", () => {
      const nestedType = createRecord({
        name: "example.Address",
        fields: [{ name: "street", type: new StringType() }],
      });

      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "address", type: nestedType },
        ],
      });

      const errors: Array<{ path: string[]; value: unknown; schema: Type }> =
        [];
      type.check({ id: 1, address: { street: 123 } }, (path, value, schema) => {
        errors.push({ path, value, schema });
      });
      assertEquals(errors.length, 1);
      assertEquals(errors[0].path, ["address", "street"]);
      assertEquals(errors[0].value, 123);
      assert(errors[0].schema instanceof StringType);
    });
  });

  describe("Serialization", () => {
    it("serializes and deserializes records with defaults", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const buffer = type.toBuffer({ id: 5 });
      const tap = new Tap(buffer);
      const decoded = type.read(tap);
      assertEquals(decoded, { id: 5, name: "unknown" });

      const roundTrip = type.fromBuffer(buffer);
      assertEquals(roundTrip, { id: 5, name: "unknown" });
    });

    it("writes records with default values for missing fields", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const buffer = new ArrayBuffer(16);
      const tap = new Tap(buffer);
      type.write(tap, { id: 42 }); // name is missing but has default

      // Verify the written data can be read back correctly
      const readTap = new Tap(buffer);
      const decoded = type.read(readTap);
      assertEquals(decoded, { id: 42, name: "unknown" });
    });

    it("throws when missing required field during write for non-nested records", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const tap = new Tap(new ArrayBuffer(16));
      assertThrows(
        () =>
          type.write(
            tap,
            { name: "Ann" } as unknown as Record<string, unknown>,
          ),
        Error,
        `Invalid value: 'undefined' for type: 
{
  "type": "record",
  "name": "Person",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string",
      "default": "unknown"
    }
  ],
  "namespace": "example"
}
 at path: id`,
      );
    });

    it("throws when missing required field during write for nested records", () => {
      const childRecord = createRecord({
        name: "example.ChildRecord",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const parentRecord = createRecord({
        name: "example.ParentRecord",
        fields: [
          { name: "child", type: childRecord },
        ],
      });

      const tap = new Tap(new ArrayBuffer(16));
      assertThrows(
        () =>
          parentRecord.write(
            tap,
            { child: { name: "Ann" } } as unknown as Record<string, unknown>,
          ),
        Error,
        `Invalid value: 'undefined' for type: 
{
  "type": "record",
  "name": "ChildRecord",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string",
      "default": "unknown"
    }
  ],
  "namespace": "example"
}
 at path: id`,
      );
    });

    it("throws on non-record values in write", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [{ name: "id", type: new IntType() }],
      });

      const expectedTypeJson = `
{
  "type": "record",
  "name": "Person",
  "fields": [
    {
      "name": "id",
      "type": "int"
    }
  ],
  "namespace": "example"
}`;

      const tap = new Tap(new ArrayBuffer(16));
      assertThrows(
        () => type.write(tap, "string" as unknown as Record<string, unknown>),
        ValidationError,
        `Invalid value: 'string' for type: ${expectedTypeJson}`,
      );
      assertThrows(
        () => type.write(tap, 42 as unknown as Record<string, unknown>),
        ValidationError,
        `Invalid value: '42' for type: ${expectedTypeJson}`,
      );
      assertThrows(
        () => type.write(tap, null as unknown as Record<string, unknown>),
        ValidationError,
        `Invalid value: 'null' for type: ${expectedTypeJson}`,
      );
      assertThrows(
        () => type.write(tap, [1, 2, 3] as unknown as Record<string, unknown>),
        ValidationError,
        `Invalid value: '
[
  1,
  2,
  3
]
' for type: ${expectedTypeJson}`,
      );
    });

    it("throws on non-record values in toBuffer", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [{ name: "id", type: new IntType() }],
      });

      assertThrows(() =>
        type.toBuffer("string" as unknown as Record<string, unknown>)
      );
      assertThrows(() =>
        type.toBuffer(42 as unknown as Record<string, unknown>)
      );
      assertThrows(() =>
        type.toBuffer(null as unknown as Record<string, unknown>)
      );
      assertThrows(() =>
        type.toBuffer([1, 2, 3] as unknown as Record<string, unknown>)
      );
    });

    it("throws when missing required field during toBuffer", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() }, // required field
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      assertThrows(
        () =>
          type.toBuffer({ name: "Ann" } as unknown as Record<string, unknown>),
        ValidationError,
        `Invalid value: 'undefined' for type: 
{
  "type": "record",
  "name": "Person",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string",
      "default": "unknown"
    }
  ],
  "namespace": "example"
}
 at path: id`,
      );
    });

    it("throws on invalid nested record values in toBuffer", () => {
      const nestedType = createRecord({
        name: "example.Address",
        fields: [{ name: "street", type: new StringType() }],
      });

      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "address", type: nestedType },
        ],
      });

      assertThrows(
        () =>
          type.toBuffer(
            { id: 1, address: { street: 123 } } as unknown as Record<
              string,
              unknown
            >,
          ),
        ValidationError,
        `Invalid value: '123' for type: string`,
      );
    });

    it("skips encoded records", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });
      const buffer = type.toBuffer({ id: 1, name: "Ann" });
      const tap = new Tap(buffer);
      type.skip(tap);
      assertEquals(tap._testOnlyPos, buffer.byteLength);
    });
  });

  describe("clone", () => {
    it("clones records and supplies defaults", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const original = { id: 9 };
      const cloned = type.clone(original);
      assertEquals(cloned, { id: 9, name: "unknown" });
      (cloned as Record<string, unknown>).name = "changed";
      assertEquals(original, { id: 9 });
    });

    it("clones nested records and supplies defaults", () => {
      const childRecord = createRecord({
        name: "example.ChildRecord",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const parentRecord = createRecord({
        name: "example.ParentRecord",
        fields: [
          { name: "child", type: childRecord },
          { name: "age", type: new IntType(), default: 25 },
        ],
      });

      const original = { child: { id: 1 } };
      const cloned = parentRecord.clone(original);
      assertEquals(cloned, { child: { id: 1, name: "unknown" }, age: 25 });

      // Verify deep cloning: modifying nested cloned object doesn't affect original
      (cloned.child as Record<string, unknown>).name = "changed";
      assertEquals(original.child, { id: 1 });
    });

    it("throws on non-record values in clone", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [{ name: "id", type: new IntType() }],
      });

      assertThrows(
        () => type.clone("string" as unknown as Record<string, unknown>),
        Error,
        "Cannot clone non-record value.",
      );
      assertThrows(
        () => type.clone(42 as unknown as Record<string, unknown>),
        Error,
        "Cannot clone non-record value.",
      );
      assertThrows(
        () => type.clone(null as unknown as Record<string, unknown>),
        Error,
        "Cannot clone non-record value.",
      );
      assertThrows(
        () => type.clone([1, 2, 3] as unknown as Record<string, unknown>),
        Error,
        "Cannot clone non-record value.",
      );
    });

    it("throws when missing required field during clone", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      assertThrows(
        () => type.clone({ name: "Ann" } as unknown as Record<string, unknown>),
        Error,
        "Missing value for record field id with no default.",
      );
    });
  });

  describe("compare", () => {
    it("compares records using field order", () => {
      const type = createRecord({
        name: "example.Score",
        fields: [
          { name: "score", type: new IntType(), order: "descending" },
          { name: "name", type: new StringType() },
          { name: "ignored", type: new StringType(), order: "ignore" },
          { name: "optional", type: new StringType(), default: "default" },
        ],
      });

      const a = { score: 10, name: "Ann", ignored: "x" };
      const b = { score: 5, name: "Bob", ignored: "y" };
      const c = { score: 10, name: "Bob", ignored: "z" };
      const d = { score: 10, name: "Ann", ignored: "x", optional: "custom" };

      assertEquals(type.compare(a, b), -1); // higher score wins (descending)
      assertEquals(type.compare(a, c), -1); // tie on score, compare by name
      assertEquals(type.compare(c, a), 1);
      assertEquals(type.compare(a, a), 0); // equal records
      assertEquals(type.compare(a, d), 1); // a uses default "default", d uses "custom", "default" > "custom"
    });

    it("compares nested records using field order", () => {
      const childType = createRecord({
        name: "example.Child",
        fields: [
          { name: "priority", type: new IntType(), order: "descending" },
          { name: "label", type: new StringType() },
        ],
      });

      const parentType = createRecord({
        name: "example.Parent",
        fields: [
          { name: "child", type: childType },
          { name: "id", type: new IntType() },
        ],
      });

      const a = { child: { priority: 10, label: "z" }, id: 1 };
      const b = { child: { priority: 5, label: "y" }, id: 2 };
      const c = { child: { priority: 10, label: "a" }, id: 3 };

      assertEquals(parentType.compare(a, b), -1); // higher child priority wins
      assertEquals(parentType.compare(a, c), 1); // same priority, compare child label
      assertEquals(parentType.compare(c, a), -1);
    });

    it("throws when nested field value is not an object in compare", () => {
      const childType = createRecord({
        name: "example.Child",
        fields: [
          { name: "priority", type: new IntType() },
        ],
      });

      const parentType = createRecord({
        name: "example.Parent",
        fields: [
          { name: "child", type: childType },
        ],
      });

      const valid = { child: { priority: 1 } };
      assertThrows(
        () =>
          parentType.compare(
            { child: "not an object" } as unknown as Record<string, unknown>,
            valid,
          ),
        Error,
        "Record comparison requires object values.",
      );
    });

    it("throws on non-record values in compare", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [{ name: "id", type: new IntType() }],
      });

      const valid = { id: 1 };
      assertThrows(() =>
        type.compare("string" as unknown as Record<string, unknown>, valid)
      );
      assertThrows(() =>
        type.compare(valid, 42 as unknown as Record<string, unknown>)
      );
      assertThrows(() =>
        type.compare(null as unknown as Record<string, unknown>, valid)
      );
      assertThrows(() =>
        type.compare(valid, [1, 2, 3] as unknown as Record<string, unknown>)
      );
    });

    it("throws when nested field is not an object in compare", () => {
      const nestedType = createRecord({
        name: "example.Address",
        fields: [{ name: "street", type: new StringType() }],
      });

      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "name", type: new StringType() },
          { name: "address", type: nestedType },
        ],
      });

      const valid = { name: "John", address: { street: "Main St" } };
      assertThrows(
        () =>
          type.compare(
            { name: "John", address: { foo: "bar" } } as unknown as Record<
              string,
              unknown
            >,
            valid,
          ),
        Error,
        "Missing comparable value for field 'street' with no default.",
      );
    });

    it("throws when missing required field during compare", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() }, // required field
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const valid = { id: 1, name: "Ann" };
      assertThrows(() =>
        type.compare(
          { name: "Ann" } as unknown as Record<string, unknown>,
          valid,
        )
      );
    });
  });

  describe("match", () => {
    it("matches encoded record buffers", () => {
      const type = createRecord({
        name: "example.Score",
        fields: [
          { name: "score", type: new IntType(), order: "descending" },
          { name: "name", type: new StringType() },
          { name: "ignored", type: new StringType(), order: "ignore" },
        ],
      });

      const a = { score: 10, name: "Ann", ignored: "x" };
      const b = { score: 5, name: "Bob", ignored: "y" };

      const bufA = type.toBuffer(a);
      const bufB = type.toBuffer(b);

      assertEquals(type.match(new Tap(bufA), new Tap(bufB)), -1); // a has higher score (10 > 5), descending order makes a < b
      assertEquals(type.match(new Tap(bufB), new Tap(bufA)), 1); // b has lower score, so b > a
      assertEquals(type.match(new Tap(bufA), new Tap(type.toBuffer(a))), 0); // identical buffers compare equal
    });
  });

  describe("createResolver", () => {
    it("creates resolver that adds defaulted fields", () => {
      const writer = createRecord({
        name: "example.Person",
        fields: [{ name: "name", type: new StringType() }],
      });
      const reader = createRecord({
        name: "example.Person",
        fields: [
          { name: "name", type: new StringType() },
          { name: "age", type: new IntType(), default: 42 },
        ],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toBuffer({ name: "Ann" });
      const tap = new Tap(buffer);
      assertEquals(resolver.read(tap), { name: "Ann", age: 42 });
    });

    it("maps writer record aliases to reader", () => {
      // This tests record-level aliases: writer record "LegacyPerson" is aliased to reader record "NewPerson".
      // Field names are identical ("name"), so no field aliasing is needed.
      const writer = createRecord({
        name: "example.LegacyPerson",
        fields: [{ name: "name", type: new StringType() }],
      });
      const reader = createRecord({
        name: "example.NewPerson",
        aliases: ["example.LegacyPerson"],
        fields: [{ name: "name", type: new StringType() }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toBuffer({ name: "Sam" });
      const tap = new Tap(buffer);
      assertEquals(resolver.read(tap), { name: "Sam" });
    });

    it("maps writer field aliases to reader field names", () => {
      // This tests field-level aliases: record names are identical ("Person"), but writer field "fullName" is aliased to reader field "name".
      const writer = createRecord({
        name: "example.Person",
        fields: [{
          name: "fullName",
          type: new StringType(),
        }],
      });
      const reader = createRecord({
        name: "example.Person",
        fields: [{
          name: "name",
          type: new StringType(),
          aliases: ["fullName"],
        }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toBuffer({ fullName: "Sam" });
      const tap = new Tap(buffer);
      assertEquals(resolver.read(tap), { name: "Sam" });
    });

    it("maps writer record and field aliases to reader", () => {
      // This tests both record-level and field-level aliases: writer record "LegacyPerson" and field "fullName" are aliased to reader record "NewPerson" and field "name".
      const writer = createRecord({
        name: "example.LegacyPerson",
        fields: [{ name: "fullName", type: new StringType() }],
      });
      const reader = createRecord({
        name: "example.NewPerson",
        aliases: ["example.LegacyPerson"],
        fields: [{
          name: "name",
          type: new StringType(),
          aliases: ["fullName"],
        }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toBuffer({ fullName: "Sam" });
      const tap = new Tap(buffer);
      assertEquals(resolver.read(tap), { name: "Sam" });
    });

    it("maps writer field aliases to reader field names", () => {
      const writer = createRecord({
        name: "example.Person",
        fields: [{
          name: "oldName",
          type: new StringType(),
          aliases: ["newName"],
        }],
      });
      const reader = createRecord({
        name: "example.Person",
        fields: [{ name: "newName", type: new StringType() }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toBuffer({ oldName: "test" });
      const tap = new Tap(buffer);
      assertEquals(resolver.read(tap), { newName: "test" });
    });

    it("skips extra writer fields via resolver", () => {
      const writer = createRecord({
        name: "example.Person",
        fields: [
          { name: "name", type: new StringType() },
          { name: "age", type: new IntType() },
        ],
      });
      const reader = createRecord({
        name: "example.Person",
        fields: [{ name: "name", type: new StringType() }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toBuffer({ name: "Ann", age: 30 });
      const tap = new Tap(buffer);
      assertEquals(resolver.read(tap), { name: "Ann" });
    });

    it("uses nested resolvers for compatible field promotion", () => {
      const writer = createRecord({
        name: "example.Payload",
        fields: [{ name: "data", type: new StringType() }],
      });
      const reader = createRecord({
        name: "example.Payload",
        fields: [{ name: "data", type: new BytesType() }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toBuffer({ data: "\x01\x02" });
      const tap = new Tap(buffer);
      const value = resolver.read(tap) as { data: Uint8Array };
      assertEquals([...value.data], [1, 2]);
    });

    it("throws resolver error when reader field lacks default", () => {
      const writer = createRecord({
        name: "example.Person",
        fields: [{ name: "age", type: new IntType() }],
      });
      const reader = createRecord({
        name: "example.Person",
        fields: [
          { name: "age", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      assertThrows(
        () => reader.createResolver(writer),
        Error,
        "Field 'name' missing from writer schema and has no default",
      );
    });

    it("throws resolver error for incompatible record names", () => {
      const writer = createRecord({
        name: "example.Writer",
        fields: [{ name: "id", type: new IntType() }],
      });
      const reader = createRecord({
        name: "example.Reader",
        fields: [{ name: "id", type: new IntType() }],
      });

      assertThrows(
        () => reader.createResolver(writer),
        Error,
        "Schema evolution not supported from writer type: example.Writer to reader type: example.Reader",
      );
    });

    it("throws for incompatible non-record writer types in createResolver", () => {
      const reader = createRecord({
        name: "example.Record",
        fields: [{ name: "id", type: new IntType() }],
      });

      const writer = new StringType();
      assertThrows(
        () => reader.createResolver(writer),
        Error,
        `Schema evolution not supported from writer type: string to reader type: 
{
  "type": "record",
  "name": "Record",
  "fields": [
    {
      "name": "id",
      "type": "int"
    }
  ],
  "namespace": "example"
}
`,
      );
    });

    it("throws when multiple writer fields map to the same reader field", () => {
      const writer = createRecord({
        name: "example.Writer",
        fields: [
          { name: "fieldA", type: new StringType(), aliases: ["shared"] },
          { name: "fieldB", type: new IntType(), aliases: ["shared"] },
        ],
      });
      const reader = createRecord({
        name: "example.Writer",
        fields: [{ name: "shared", type: new StringType() }],
      });

      assertThrows(
        () => reader.createResolver(writer),
        Error,
        "Multiple writer fields map to reader field: shared",
      );
    });
  });

  describe("toJSON", () => {
    it("exposes schema metadata via toJSON", () => {
      const type = createRecord({
        name: "Person",
        namespace: "example",
        aliases: ["LegacyPerson"],
        fields: [
          {
            name: "name",
            type: new StringType(),
            aliases: ["fullName"],
            default: "Sam",
          },
          {
            name: "score",
            type: new IntType(),
            order: "descending",
          },
        ],
      });

      const json = type.toJSON() as Record<string, unknown>;
      assertEquals(json.type, "record");
      assertEquals(json.name, "Person");
      assertEquals(json.namespace, "example");
      assertEquals(json.aliases, ["example.LegacyPerson"]);
      assertEquals(json.fields, [
        {
          name: "name",
          type: "string",
          aliases: ["fullName"],
          default: "Sam",
        },
        {
          name: "score",
          type: "int",
          order: "descending",
        },
      ]);
    });

    it("exposes minimal schema metadata via toJSON", () => {
      const type = createRecord({
        name: "Simple",
        fields: [
          {
            name: "id",
            type: new IntType(),
          },
        ],
      });

      const json = type.toJSON() as Record<string, unknown>;
      assertEquals(json.type, "record");
      assertEquals(json.name, "Simple");
      assert(!("namespace" in json));
      assert(!("aliases" in json));
      assertEquals(json.fields, [
        {
          name: "id",
          type: "int",
        },
      ]);
    });
  });

  describe("random", () => {
    it("generates random record values", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      const randomValue = type.random();
      assert(typeof randomValue === "object" && randomValue !== null);
      assert("id" in randomValue);
      assert("name" in randomValue);
      assert(typeof (randomValue as Record<string, unknown>).id === "number");
      assert(typeof (randomValue as Record<string, unknown>).name === "string");
    });
  });

  describe("Field methods", () => {
    it("gets fields by name", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      const idField = type.getField("id");
      assert(idField);
      assertEquals(idField!.getName(), "id");

      const nameField = type.getField("name");
      assert(nameField);
      assertEquals(nameField!.getName(), "name");

      const missingField = type.getField("age");
      assertEquals(missingField, undefined);
    });

    it("accesses nested record fields", () => {
      const nestedType = createRecord({
        name: "example.Address",
        fields: [
          { name: "street", type: new StringType() },
          { name: "city", type: new StringType() },
        ],
      });

      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "name", type: new StringType() },
          { name: "address", type: nestedType },
        ],
      });

      const addressField = type.getField("address");
      assert(addressField);

      const addressType = addressField!.getType();
      assert(addressType instanceof RecordType);

      const streetField = (addressType as RecordType).getField("street");
      assert(streetField);
      assertEquals(streetField!.getName(), "street");
    });

    it("nameMatches field names and aliases", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType(), aliases: ["identifier"] },
        ],
      });

      const field = type.getField("id");
      assert(field);
      assert(field!.nameMatches("id"));
      assert(field!.nameMatches("identifier"));
      assert(!field!.nameMatches("name"));
    });

    it("throws when getting default for field without default", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() }, // no default
        ],
      });

      const field = type.getField("id");
      assert(field);
      assertThrows(
        () => field!.getDefault(),
        Error,
        "Field 'id' has no default",
      );
    });
  });
});
