import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../../serialization/sync_tap.ts";
import { IntType } from "../../primitive/int_type.ts";
import { StringType } from "../../primitive/string_type.ts";
import { LongType } from "../../primitive/long_type.ts";
import { BytesType } from "../../primitive/bytes_type.ts";
import { RecordType } from "../record_type.ts";
import { resolveNames } from "../resolve_names.ts";
import type { Type } from "../../type.ts";
import { ValidationError } from "../../error.ts";
import { NullType } from "../../primitive/null_type.ts";
import { UnionType } from "../union_type.ts";
import { createType } from "../../../type/create_type.ts";

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
  describe("constructor validation", () => {
    it("requires a fields array", () => {
      const names = resolveNames({ name: "example.Empty" });
      assertThrows(() =>
        new RecordType({
          ...names,
          fields: undefined as unknown as FieldSpec[],
        })
      );
    });

    it("supports lazy field thunks for recursive schemas", async () => {
      const names = resolveNames({ name: "example.Node" });

      // Build the record in two stages: register the named type first, then
      // resolve fields when they are actually needed. This mirrors how
      // createType() handles recursive schemas.
      // deno-lint-ignore prefer-const
      let nodeType: RecordType;
      nodeType = new RecordType({
        ...names,
        fields: () => [
          { name: "value", type: new IntType() },
          {
            name: "next",
            // The union includes the record itself, relying on the thunk above
            // so the RecordType instance is already assigned when this executes.
            type: new UnionType({ types: [new NullType(), nodeType] }),
          },
        ],
      });

      // Trigger field materialization and verify we can encode/decode recursive data.
      const value = {
        value: 1,
        next: {
          "example.Node": {
            value: 2,
            next: {
              "example.Node": {
                value: 3,
                next: null,
              },
            },
          },
        },
      };
      const buffer = await nodeType.toBuffer(value);
      const decoded = await nodeType.fromBuffer(buffer);

      assertEquals(decoded, value);
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
    it("serializes and deserializes records with defaults", async () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const buffer = await type.toBuffer({ id: 5 });
      const tap = new Tap(buffer);
      const decoded = await type.read(tap);
      assertEquals(decoded, { id: 5, name: "unknown" });

      const roundTrip = await type.fromBuffer(buffer);
      assertEquals(roundTrip, { id: 5, name: "unknown" });
    });

    it("writes records with default values for missing fields", async () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const buffer = new ArrayBuffer(16);
      const tap = new Tap(buffer);
      await type.write(tap, { id: 42 }); // name is missing but has default

      // Verify the written data can be read back correctly
      const readTap = new Tap(buffer);
      const decoded = await type.read(readTap);
      assertEquals(decoded, { id: 42, name: "unknown" });
    });

    it("throws when missing required field during write for non-nested records", async () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const tap = new Tap(new ArrayBuffer(16));
      await assertRejects(
        async () =>
          await type.write(
            tap,
            { name: "Ann" } as unknown as Record<string, unknown>,
          ),
        Error,
        "Invalid value: 'undefined'",
      );
    });

    it("throws when missing required field during write for nested records", async () => {
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
      await assertRejects(
        async () =>
          await parentRecord.write(
            tap,
            { child: { name: "Ann" } } as unknown as Record<string, unknown>,
          ),
        Error,
        "Invalid value: 'undefined'",
      );
    });

    it("throws on non-record values in write", async () => {
      const type = createRecord({
        name: "example.Person",
        fields: [{ name: "id", type: new IntType() }],
      });

      const tap = new Tap(new ArrayBuffer(16));
      await assertRejects(
        async () =>
          await type.write(tap, "string" as unknown as Record<string, unknown>),
        ValidationError,
        "Invalid value: 'string'",
      );
      await assertRejects(
        async () =>
          await type.write(tap, 42 as unknown as Record<string, unknown>),
        ValidationError,
        "Invalid value: '42'",
      );
      await assertRejects(
        async () =>
          await type.write(tap, null as unknown as Record<string, unknown>),
        ValidationError,
        "Invalid value: 'null'",
      );
      await assertRejects(
        async () =>
          await type.write(
            tap,
            [1, 2, 3] as unknown as Record<string, unknown>,
          ),
        ValidationError,
        "Invalid value:",
      );
    });

    it("throws on non-record values in toBuffer", async () => {
      const type = createRecord({
        name: "example.Person",
        fields: [{ name: "id", type: new IntType() }],
      });

      await assertRejects(async () =>
        await type.toBuffer("string" as unknown as Record<string, unknown>)
      );
      await assertRejects(async () =>
        await type.toBuffer(42 as unknown as Record<string, unknown>)
      );
      await assertRejects(async () =>
        await type.toBuffer(null as unknown as Record<string, unknown>)
      );
      await assertRejects(async () =>
        await type.toBuffer([1, 2, 3] as unknown as Record<string, unknown>)
      );
    });

    it("throws when missing required field during toBuffer", async () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() }, // required field
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      await assertRejects(
        async () =>
          await type.toBuffer(
            { name: "Ann" } as unknown as Record<string, unknown>,
          ),
        ValidationError,
        "Invalid value: 'undefined'",
      );
    });

    it("throws on invalid nested record values in toBuffer", async () => {
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

      await assertRejects(
        async () =>
          await type.toBuffer(
            { id: 1, address: { street: 123 } } as unknown as Record<
              string,
              unknown
            >,
          ),
        ValidationError,
        `Invalid value: '123' for type: string`,
      );
    });

    it("skips encoded records", async () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });
      const buffer = await type.toBuffer({ id: 1, name: "Ann" });
      const tap = new Tap(buffer);
      await type.skip(tap);
      assertEquals(tap.getPos(), buffer.byteLength);
    });
  });

  describe("maps with union values and defaults", () => {
    const schema = {
      type: "record",
      name: "example.UnionMapRecord",
      fields: [
        {
          name: "items",
          type: { type: "map", values: ["null", "long", "double", "bytes"] },
          default: {},
        },
      ],
    } as const;

    it("applies empty-map default and round-trips", async () => {
      const type = createType(schema);

      const buffer = await type.toBuffer({});
      const decoded = await type.fromBuffer(buffer) as {
        items: Map<string, unknown>;
      };

      assert(decoded.items instanceof Map);
      assertEquals(decoded, { items: new Map() });
    });

    it("round-trips map entries for each union branch", async () => {
      const type = createType(schema);
      const value = {
        items: new Map<string, unknown>([
          ["nothing", null],
          ["longVal", { long: 123n }],
          ["doubleVal", { double: 1.5 }],
          ["bytesVal", { bytes: new Uint8Array([1, 2, 3]) }],
        ]),
      };

      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
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
      const cloned = type.cloneFromValue(original);
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
      const cloned = parentRecord.cloneFromValue(original);
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
        () =>
          type.cloneFromValue("string" as unknown as Record<string, unknown>),
        Error,
        "Cannot clone non-record value.",
      );
      assertThrows(
        () => type.cloneFromValue(42 as unknown as Record<string, unknown>),
        Error,
        "Cannot clone non-record value.",
      );
      assertThrows(
        () => type.cloneFromValue(null as unknown as Record<string, unknown>),
        Error,
        "Cannot clone non-record value.",
      );
      assertThrows(
        () =>
          type.cloneFromValue([1, 2, 3] as unknown as Record<string, unknown>),
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
        () =>
          type.cloneFromValue(
            { name: "Ann" } as unknown as Record<string, unknown>,
          ),
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
    it("matches encoded record buffers", async () => {
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

      const bufA = await type.toBuffer(a);
      const bufB = await type.toBuffer(b);

      assertEquals(await type.match(new Tap(bufA), new Tap(bufB)), -1); // a has higher score (10 > 5), descending order makes a < b
      assertEquals(await type.match(new Tap(bufB), new Tap(bufA)), 1); // b has lower score, so b > a
      assertEquals(
        await type.match(new Tap(bufA), new Tap(await type.toBuffer(a))),
        0,
      ); // identical buffers compare equal
    });
  });

  describe("createResolver", () => {
    it("short-circuits resolver creation for the same record instance", async () => {
      const record = createRecord({
        name: "example.Solo",
        fields: [
          { name: "id", type: new IntType() },
          { name: "note", type: new StringType(), default: "same-instance" },
        ],
      });

      const resolver = record.createResolver(record);
      const buffer = await record.toBuffer({ id: 1 });
      const tap = new Tap(buffer);
      assertEquals(await resolver.read(tap), {
        id: 1,
        note: "same-instance",
      });
    });

    it("creates resolver that adds defaulted fields", async () => {
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
      const buffer = await writer.toBuffer({ name: "Ann" });
      const tap = new Tap(buffer);
      assertEquals(await resolver.read(tap), { name: "Ann", age: 42 });
    });

    it("maps writer record aliases to reader", async () => {
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
      const buffer = await writer.toBuffer({ name: "Sam" });
      const tap = new Tap(buffer);
      assertEquals(await resolver.read(tap), { name: "Sam" });
    });

    it("maps writer field aliases to reader field names", async () => {
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
      const buffer = await writer.toBuffer({ fullName: "Sam" });
      const tap = new Tap(buffer);
      assertEquals(await resolver.read(tap), { name: "Sam" });
    });

    it("maps writer record and field aliases to reader", async () => {
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
      const buffer = await writer.toBuffer({ fullName: "Sam" });
      const tap = new Tap(buffer);
      assertEquals(await resolver.read(tap), { name: "Sam" });
    });

    it("maps writer field aliases to reader field names (writer alias matches reader name)", async () => {
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
      const buffer = await writer.toBuffer({ oldName: "test" });
      const tap = new Tap(buffer);
      assertEquals(await resolver.read(tap), { newName: "test" });
    });

    it("skips extra writer fields via resolver", async () => {
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
      const buffer = await writer.toBuffer({ name: "Ann", age: 30 });
      const tap = new Tap(buffer);
      assertEquals(await resolver.read(tap), { name: "Ann" });
    });

    it("uses nested resolvers for compatible field promotion", async () => {
      const writer = createRecord({
        name: "example.Payload",
        fields: [{ name: "data", type: new StringType() }],
      });
      const reader = createRecord({
        name: "example.Payload",
        fields: [{ name: "data", type: new BytesType() }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = await writer.toBuffer({ data: "\x01\x02" });
      const tap = new Tap(buffer);
      const value = await resolver.read(tap) as { data: Uint8Array };
      assertEquals([...value.data], [1, 2]);
    });

    it("skips extra writer fields via sync resolver", () => {
      const writer = createRecord({
        name: "example.SyncWriter",
        fields: [
          { name: "name", type: new StringType() },
          { name: "age", type: new IntType() },
        ],
      });
      const reader = createRecord({
        name: "example.SyncWriter",
        fields: [{ name: "name", type: new StringType() }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toSyncBuffer({ name: "Ann", age: 30 });
      const result = resolver.readSync(new SyncReadableTap(buffer));
      assertEquals(result, { name: "Ann" });
    });

    it("uses nested resolvers for compatible field promotion via sync resolver", () => {
      const writer = createRecord({
        name: "example.SyncPayload",
        fields: [{ name: "data", type: new IntType() }],
      });
      const reader = createRecord({
        name: "example.SyncPayload",
        fields: [{ name: "data", type: new LongType() }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toSyncBuffer({ data: 15 });
      const result = resolver.readSync(new SyncReadableTap(buffer)) as {
        data: bigint;
      };
      assertEquals(result.data, 15n);
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
        "Schema evolution not supported from writer type: string to reader type:",
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
      assertEquals(json.name, "example.Person");
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

  describe("sync serialization", () => {
    const type = createRecord({
      name: "example.Sync",
      fields: [
        { name: "id", type: new IntType() },
        { name: "name", type: new StringType() },
      ],
    });

    it("round-trips via sync buffer", () => {
      const recordValue = { id: 1, name: "Ann" };
      const buffer = type.toSyncBuffer(recordValue);
      assertEquals(type.fromSyncBuffer(buffer), recordValue);
    });

    it("reads and writes via sync taps", () => {
      const recordValue = { id: 2, name: "Bob" };
      const buffer = new ArrayBuffer(256);
      const writeTap = new SyncWritableTap(buffer);
      type.writeSync(writeTap, recordValue);
      const readTap = new SyncReadableTap(buffer);
      assertEquals(type.readSync(readTap), recordValue);
      assertEquals(readTap.getPos(), writeTap.getPos());
    });

    it("throws when writeSync receives a non-record value", () => {
      const buffer = new ArrayBuffer(64);
      const tap = new SyncWritableTap(buffer);
      assertThrows(
        () =>
          type.writeSync(
            tap,
            "not a record" as unknown as Record<string, unknown>,
          ),
        Error,
        "Invalid value",
      );
    });

    it("throws when toSyncBuffer receives a non-record value", () => {
      assertThrows(
        () =>
          type.toSyncBuffer(
            "not a record" as unknown as Record<string, unknown>,
          ),
        Error,
        "Invalid value",
      );
    });

    it("skips records via sync taps", () => {
      const recordValue = { id: 3, name: "Cleo" };
      const buffer = type.toSyncBuffer(recordValue);
      const tap = new SyncReadableTap(buffer);
      assertEquals(tap.getPos(), 0);
      type.skipSync(tap);
      assertEquals(tap.getPos(), buffer.byteLength);
    });

    it("matches encoded records via sync taps", () => {
      const a = { id: 1, name: "A" };
      const b = { id: 2, name: "B" };
      const bufA = type.toSyncBuffer(a);
      const bufB = type.toSyncBuffer(b);

      assertEquals(
        type.matchSync(new SyncReadableTap(bufA), new SyncReadableTap(bufB)),
        -1,
      );
      assertEquals(
        type.matchSync(new SyncReadableTap(bufA), new SyncReadableTap(bufA)),
        0,
      );
    });

    it("skips ignore-ordered fields via matchSync", () => {
      const ignoreType = createRecord({
        name: "example.IgnoreOrder",
        fields: [
          { name: "score", type: new IntType(), order: "ascending" },
          { name: "ignored", type: new IntType(), order: "ignore" },
        ],
      });

      const valueA = { score: 5, ignored: 10 };
      const valueB = { score: 5, ignored: 20 };
      const bufA = ignoreType.toSyncBuffer(valueA);
      const bufB = ignoreType.toSyncBuffer(valueB);

      assertEquals(
        ignoreType.matchSync(
          new SyncReadableTap(bufA),
          new SyncReadableTap(bufB),
        ),
        0,
      );
    });

    it("applies defaults when writing sync data", () => {
      const defaultType = createRecord({
        name: "example.DefaultSync",
        fields: [
          { name: "id", type: new IntType() },
          { name: "rating", type: new IntType(), default: 7 },
        ],
      });

      const recordValue = { id: 9 };
      const buffer = new ArrayBuffer(64);
      const writeTap = new SyncWritableTap(buffer);
      defaultType.writeSync(writeTap, recordValue);
      const readTap = new SyncReadableTap(buffer);

      assertEquals(
        defaultType.readSync(readTap),
        { id: 9, rating: 7 },
      );

      const syncBuffer = defaultType.toSyncBuffer(recordValue);
      assertEquals(defaultType.fromSyncBuffer(syncBuffer), {
        id: 9,
        rating: 7,
      });
    });

    it("throws when missing required field during writeSync", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      const buffer = new ArrayBuffer(16);
      const tap = new SyncWritableTap(buffer);
      assertThrows(
        () =>
          type.writeSync(
            tap,
            { name: "Ann" } as unknown as Record<string, unknown>,
          ),
        Error,
        `Invalid value: 'undefined' for type:`,
      );
    });

    it("throws when missing required field during toSyncBuffer", () => {
      const type = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() }, // required field
          { name: "name", type: new StringType(), default: "unknown" },
        ],
      });

      assertThrows(
        () =>
          type.toSyncBuffer(
            { name: "Ann" } as unknown as Record<string, unknown>,
          ),
        Error,
        `Invalid value: 'undefined' for type:`,
      );
    });

    it("uses nested resolvers for compatible field promotion via sync resolver", () => {
      const writer = createRecord({
        name: "example.SyncPayload",
        fields: [{ name: "data", type: new StringType() }],
      });
      const reader = createRecord({
        name: "example.SyncPayload",
        fields: [{ name: "data", type: new BytesType() }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toSyncBuffer({ data: "\x01\x02" });
      const result = resolver.readSync(new SyncReadableTap(buffer)) as {
        data: Uint8Array;
      };
      assertEquals([...result.data], [1, 2]);
    });

    it("uses direct field reading when no resolver needed via sync resolver", () => {
      const sharedType = new StringType();
      const writer = createRecord({
        name: "example.SyncDirect",
        fields: [{ name: "data", type: sharedType }],
      });
      const reader = createRecord({
        name: "example.SyncDirect",
        fields: [{ name: "data", type: sharedType }],
      });

      const resolver = reader.createResolver(writer);
      const buffer = writer.toSyncBuffer({ data: "test" });
      const result = resolver.readSync(new SyncReadableTap(buffer)) as {
        data: string;
      };
      assertEquals(result.data, "test");
    });

    it("reads records via sync resolver", () => {
      const writer = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });
      const reader = createRecord({
        name: "example.Person",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
          { name: "rating", type: new IntType(), default: 0 },
        ],
      });
      const resolver = reader.createResolver(writer);

      const value = { id: 3, name: "Sync" };
      const buffer = writer.toSyncBuffer(value);
      const result = resolver.readSync(new SyncReadableTap(buffer)) as {
        id: number;
        name: string;
        rating: number;
      };
      assertEquals(result.id, value.id);
      assertEquals(result.name, value.name);
      assertEquals(result.rating, 0);
    });
  });

  describe("validate=false mode", () => {
    it("writes without validation via write() when validate=false", async () => {
      const names = resolveNames({ name: "example.NoValidate" });
      const type = new RecordType({
        ...names,
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
        validate: false,
      });

      const buffer = new ArrayBuffer(64);
      const tap = new Tap(buffer);
      await type.write(tap, { id: 42, name: "test" });

      const readTap = new Tap(buffer);
      const decoded = await type.read(readTap);
      assertEquals(decoded, { id: 42, name: "test" });
    });

    it("writes without validation via writeSync() when validate=false", () => {
      const names = resolveNames({ name: "example.NoValidateSync" });
      const type = new RecordType({
        ...names,
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
        validate: false,
      });

      const buffer = new ArrayBuffer(64);
      const writeTap = new SyncWritableTap(buffer);
      type.writeSync(writeTap, { id: 42, name: "test" });

      const readTap = new SyncReadableTap(buffer);
      const decoded = type.readSync(readTap);
      assertEquals(decoded, { id: 42, name: "test" });
    });

    it("uses toBuffer() without validation when validate=false", async () => {
      const names = resolveNames({ name: "example.NoValidateBuffer" });
      const type = new RecordType({
        ...names,
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
        validate: false,
      });

      const buffer = await type.toBuffer({ id: 1, name: "test" });
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, { id: 1, name: "test" });
    });

    it("uses toSyncBuffer() without validation when validate=false", () => {
      const names = resolveNames({ name: "example.NoValidateSyncBuffer" });
      const type = new RecordType({
        ...names,
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
        validate: false,
      });

      const buffer = type.toSyncBuffer({ id: 1, name: "test" });
      const decoded = type.fromSyncBuffer(buffer);
      assertEquals(decoded, { id: 1, name: "test" });
    });
  });

  describe("writeUnchecked methods", () => {
    it("writes async unchecked", async () => {
      const type = createRecord({
        name: "example.UncheckedAsync",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      const buffer = new ArrayBuffer(64);
      const tap = new Tap(buffer);
      await type.writeUnchecked(tap, { id: 99, name: "unchecked" });

      const readTap = new Tap(buffer);
      const decoded = await type.read(readTap);
      assertEquals(decoded, { id: 99, name: "unchecked" });
    });

    it("writes sync unchecked", () => {
      const type = createRecord({
        name: "example.UncheckedSync",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      const buffer = new ArrayBuffer(64);
      const writeTap = new SyncWritableTap(buffer);
      type.writeSyncUnchecked(writeTap, { id: 99, name: "unchecked" });

      const readTap = new SyncReadableTap(buffer);
      const decoded = type.readSync(readTap);
      assertEquals(decoded, { id: 99, name: "unchecked" });
    });
  });

  describe("compiled unchecked writers with various primitive types", () => {
    it("handles all primitive types in unchecked async mode", async () => {
      const BooleanType = (await import("../../primitive/boolean_type.ts"))
        .BooleanType;
      const FloatType = (await import("../../primitive/float_type.ts"))
        .FloatType;
      const DoubleType = (await import("../../primitive/double_type.ts"))
        .DoubleType;

      // Create record with all primitive field types to exercise each branch
      // in #compileUncheckedWriter
      const names = resolveNames({ name: "example.AllPrimitives" });
      const type = new RecordType({
        ...names,
        fields: [
          { name: "nullField", type: new NullType() },
          { name: "boolField", type: new BooleanType() },
          { name: "intField", type: new IntType() },
          { name: "longField", type: new LongType() },
          { name: "floatField", type: new FloatType() },
          { name: "doubleField", type: new DoubleType() },
          { name: "bytesField", type: new BytesType() },
          { name: "stringField", type: new StringType() },
        ],
        validate: false,
      });

      const value = {
        nullField: null,
        boolField: true,
        intField: 42,
        longField: 123n,
        floatField: 3.14,
        doubleField: 2.718281828,
        bytesField: new Uint8Array([1, 2, 3]),
        stringField: "hello",
      };

      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);

      assertEquals(decoded.nullField, null);
      assertEquals(decoded.boolField, true);
      assertEquals(decoded.intField, 42);
      assertEquals(decoded.longField, 123n);
      // Float comparison with tolerance
      assert(Math.abs((decoded.floatField as number) - 3.14) < 0.001);
      assertEquals(decoded.doubleField, 2.718281828);
      assertEquals([...(decoded.bytesField as Uint8Array)], [1, 2, 3]);
      assertEquals(decoded.stringField, "hello");
    });

    it("handles all primitive types in unchecked sync mode", async () => {
      const BooleanType = (await import("../../primitive/boolean_type.ts"))
        .BooleanType;
      const FloatType = (await import("../../primitive/float_type.ts"))
        .FloatType;
      const DoubleType = (await import("../../primitive/double_type.ts"))
        .DoubleType;

      const names = resolveNames({ name: "example.AllPrimitivesSync" });
      const type = new RecordType({
        ...names,
        fields: [
          { name: "nullField", type: new NullType() },
          { name: "boolField", type: new BooleanType() },
          { name: "intField", type: new IntType() },
          { name: "longField", type: new LongType() },
          { name: "floatField", type: new FloatType() },
          { name: "doubleField", type: new DoubleType() },
          { name: "bytesField", type: new BytesType() },
          { name: "stringField", type: new StringType() },
        ],
        validate: false,
      });

      const value = {
        nullField: null,
        boolField: false,
        intField: -100,
        longField: -999n,
        floatField: 1.5,
        doubleField: 3.14159,
        bytesField: new Uint8Array([4, 5, 6]),
        stringField: "world",
      };

      const buffer = type.toSyncBuffer(value);
      const decoded = type.fromSyncBuffer(buffer);

      assertEquals(decoded.nullField, null);
      assertEquals(decoded.boolField, false);
      assertEquals(decoded.intField, -100);
      assertEquals(decoded.longField, -999n);
      assert(Math.abs((decoded.floatField as number) - 1.5) < 0.001);
      assertEquals(decoded.doubleField, 3.14159);
      assertEquals([...(decoded.bytesField as Uint8Array)], [4, 5, 6]);
      assertEquals(decoded.stringField, "world");
    });

    it("handles nested records in unchecked mode", async () => {
      const innerNames = resolveNames({ name: "example.InnerUnchecked" });
      const inner = new RecordType({
        ...innerNames,
        fields: [{ name: "value", type: new IntType() }],
        validate: false,
      });

      const outerNames = resolveNames({ name: "example.OuterUnchecked" });
      const outer = new RecordType({
        ...outerNames,
        fields: [{ name: "nested", type: inner }],
        validate: false,
      });

      const value = { nested: { value: 42 } };

      // Test async
      const asyncBuffer = await outer.toBuffer(value);
      const asyncDecoded = await outer.fromBuffer(asyncBuffer);
      assertEquals(asyncDecoded, value);

      // Test sync
      const syncBuffer = outer.toSyncBuffer(value);
      const syncDecoded = outer.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    it("handles union types in unchecked mode via fallback writeUnchecked", async () => {
      const unionType = new UnionType({
        types: [new NullType(), new IntType()],
      });

      const names = resolveNames({ name: "example.UnionFieldUnchecked" });
      const type = new RecordType({
        ...names,
        fields: [{ name: "maybeInt", type: unionType }],
        validate: false,
      });

      const value = { maybeInt: { int: 42 } };

      // Test async
      const asyncBuffer = await type.toBuffer(value);
      const asyncDecoded = await type.fromBuffer(asyncBuffer);
      assertEquals(asyncDecoded, value);

      // Test sync
      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    it("applies defaults in unchecked mode without throwing", async () => {
      const names = resolveNames({ name: "example.DefaultsUnchecked" });
      const type = new RecordType({
        ...names,
        fields: [
          { name: "id", type: new IntType() },
          { name: "score", type: new IntType(), default: 0 },
        ],
        validate: false,
      });

      // Missing 'score' field - should use default
      const value = { id: 10 };

      const asyncBuffer = await type.toBuffer(value);
      const asyncDecoded = await type.fromBuffer(asyncBuffer);
      assertEquals(asyncDecoded, { id: 10, score: 0 });

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, { id: 10, score: 0 });
    });
  });

  describe("compiled writer caching", () => {
    it("caches compiled writers on repeated calls", async () => {
      const type = createRecord({
        name: "example.CachedWriter",
        fields: [{ name: "id", type: new IntType() }],
      });

      const value = { id: 1 };

      // First call creates the writer
      const buffer1 = await type.toBuffer(value);
      // Second call should use cached writer
      const buffer2 = await type.toBuffer(value);

      assertEquals(buffer1.byteLength, buffer2.byteLength);
      assertEquals(
        new Uint8Array(buffer1),
        new Uint8Array(buffer2),
      );
    });

    it("caches compiled sync writers on repeated calls", () => {
      const type = createRecord({
        name: "example.CachedSyncWriter",
        fields: [{ name: "id", type: new IntType() }],
      });

      const value = { id: 1 };

      const buffer1 = type.toSyncBuffer(value);
      const buffer2 = type.toSyncBuffer(value);

      assertEquals(buffer1.byteLength, buffer2.byteLength);
      assertEquals(
        new Uint8Array(buffer1),
        new Uint8Array(buffer2),
      );
    });

    it("caches unchecked compiled writers on repeated calls", async () => {
      const names = resolveNames({ name: "example.CachedUnchecked" });
      const type = new RecordType({
        ...names,
        fields: [{ name: "id", type: new IntType() }],
        validate: false,
      });

      const value = { id: 1 };

      // First call creates the writer, second uses cached
      const buffer1 = await type.toBuffer(value);
      const buffer2 = await type.toBuffer(value);

      assertEquals(buffer1.byteLength, buffer2.byteLength);
    });

    it("caches unchecked compiled sync writers on repeated calls", () => {
      const names = resolveNames({ name: "example.CachedUncheckedSync" });
      const type = new RecordType({
        ...names,
        fields: [{ name: "id", type: new IntType() }],
        validate: false,
      });

      const value = { id: 1 };

      const buffer1 = type.toSyncBuffer(value);
      const buffer2 = type.toSyncBuffer(value);

      assertEquals(buffer1.byteLength, buffer2.byteLength);
    });
  });

  describe("nested records with validation", () => {
    it("handles nested RecordType fields in validated sync mode", () => {
      const innerNames = resolveNames({ name: "example.InnerValidated" });
      const inner = new RecordType({
        ...innerNames,
        fields: [{ name: "value", type: new IntType() }],
        validate: true,
      });

      const outerNames = resolveNames({ name: "example.OuterValidated" });
      const outer = new RecordType({
        ...outerNames,
        fields: [{ name: "nested", type: inner }],
        validate: true,
      });

      const value = { nested: { value: 42 } };

      const buffer = outer.toSyncBuffer(value);
      const decoded = outer.fromSyncBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("handles nested RecordType fields in validated async mode", async () => {
      const innerNames = resolveNames({ name: "example.InnerValidatedAsync" });
      const inner = new RecordType({
        ...innerNames,
        fields: [{ name: "value", type: new IntType() }],
        validate: true,
      });

      const outerNames = resolveNames({ name: "example.OuterValidatedAsync" });
      const outer = new RecordType({
        ...outerNames,
        fields: [{ name: "nested", type: inner }],
        validate: true,
      });

      const value = { nested: { value: 42 } };

      const buffer = await outer.toBuffer(value);
      const decoded = await outer.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("handles recursive record types with cache hit during compilation", async () => {
      // This test exercises the cache early-return in #getOrCreateCompiledWriter
      // by creating a recursive record type that references itself
      const names = resolveNames({ name: "example.RecursiveNode" });

      // deno-lint-ignore prefer-const
      let nodeType: RecordType;
      nodeType = new RecordType({
        ...names,
        fields: () => [
          { name: "value", type: new IntType() },
          {
            name: "children",
            // Array of the same record type
            type: new UnionType({
              types: [new NullType(), nodeType],
            }),
          },
        ],
        validate: true,
      });

      const value = {
        value: 1,
        children: {
          "example.RecursiveNode": {
            value: 2,
            children: null,
          },
        },
      };

      // First serialization compiles the writer
      const buffer1 = await nodeType.toBuffer(value);
      // Second call should use cached writer
      const buffer2 = await nodeType.toBuffer(value);

      assertEquals(buffer1.byteLength, buffer2.byteLength);

      const decoded = await nodeType.fromBuffer(buffer1);
      assertEquals(decoded, value);
    });

    it("handles recursive record types in sync mode with cache hit", () => {
      const names = resolveNames({ name: "example.RecursiveSyncNode" });

      // deno-lint-ignore prefer-const
      let nodeType: RecordType;
      nodeType = new RecordType({
        ...names,
        fields: () => [
          { name: "value", type: new IntType() },
          {
            name: "next",
            type: new UnionType({
              types: [new NullType(), nodeType],
            }),
          },
        ],
        validate: true,
      });

      const value = {
        value: 1,
        next: {
          "example.RecursiveSyncNode": {
            value: 2,
            next: null,
          },
        },
      };

      const buffer1 = nodeType.toSyncBuffer(value);
      const buffer2 = nodeType.toSyncBuffer(value);

      assertEquals(buffer1.byteLength, buffer2.byteLength);
      assertEquals(nodeType.fromSyncBuffer(buffer1), value);
    });
  });

  describe("unchecked mode edge cases", () => {
    it("still throws at primitive level when missing required field in unchecked async mode", async () => {
      const names = resolveNames({ name: "example.NoDefaultUnchecked" });
      const type = new RecordType({
        ...names,
        fields: [
          { name: "id", type: new IntType() },
          { name: "optional", type: new IntType() }, // No default
        ],
        validate: false,
      });

      // Missing 'optional' field with no default - in unchecked mode this passes undefined
      // to the primitive serializer, which will throw because it can't convert undefined to int
      const buffer = new ArrayBuffer(64);
      const tap = new Tap(buffer);
      await assertRejects(
        async () => await type.writeUnchecked(tap, { id: 1 }),
        TypeError,
        "undefined",
      );
    });

    it("still throws at primitive level when missing required field in unchecked sync mode", () => {
      const names = resolveNames({ name: "example.NoDefaultUncheckedSync" });
      const type = new RecordType({
        ...names,
        fields: [
          { name: "id", type: new IntType() },
          { name: "optional", type: new IntType() }, // No default
        ],
        validate: false,
      });

      // Missing 'optional' field with no default
      const buffer = new ArrayBuffer(64);
      const tap = new SyncWritableTap(buffer);
      assertThrows(
        () => type.writeSyncUnchecked(tap, { id: 1 }),
        RangeError,
        "undefined",
      );
    });
  });

  describe("mutual recursion cache hit", () => {
    it("hits cache early-return with truly direct RecordType mutual recursion (async)", async () => {
      // Create mutually recursive record types where both fields are DIRECT RecordType
      // (not wrapped in Union). This exercises the cache early-return during compilation.
      // When A compiles: sets placeholder -> compiles B -> B sets placeholder -> B compiles A
      // -> A already has placeholder (cache hit!) -> returns without infinite recursion.
      const namesA = resolveNames({ name: "example.DirectMutualA" });
      const namesB = resolveNames({ name: "example.DirectMutualB" });

      // deno-lint-ignore prefer-const
      let typeA: RecordType;
      // deno-lint-ignore prefer-const
      let typeB: RecordType;

      typeA = new RecordType({
        ...namesA,
        fields: () => [
          { name: "valueA", type: new IntType() },
          { name: "refB", type: typeB },
        ],
        validate: true,
      });

      typeB = new RecordType({
        ...namesB,
        fields: () => [
          { name: "valueB", type: new IntType() },
          { name: "refA", type: typeA },
        ],
        validate: true,
      });

      // Minimal nested value - the cache hit happens at compile time, not at runtime depth.
      // We just need enough structure to trigger serialization; null terminates the chain.
      const value = {
        valueA: 1,
        refB: {
          valueB: 2,
          // deno-lint-ignore no-explicit-any
          refA: null as any,
        },
      };

      // Throws during serialization due to null, but compilation succeeded (cache hit worked)
      await assertRejects(async () => await typeA.toBuffer(value), Error);
    });

    it("hits cache early-return with truly direct RecordType mutual recursion (sync)", () => {
      // Same test for sync mode - exercises the sync compiled writer cache early-return
      const namesA = resolveNames({ name: "example.DirectMutualSyncA" });
      const namesB = resolveNames({ name: "example.DirectMutualSyncB" });

      // deno-lint-ignore prefer-const
      let typeA: RecordType;
      // deno-lint-ignore prefer-const
      let typeB: RecordType;

      typeA = new RecordType({
        ...namesA,
        fields: () => [
          { name: "valueA", type: new IntType() },
          { name: "refB", type: typeB },
        ],
        validate: true,
      });

      typeB = new RecordType({
        ...namesB,
        fields: () => [
          { name: "valueB", type: new IntType() },
          { name: "refA", type: typeA },
        ],
        validate: true,
      });

      // Minimal nested value - cache hit happens at compile time
      const value = {
        valueA: 1,
        refB: {
          valueB: 2,
          // deno-lint-ignore no-explicit-any
          refA: null as any,
        },
      };

      // Throws during serialization due to null, but compilation succeeded
      assertThrows(() => typeA.toSyncBuffer(value), Error);
    });
  });
});
