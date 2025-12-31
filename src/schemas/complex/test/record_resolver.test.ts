import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import { SyncReadableTap } from "../../../serialization/tap_sync.ts";
import { IntType } from "../../primitive/int_type.ts";
import { StringType } from "../../primitive/string_type.ts";
import { LongType } from "../../primitive/long_type.ts";
import { BytesType } from "../../primitive/bytes_type.ts";
import { createRecord } from "./record_test_utils.ts";

describe("RecordResolver", () => {
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
});
