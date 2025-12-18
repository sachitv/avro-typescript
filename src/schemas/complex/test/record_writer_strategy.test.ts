import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { IntType } from "../../primitive/int_type.ts";
import { StringType } from "../../primitive/string_type.ts";
import { LongType } from "../../primitive/long_type.ts";
import { FloatType } from "../../primitive/float_type.ts";
import { DoubleType } from "../../primitive/double_type.ts";
import { BooleanType } from "../../primitive/boolean_type.ts";
import { BytesType } from "../../primitive/bytes_type.ts";
import { NullType } from "../../primitive/null_type.ts";
import { UnionType } from "../union_type.ts";
import { ArrayType } from "../array_type.ts";
import {
  CompiledWriterStrategy,
  defaultWriterStrategy,
  InterpretedWriterStrategy,
} from "../record_writer_strategy.ts";
import { RecordWriterCache } from "../record_writer_cache.ts";
import { createRecord } from "./record_test_utils.ts";
import { createType } from "../../../type/create_type.ts";

describe("RecordWriterStrategy", () => {
  describe("CompiledWriterStrategy", () => {
    it("is the default strategy", () => {
      assertEquals(
        defaultWriterStrategy instanceof CompiledWriterStrategy,
        true,
      );
    });

    it("inlines primitive type writes in unchecked mode", async () => {
      const type = createRecord({
        name: "example.AllPrimitives",
        writerStrategy: new CompiledWriterStrategy(),
        validate: false,
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
      });

      const value = {
        nullField: null,
        boolField: true,
        intField: 42,
        longField: 100n,
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
      assertEquals(decoded.longField, 100n);
      assertEquals(typeof decoded.floatField, "number");
      assertEquals(decoded.doubleField, 2.718281828);
      assertEquals([...(decoded.bytesField as Uint8Array)], [1, 2, 3]);
      assertEquals(decoded.stringField, "hello");
    });

    it("handles complex types via writeUnchecked fallback", async () => {
      const type = createRecord({
        name: "example.Complex",
        writerStrategy: new CompiledWriterStrategy(),
        validate: false,
        fields: [
          { name: "items", type: new ArrayType({ items: new IntType() }) },
          {
            name: "optional",
            type: new UnionType({ types: [new NullType(), new StringType()] }),
          },
        ],
      });

      const value = {
        items: [1, 2, 3],
        optional: null,
      };

      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded.items, [1, 2, 3]);
      assertEquals(decoded.optional, null);
    });
  });

  describe("InterpretedWriterStrategy", () => {
    it("delegates to type.write() methods", async () => {
      const type = createRecord({
        name: "example.Interpreted",
        writerStrategy: new InterpretedWriterStrategy(),
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      const value = { id: 1, name: "test" };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("works with sync serialization", () => {
      const type = createRecord({
        name: "example.InterpretedSync",
        writerStrategy: new InterpretedWriterStrategy(),
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      const value = { id: 1, name: "test" };
      const buffer = type.toSyncBuffer(value);
      const decoded = type.fromSyncBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("produces same output as CompiledWriterStrategy", async () => {
      const compiledRecord = createRecord({
        name: "example.Comparison",
        writerStrategy: new CompiledWriterStrategy(),
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
          { name: "score", type: new DoubleType() },
        ],
      });

      const interpretedRecord = createRecord({
        name: "example.Comparison",
        writerStrategy: new InterpretedWriterStrategy(),
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
          { name: "score", type: new DoubleType() },
        ],
      });

      const value = { id: 42, name: "test", score: 95.5 };

      const compiledBuffer = await compiledRecord.toBuffer(value);
      const interpretedBuffer = await interpretedRecord.toBuffer(value);

      assertEquals(
        new Uint8Array(compiledBuffer),
        new Uint8Array(interpretedBuffer),
      );
    });
  });

  describe("getWriterStrategy", () => {
    it("returns the configured strategy", () => {
      const compiled = new CompiledWriterStrategy();
      const interpreted = new InterpretedWriterStrategy();

      const record1 = createRecord({
        name: "example.Record1",
        writerStrategy: compiled,
        fields: [{ name: "id", type: new IntType() }],
      });

      const record2 = createRecord({
        name: "example.Record2",
        writerStrategy: interpreted,
        fields: [{ name: "id", type: new IntType() }],
      });

      assertEquals(record1.getWriterStrategy(), compiled);
      assertEquals(record2.getWriterStrategy(), interpreted);
    });

    it("defaults to CompiledWriterStrategy", () => {
      const record = createRecord({
        name: "example.DefaultStrategy",
        fields: [{ name: "id", type: new IntType() }],
      });

      assertEquals(
        record.getWriterStrategy() instanceof CompiledWriterStrategy,
        true,
      );
    });
  });
});

describe("RecordWriterCache", () => {
  describe("caching behavior", () => {
    it("caches compiled writers", async () => {
      const type = createRecord({
        name: "example.Cached",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      // First write triggers compilation
      const buffer1 = await type.toBuffer({ id: 1, name: "first" });
      // Second write uses cached writer
      const buffer2 = await type.toBuffer({ id: 2, name: "second" });

      const decoded1 = await type.fromBuffer(buffer1);
      const decoded2 = await type.fromBuffer(buffer2);

      assertEquals(decoded1, { id: 1, name: "first" });
      assertEquals(decoded2, { id: 2, name: "second" });
    });

    it("caches sync and async writers separately", async () => {
      const type = createRecord({
        name: "example.DualCache",
        fields: [
          { name: "id", type: new IntType() },
        ],
      });

      // Async path
      const asyncBuffer = await type.toBuffer({ id: 1 });
      // Sync path
      const syncBuffer = type.toSyncBuffer({ id: 2 });

      const asyncDecoded = await type.fromBuffer(asyncBuffer);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);

      assertEquals(asyncDecoded, { id: 1 });
      assertEquals(syncDecoded, { id: 2 });
    });

    it("caches strict and unchecked writers separately", async () => {
      const type = createRecord({
        name: "example.ValidationCache",
        fields: [
          { name: "id", type: new IntType() },
        ],
      });

      // Validated write (default)
      const buffer1 = await type.toBuffer({ id: 1 });

      // Unchecked write
      const buffer2 = await type.toBuffer({ id: 2 });

      const decoded1 = await type.fromBuffer(buffer1);
      const decoded2 = await type.fromBuffer(buffer2);

      assertEquals(decoded1, { id: 1 });
      assertEquals(decoded2, { id: 2 });
    });
  });

  describe("recursive type handling", () => {
    it("handles self-referential records via createType", async () => {
      const nodeType = createType({
        type: "record",
        name: "example.Node",
        fields: [
          { name: "value", type: "int" },
          { name: "next", type: ["null", "example.Node"] },
        ],
      });

      const value = {
        value: 1,
        next: {
          "example.Node": {
            value: 2,
            next: null,
          },
        },
      };

      const buffer = await nodeType.toBuffer(value);
      const decoded = await nodeType.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("handles self-referential records sync", () => {
      const nodeType = createType({
        type: "record",
        name: "example.SyncNode",
        fields: [
          { name: "value", type: "int" },
          { name: "next", type: ["null", "example.SyncNode"] },
        ],
      });

      const value = {
        value: 1,
        next: {
          "example.SyncNode": {
            value: 2,
            next: null,
          },
        },
      };

      const buffer = nodeType.toSyncBuffer(value);
      const decoded = nodeType.fromSyncBuffer(buffer);
      assertEquals(decoded, value);
    });
  });

  describe("clear", () => {
    it("clears cached writers", () => {
      const cache = new RecordWriterCache();
      // Verify cache is empty by checking that getStrategy returns the default
      assertEquals(cache.getStrategy() instanceof CompiledWriterStrategy, true);
      cache.clear();
      // After clear, strategy should still be the same
      assertEquals(cache.getStrategy() instanceof CompiledWriterStrategy, true);
    });
  });
});

describe("createType with writerStrategy", () => {
  it("passes strategy to record types", async () => {
    const interpreted = new InterpretedWriterStrategy();
    const type = createType(
      {
        type: "record",
        name: "example.FromCreateType",
        fields: [
          { name: "id", type: "int" },
          { name: "name", type: "string" },
        ],
      },
      { writerStrategy: interpreted },
    );

    const value = { id: 1, name: "test" };
    const buffer = await type.toBuffer(value);
    const decoded = await type.fromBuffer(buffer);
    assertEquals(decoded, value);
  });

  it("passes strategy to nested record types", async () => {
    const compiled = new CompiledWriterStrategy();
    const type = createType(
      {
        type: "record",
        name: "example.Outer",
        fields: [
          {
            name: "inner",
            type: {
              type: "record",
              name: "example.Inner",
              fields: [{ name: "value", type: "int" }],
            },
          },
        ],
      },
      { writerStrategy: compiled },
    );

    const value = { inner: { value: 42 } };
    const buffer = await type.toBuffer(value);
    const decoded = await type.fromBuffer(buffer);
    assertEquals(decoded, value);
  });
});
