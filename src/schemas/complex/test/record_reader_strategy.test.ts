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
import { ArrayType } from "../array_type.ts";
import {
  type CompiledReader,
  CompiledReaderStrategy,
  type CompiledSyncReader,
  defaultReaderStrategy,
  InterpretedReaderStrategy,
  type RecordReaderContext,
} from "../record_reader_strategy.ts";
import { createRecord } from "./record_test_utils.ts";
import { createType } from "../../../type/create_type.ts";
import { TestTap } from "../../../serialization/test/test_tap.ts";
import {
  SyncReadableTap,
  type SyncReadableTapLike,
  SyncWritableTap,
} from "../../../serialization/tap_sync.ts";
import type { ReadableTapLike } from "../../../serialization/tap.ts";

describe("RecordReaderStrategy", () => {
  describe("CompiledReaderStrategy", () => {
    it("is the default strategy", () => {
      assertEquals(
        defaultReaderStrategy instanceof CompiledReaderStrategy,
        true,
      );
    });

    it("inlines primitive type reads for all primitive types", async () => {
      const type = createRecord({
        name: "example.AllPrimitives",
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

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded.nullField, null);
      assertEquals(syncDecoded.boolField, true);
      assertEquals(syncDecoded.intField, 42);
      assertEquals(syncDecoded.longField, 100n);
    });

    it("handles complex types via read fallback", async () => {
      const type = createRecord({
        name: "example.Complex",
        fields: [
          { name: "items", type: new ArrayType({ items: new IntType() }) },
        ],
      });

      const value = {
        items: [1, 2, 3],
      };

      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded.items, [1, 2, 3]);

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded.items, [1, 2, 3]);
    });

    it("handles nested record types", async () => {
      const type = createType({
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
      });

      const value = { inner: { value: 42 } };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    describe("specialized sync readers for different field counts", () => {
      it("handles 0-field records", () => {
        const type = createRecord({
          name: "example.Empty",
          fields: [],
        });

        const buffer = type.toSyncBuffer({});
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, {});
      });

      it("handles 1-field records", () => {
        const type = createRecord({
          name: "example.OneField",
          fields: [{ name: "a", type: new IntType() }],
        });

        const value = { a: 1 };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles 2-field records", () => {
        const type = createRecord({
          name: "example.TwoFields",
          fields: [
            { name: "a", type: new IntType() },
            { name: "b", type: new IntType() },
          ],
        });

        const value = { a: 1, b: 2 };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles 3-field records", () => {
        const type = createRecord({
          name: "example.ThreeFields",
          fields: [
            { name: "a", type: new IntType() },
            { name: "b", type: new IntType() },
            { name: "c", type: new IntType() },
          ],
        });

        const value = { a: 1, b: 2, c: 3 };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles 4-field records", () => {
        const type = createRecord({
          name: "example.FourFields",
          fields: [
            { name: "a", type: new IntType() },
            { name: "b", type: new IntType() },
            { name: "c", type: new IntType() },
            { name: "d", type: new IntType() },
          ],
        });

        const value = { a: 1, b: 2, c: 3, d: 4 };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles 5-field records", () => {
        const type = createRecord({
          name: "example.FiveFields",
          fields: [
            { name: "a", type: new IntType() },
            { name: "b", type: new IntType() },
            { name: "c", type: new IntType() },
            { name: "d", type: new IntType() },
            { name: "e", type: new IntType() },
          ],
        });

        const value = { a: 1, b: 2, c: 3, d: 4, e: 5 };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles 6-field records", () => {
        const type = createRecord({
          name: "example.SixFields",
          fields: [
            { name: "a", type: new IntType() },
            { name: "b", type: new IntType() },
            { name: "c", type: new IntType() },
            { name: "d", type: new IntType() },
            { name: "e", type: new IntType() },
            { name: "f", type: new IntType() },
          ],
        });

        const value = { a: 1, b: 2, c: 3, d: 4, e: 5, f: 6 };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles 7-field records", () => {
        const type = createRecord({
          name: "example.SevenFields",
          fields: [
            { name: "a", type: new IntType() },
            { name: "b", type: new IntType() },
            { name: "c", type: new IntType() },
            { name: "d", type: new IntType() },
            { name: "e", type: new IntType() },
            { name: "f", type: new IntType() },
            { name: "g", type: new IntType() },
          ],
        });

        const value = { a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7 };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles 8-field records", () => {
        const type = createRecord({
          name: "example.EightFields",
          fields: [
            { name: "a", type: new IntType() },
            { name: "b", type: new IntType() },
            { name: "c", type: new IntType() },
            { name: "d", type: new IntType() },
            { name: "e", type: new IntType() },
            { name: "f", type: new IntType() },
            { name: "g", type: new IntType() },
            { name: "h", type: new IntType() },
          ],
        });

        const value = { a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8 };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles 9-field records", () => {
        const type = createRecord({
          name: "example.NineFields",
          fields: [
            { name: "a", type: new IntType() },
            { name: "b", type: new IntType() },
            { name: "c", type: new IntType() },
            { name: "d", type: new IntType() },
            { name: "e", type: new IntType() },
            { name: "f", type: new IntType() },
            { name: "g", type: new IntType() },
            { name: "h", type: new IntType() },
            { name: "i", type: new IntType() },
          ],
        });

        const value = {
          a: 1,
          b: 2,
          c: 3,
          d: 4,
          e: 5,
          f: 6,
          g: 7,
          h: 8,
          i: 9,
        };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles 10-field records", () => {
        const type = createRecord({
          name: "example.TenFields",
          fields: [
            { name: "a", type: new IntType() },
            { name: "b", type: new IntType() },
            { name: "c", type: new IntType() },
            { name: "d", type: new IntType() },
            { name: "e", type: new IntType() },
            { name: "f", type: new IntType() },
            { name: "g", type: new IntType() },
            { name: "h", type: new IntType() },
            { name: "i", type: new IntType() },
            { name: "j", type: new IntType() },
          ],
        });

        const value = {
          a: 1,
          b: 2,
          c: 3,
          d: 4,
          e: 5,
          f: 6,
          g: 7,
          h: 8,
          i: 9,
          j: 10,
        };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles 11-field records (fallback to loop)", () => {
        const type = createRecord({
          name: "example.ElevenFields",
          fields: [
            { name: "a", type: new IntType() },
            { name: "b", type: new IntType() },
            { name: "c", type: new IntType() },
            { name: "d", type: new IntType() },
            { name: "e", type: new IntType() },
            { name: "f", type: new IntType() },
            { name: "g", type: new IntType() },
            { name: "h", type: new IntType() },
            { name: "i", type: new IntType() },
            { name: "j", type: new IntType() },
            { name: "k", type: new IntType() },
          ],
        });

        const value = {
          a: 1,
          b: 2,
          c: 3,
          d: 4,
          e: 5,
          f: 6,
          g: 7,
          h: 8,
          i: 9,
          j: 10,
          k: 11,
        };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });

      it("handles large records (>10 fields fallback)", () => {
        const type = createRecord({
          name: "example.LargeRecord",
          fields: [
            { name: "f1", type: new IntType() },
            { name: "f2", type: new IntType() },
            { name: "f3", type: new IntType() },
            { name: "f4", type: new IntType() },
            { name: "f5", type: new IntType() },
            { name: "f6", type: new IntType() },
            { name: "f7", type: new IntType() },
            { name: "f8", type: new IntType() },
            { name: "f9", type: new IntType() },
            { name: "f10", type: new IntType() },
            { name: "f11", type: new IntType() },
            { name: "f12", type: new IntType() },
            { name: "f13", type: new IntType() },
            { name: "f14", type: new IntType() },
            { name: "f15", type: new IntType() },
          ],
        });

        const value = {
          f1: 1,
          f2: 2,
          f3: 3,
          f4: 4,
          f5: 5,
          f6: 6,
          f7: 7,
          f8: 8,
          f9: 9,
          f10: 10,
          f11: 11,
          f12: 12,
          f13: 13,
          f14: 14,
          f15: 15,
        };
        const buffer = type.toSyncBuffer(value);
        const decoded = type.fromSyncBuffer(buffer);
        assertEquals(decoded, value);
      });
    });

    describe("compileFieldReader direct API", () => {
      it("compiles field readers for primitives", async () => {
        const strategy = new CompiledReaderStrategy();
        const intType = new IntType();

        const reader = strategy.compileFieldReader(intType, () => {
          throw new Error("Should not be called for primitives");
        });

        const buffer = new ArrayBuffer(16);
        const tap = new TestTap(buffer);
        await tap.writeInt(42);
        tap._testOnlyResetPos();
        const result = await reader(tap);
        assertEquals(result, 42);
      });

      it("uses getRecordReader for nested record types", async () => {
        const strategy = new CompiledReaderStrategy();
        const nestedType = createType({
          type: "record",
          name: "example.Nested",
          fields: [{ name: "value", type: "int" }],
        });

        let getRecordReaderCalled = false;
        const reader = strategy.compileFieldReader(nestedType, (type) => {
          getRecordReaderCalled = true;
          assertEquals(type, nestedType);
          return async (tap: ReadableTapLike) => {
            const v = await tap.readInt();
            return { value: v };
          };
        });

        assertEquals(getRecordReaderCalled, true);

        const buffer = new ArrayBuffer(16);
        const tap = new TestTap(buffer);
        await tap.writeInt(99);
        tap._testOnlyResetPos();
        const result = await reader(tap);
        assertEquals(result, { value: 99 });
      });

      it("compiles sync field readers for primitives", () => {
        const strategy = new CompiledReaderStrategy();
        const intType = new IntType();

        const reader = strategy.compileSyncFieldReader(intType, () => {
          throw new Error("Should not be called for primitives");
        });

        const buffer = new ArrayBuffer(16);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeInt(42);
        const readTap = new SyncReadableTap(buffer);
        const result = reader(readTap);
        assertEquals(result, 42);
      });

      it("uses getRecordReader for nested record types (sync)", () => {
        const strategy = new CompiledReaderStrategy();
        const nestedType = createType({
          type: "record",
          name: "example.NestedSync",
          fields: [{ name: "value", type: "int" }],
        });

        let getRecordReaderCalled = false;
        const reader = strategy.compileSyncFieldReader(nestedType, (type) => {
          getRecordReaderCalled = true;
          assertEquals(type, nestedType);
          return (tap: SyncReadableTapLike) => {
            const v = tap.readInt();
            return { value: v };
          };
        });

        assertEquals(getRecordReaderCalled, true);

        const buffer = new ArrayBuffer(16);
        const writeTap = new SyncWritableTap(buffer);
        writeTap.writeInt(88);
        const readTap = new SyncReadableTap(buffer);
        const result = reader(readTap);
        assertEquals(result, { value: 88 });
      });
    });

    describe("assembleRecordReader direct API", () => {
      it("assembles async record reader", async () => {
        const strategy = new CompiledReaderStrategy();
        const context: RecordReaderContext = {
          fieldNames: ["id", "name"],
          fieldTypes: [new IntType(), new StringType()],
        };

        const fieldReaders: CompiledReader[] = [
          async (tap: ReadableTapLike) => await tap.readInt(),
          async (tap: ReadableTapLike) => await tap.readString(),
        ];

        const reader = strategy.assembleRecordReader(context, fieldReaders);

        const buffer = new ArrayBuffer(64);
        const tap = new TestTap(buffer);
        await tap.writeInt(42);
        await tap.writeString("test");
        tap._testOnlyResetPos();

        const result = await reader(tap);
        assertEquals(result, { id: 42, name: "test" });
      });

      it("assembles sync record reader with loop fallback for >10 fields", () => {
        const strategy = new CompiledReaderStrategy();
        const fieldNames = Array.from({ length: 12 }, (_, i) => `f${i}`);
        const fieldTypes = Array.from({ length: 12 }, () => new IntType());
        const context: RecordReaderContext = { fieldNames, fieldTypes };

        const fieldReaders: CompiledSyncReader[] = fieldNames.map(
          (_, i) => (tap: SyncReadableTapLike) => tap.readInt() + i * 100,
        );

        const reader = strategy.assembleSyncRecordReader(context, fieldReaders);

        const buffer = new ArrayBuffer(128);
        const writeTap = new SyncWritableTap(buffer);
        for (let i = 0; i < 12; i++) {
          writeTap.writeInt(i);
        }
        const readTap = new SyncReadableTap(buffer);

        const result = reader(readTap) as Record<string, number>;
        for (let i = 0; i < 12; i++) {
          assertEquals(result[`f${i}`], i + i * 100);
        }
      });
    });
  });

  describe("InterpretedReaderStrategy", () => {
    it("delegates to type.read() methods", async () => {
      const strategy = new InterpretedReaderStrategy();
      const intType = new IntType();

      const reader = strategy.compileFieldReader(intType, () => {
        throw new Error("Should not be called");
      });

      const buffer = new ArrayBuffer(16);
      const tap = new TestTap(buffer);
      await tap.writeInt(42);
      tap._testOnlyResetPos();
      const result = await reader(tap);
      assertEquals(result, 42);
    });

    it("delegates to type.readSync() methods", () => {
      const strategy = new InterpretedReaderStrategy();
      const intType = new IntType();

      const reader = strategy.compileSyncFieldReader(intType, () => {
        throw new Error("Should not be called");
      });

      const buffer = new ArrayBuffer(16);
      const writeTap = new SyncWritableTap(buffer);
      writeTap.writeInt(42);
      const readTap = new SyncReadableTap(buffer);
      const result = reader(readTap);
      assertEquals(result, 42);
    });

    it("assembles async record reader", async () => {
      const strategy = new InterpretedReaderStrategy();
      const context: RecordReaderContext = {
        fieldNames: ["id", "name"],
        fieldTypes: [new IntType(), new StringType()],
      };

      const fieldReaders: CompiledReader[] = [
        async (tap: ReadableTapLike) => await tap.readInt(),
        async (tap: ReadableTapLike) => await tap.readString(),
      ];

      const reader = strategy.assembleRecordReader(context, fieldReaders);

      const buffer = new ArrayBuffer(64);
      const tap = new TestTap(buffer);
      await tap.writeInt(42);
      await tap.writeString("test");
      tap._testOnlyResetPos();

      const result = await reader(tap);
      assertEquals(result, { id: 42, name: "test" });
    });

    it("assembles sync record reader", () => {
      const strategy = new InterpretedReaderStrategy();
      const context: RecordReaderContext = {
        fieldNames: ["id", "name"],
        fieldTypes: [new IntType(), new StringType()],
      };

      const fieldReaders: CompiledSyncReader[] = [
        (tap: SyncReadableTapLike) => tap.readInt(),
        (tap: SyncReadableTapLike) => tap.readString(),
      ];

      const reader = strategy.assembleSyncRecordReader(context, fieldReaders);

      const buffer = new ArrayBuffer(64);
      const writeTap = new SyncWritableTap(buffer);
      writeTap.writeInt(55);
      writeTap.writeString("hello");
      const readTap = new SyncReadableTap(buffer);

      const result = reader(readTap);
      assertEquals(result, { id: 55, name: "hello" });
    });

    it("works with records created via createRecord", async () => {
      const type = createRecord({
        name: "example.InterpretedRecord",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      const value = { id: 1, name: "test" };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    it("produces same output as CompiledReaderStrategy", async () => {
      const type = createRecord({
        name: "example.ComparisonRecord",
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
          { name: "score", type: new DoubleType() },
        ],
      });

      const value = { id: 42, name: "test", score: 95.5 };

      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });
  });

  describe("recursive types", () => {
    it("handles self-referential records", async () => {
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
});
