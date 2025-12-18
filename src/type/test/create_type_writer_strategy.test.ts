import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { createType } from "../create_type.ts";
import {
  type CompiledSyncWriter,
  type CompiledWriter,
  CompiledWriterStrategy,
  InterpretedWriterStrategy,
  type RecordWriterContext,
  type RecordWriterStrategy,
} from "../../schemas/complex/record_writer_strategy.ts";
import type { RecordType } from "../../schemas/complex/record_type.ts";
import type { Type } from "../../schemas/type.ts";
import type { WritableTapLike } from "../../serialization/tap.ts";
import type { SyncWritableTapLike } from "../../serialization/sync_tap.ts";
import { CountingWritableTap } from "../../serialization/counting_writable_tap.ts";
import { SyncCountingWritableTap } from "../../serialization/sync_counting_writable_tap.ts";

describe("createType writerStrategy option", () => {
  describe("CompiledWriterStrategy (default)", () => {
    it("uses CompiledWriterStrategy by default", () => {
      const type = createType({
        type: "record",
        name: "DefaultStrategy",
        fields: [{ name: "id", type: "int" }],
      }) as RecordType;

      assertEquals(
        type.getWriterStrategy() instanceof CompiledWriterStrategy,
        true,
      );
    });

    it("round-trips all primitive types with compiled strategy", async () => {
      const type = createType(
        {
          type: "record",
          name: "AllPrimitives",
          fields: [
            { name: "nullField", type: "null" },
            { name: "boolField", type: "boolean" },
            { name: "intField", type: "int" },
            { name: "longField", type: "long" },
            { name: "floatField", type: "float" },
            { name: "doubleField", type: "double" },
            { name: "bytesField", type: "bytes" },
            { name: "stringField", type: "string" },
          ],
        },
        { writerStrategy: new CompiledWriterStrategy() },
      );

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

      // Test async path
      const asyncBuffer = await type.toBuffer(value);
      const asyncDecoded = (await type.fromBuffer(asyncBuffer)) as Record<
        string,
        unknown
      >;
      assertEquals(asyncDecoded.nullField, null);
      assertEquals(asyncDecoded.boolField, true);
      assertEquals(asyncDecoded.intField, 42);
      assertEquals(asyncDecoded.longField, 100n);
      assertEquals(typeof asyncDecoded.floatField, "number");
      assertEquals(asyncDecoded.doubleField, 2.718281828);
      assertEquals([...(asyncDecoded.bytesField as Uint8Array)], [1, 2, 3]);
      assertEquals(asyncDecoded.stringField, "hello");

      // Test sync path
      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer) as Record<
        string,
        unknown
      >;
      assertEquals(syncDecoded.nullField, null);
      assertEquals(syncDecoded.boolField, true);
      assertEquals(syncDecoded.intField, 42);
    });

    it("handles unchecked writes with all primitive types", async () => {
      const type = createType(
        {
          type: "record",
          name: "UncheckedPrimitives",
          fields: [
            { name: "nullField", type: "null" },
            { name: "boolField", type: "boolean" },
            { name: "intField", type: "int" },
            { name: "longField", type: "long" },
            { name: "floatField", type: "float" },
            { name: "doubleField", type: "double" },
            { name: "bytesField", type: "bytes" },
            { name: "stringField", type: "string" },
          ],
        },
        { writerStrategy: new CompiledWriterStrategy(), validate: false },
      );

      const value = {
        nullField: null,
        boolField: false,
        intField: -1,
        longField: -100n,
        floatField: -3.14,
        doubleField: -2.718281828,
        bytesField: new Uint8Array([4, 5, 6]),
        stringField: "world",
      };

      // Async unchecked
      const asyncBuffer = await type.toBuffer(value);
      const asyncDecoded = (await type.fromBuffer(asyncBuffer)) as Record<
        string,
        unknown
      >;
      assertEquals(asyncDecoded.intField, -1);

      // Sync unchecked
      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer) as Record<
        string,
        unknown
      >;
      assertEquals(syncDecoded.intField, -1);
    });

    it("uses fallback writeUnchecked for complex types", async () => {
      const type = createType(
        {
          type: "record",
          name: "ComplexFields",
          fields: [
            { name: "items", type: { type: "array", items: "int" } },
            { name: "mapping", type: { type: "map", values: "string" } },
            { name: "optional", type: ["null", "int"] },
          ],
        },
        { writerStrategy: new CompiledWriterStrategy(), validate: false },
      );

      const value = {
        items: [1, 2, 3],
        mapping: new Map([["a", "one"], ["b", "two"]]),
        optional: null,
      };

      const buffer = await type.toBuffer(value);
      const decoded = (await type.fromBuffer(buffer)) as Record<
        string,
        unknown
      >;
      assertEquals(decoded.items, [1, 2, 3]);
      assertEquals(
        (decoded.mapping as Map<string, string>).get("a"),
        "one",
      );
      assertEquals(
        (decoded.mapping as Map<string, string>).get("b"),
        "two",
      );
      assertEquals(decoded.optional, null);

      // Also test sync
      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer) as Record<
        string,
        unknown
      >;
      assertEquals(syncDecoded.items, [1, 2, 3]);
    });
  });

  describe("InterpretedWriterStrategy", () => {
    it("can be explicitly specified", () => {
      const strategy = new InterpretedWriterStrategy();
      const type = createType(
        {
          type: "record",
          name: "InterpretedRecord",
          fields: [{ name: "id", type: "int" }],
        },
        { writerStrategy: strategy },
      ) as RecordType;

      assertEquals(type.getWriterStrategy(), strategy);
    });

    it("round-trips values with interpreted strategy (validated)", async () => {
      const type = createType(
        {
          type: "record",
          name: "InterpretedValidated",
          fields: [
            { name: "id", type: "int" },
            { name: "name", type: "string" },
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy() },
      );

      const value = { id: 42, name: "test" };

      // Async
      const asyncBuffer = await type.toBuffer(value);
      const asyncDecoded = await type.fromBuffer(asyncBuffer);
      assertEquals(asyncDecoded, value);

      // Sync
      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    it("round-trips values with interpreted strategy (unchecked)", async () => {
      const type = createType(
        {
          type: "record",
          name: "InterpretedUnchecked",
          fields: [
            { name: "id", type: "int" },
            { name: "name", type: "string" },
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: false },
      );

      const value = { id: 42, name: "test" };

      // Async unchecked path
      const asyncBuffer = await type.toBuffer(value);
      const asyncDecoded = await type.fromBuffer(asyncBuffer);
      assertEquals(asyncDecoded, value);

      // Sync unchecked path
      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    it("produces identical output to CompiledWriterStrategy", async () => {
      const compiledType = createType(
        {
          type: "record",
          name: "ComparisonRecord",
          fields: [
            { name: "id", type: "int" },
            { name: "name", type: "string" },
            { name: "score", type: "double" },
          ],
        },
        { writerStrategy: new CompiledWriterStrategy() },
      );

      const interpretedType = createType(
        {
          type: "record",
          name: "ComparisonRecord2",
          fields: [
            { name: "id", type: "int" },
            { name: "name", type: "string" },
            { name: "score", type: "double" },
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy() },
      );

      const value = { id: 42, name: "test", score: 95.5 };

      const compiledBuffer = await compiledType.toBuffer(value);
      const interpretedBuffer = await interpretedType.toBuffer(value);

      assertEquals(
        new Uint8Array(compiledBuffer),
        new Uint8Array(interpretedBuffer),
      );

      // Also check sync
      const compiledSyncBuffer = compiledType.toSyncBuffer(value);
      const interpretedSyncBuffer = interpretedType.toSyncBuffer(value);

      assertEquals(
        new Uint8Array(compiledSyncBuffer),
        new Uint8Array(interpretedSyncBuffer),
      );
    });
  });

  describe("strategy propagation to nested records", () => {
    it("propagates strategy to nested record types", async () => {
      const strategy = new InterpretedWriterStrategy();
      const type = createType(
        {
          type: "record",
          name: "OuterRecord",
          fields: [
            {
              name: "inner",
              type: {
                type: "record",
                name: "InnerRecord",
                fields: [{ name: "value", type: "int" }],
              },
            },
          ],
        },
        { writerStrategy: strategy },
      ) as RecordType;

      // Outer has the strategy
      assertEquals(type.getWriterStrategy(), strategy);

      // Inner should have the same strategy
      const innerType = type.getField("inner")?.getType() as RecordType;
      assertEquals(innerType.getWriterStrategy(), strategy);

      // Verify it works
      const value = { inner: { value: 42 } };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("propagates strategy through deeply nested records", async () => {
      const strategy = new CompiledWriterStrategy();
      const type = createType(
        {
          type: "record",
          name: "Level1",
          fields: [
            {
              name: "level2",
              type: {
                type: "record",
                name: "Level2",
                fields: [
                  {
                    name: "level3",
                    type: {
                      type: "record",
                      name: "Level3",
                      fields: [{ name: "value", type: "string" }],
                    },
                  },
                ],
              },
            },
          ],
        },
        { writerStrategy: strategy },
      ) as RecordType;

      const level2 = type.getField("level2")?.getType() as RecordType;
      const level3 = level2.getField("level3")?.getType() as RecordType;

      assertEquals(type.getWriterStrategy(), strategy);
      assertEquals(level2.getWriterStrategy(), strategy);
      assertEquals(level3.getWriterStrategy(), strategy);

      const value = { level2: { level3: { value: "deep" } } };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("propagates strategy to records within unions", async () => {
      const strategy = new InterpretedWriterStrategy();
      const type = createType(
        {
          type: "record",
          name: "UnionContainer",
          fields: [
            {
              name: "data",
              type: [
                "null",
                {
                  type: "record",
                  name: "NestedInUnion",
                  fields: [{ name: "id", type: "int" }],
                },
              ],
            },
          ],
        },
        { writerStrategy: strategy },
      ) as RecordType;

      assertEquals(type.getWriterStrategy(), strategy);

      const value = { data: { NestedInUnion: { id: 123 } } };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("propagates strategy to records within arrays", async () => {
      const strategy = new CompiledWriterStrategy();
      const type = createType(
        {
          type: "record",
          name: "ArrayContainer",
          fields: [
            {
              name: "items",
              type: {
                type: "array",
                items: {
                  type: "record",
                  name: "ArrayItem",
                  fields: [{ name: "value", type: "int" }],
                },
              },
            },
          ],
        },
        { writerStrategy: strategy },
      ) as RecordType;

      assertEquals(type.getWriterStrategy(), strategy);

      const value = { items: [{ value: 1 }, { value: 2 }] };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("propagates strategy to records within maps", async () => {
      const strategy = new InterpretedWriterStrategy();
      const type = createType(
        {
          type: "record",
          name: "MapContainer",
          fields: [
            {
              name: "mapping",
              type: {
                type: "map",
                values: {
                  type: "record",
                  name: "MapValue",
                  fields: [{ name: "data", type: "string" }],
                },
              },
            },
          ],
        },
        { writerStrategy: strategy },
      ) as RecordType;

      assertEquals(type.getWriterStrategy(), strategy);

      const value = { mapping: new Map([["key1", { data: "value1" }]]) };
      const buffer = await type.toBuffer(value);
      const decoded = (await type.fromBuffer(buffer)) as {
        mapping: Map<string, { data: string }>;
      };
      assertEquals(decoded.mapping.get("key1")?.data, "value1");
    });
  });

  describe("strategy with field defaults", () => {
    it("handles field defaults with compiled strategy", async () => {
      const type = createType(
        {
          type: "record",
          name: "WithDefaults",
          fields: [
            { name: "required", type: "int" },
            { name: "optional", type: "string", default: "default_value" },
          ],
        },
        { writerStrategy: new CompiledWriterStrategy() },
      );

      const value = { required: 42 };
      const buffer = await type.toBuffer(value);
      const decoded = (await type.fromBuffer(buffer)) as {
        required: number;
        optional: string;
      };
      assertEquals(decoded.required, 42);
      assertEquals(decoded.optional, "default_value");

      // Also sync
      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer) as {
        required: number;
        optional: string;
      };
      assertEquals(syncDecoded.required, 42);
      assertEquals(syncDecoded.optional, "default_value");
    });

    it("handles field defaults with interpreted strategy", async () => {
      const type = createType(
        {
          type: "record",
          name: "WithDefaultsInterpreted",
          fields: [
            { name: "required", type: "int" },
            { name: "optional", type: "int", default: 100 },
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy() },
      );

      const value = { required: 42 };
      const buffer = await type.toBuffer(value);
      const decoded = (await type.fromBuffer(buffer)) as {
        required: number;
        optional: number;
      };
      assertEquals(decoded.required, 42);
      assertEquals(decoded.optional, 100);

      // Also sync
      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer) as {
        required: number;
        optional: number;
      };
      assertEquals(syncDecoded.required, 42);
      assertEquals(syncDecoded.optional, 100);
    });

    it("handles nullable defaults with compiled unchecked", async () => {
      const type = createType(
        {
          type: "record",
          name: "NullableDefaults",
          fields: [
            { name: "id", type: "int" },
            { name: "optional", type: ["null", "string"], default: null },
          ],
        },
        { writerStrategy: new CompiledWriterStrategy(), validate: false },
      );

      const value = { id: 1 };
      const buffer = await type.toBuffer(value);
      const decoded = (await type.fromBuffer(buffer)) as {
        id: number;
        optional: null;
      };
      assertEquals(decoded.id, 1);
      assertEquals(decoded.optional, null);
    });

    it("handles nullable defaults with interpreted unchecked", async () => {
      const type = createType(
        {
          type: "record",
          name: "NullableDefaultsInterpreted",
          fields: [
            { name: "id", type: "int" },
            { name: "optional", type: ["null", "int"], default: null },
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: false },
      );

      const value = { id: 1 };
      const buffer = await type.toBuffer(value);
      const decoded = (await type.fromBuffer(buffer)) as {
        id: number;
        optional: null;
      };
      assertEquals(decoded.id, 1);
      assertEquals(decoded.optional, null);
    });
  });

  describe("strategy with recursive types", () => {
    it("handles self-referential records with compiled strategy", async () => {
      const type = createType(
        {
          type: "record",
          name: "RecursiveNode",
          fields: [
            { name: "value", type: "int" },
            { name: "next", type: ["null", "RecursiveNode"] },
          ],
        },
        { writerStrategy: new CompiledWriterStrategy() },
      );

      const value = {
        value: 1,
        next: { RecursiveNode: { value: 2, next: null } },
      };

      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);

      // Sync too
      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    it("handles self-referential records with interpreted strategy", async () => {
      const type = createType(
        {
          type: "record",
          name: "RecursiveNodeInterpreted",
          fields: [
            { name: "value", type: "int" },
            { name: "next", type: ["null", "RecursiveNodeInterpreted"] },
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy() },
      );

      const value = {
        value: 1,
        next: { RecursiveNodeInterpreted: { value: 2, next: null } },
      };

      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });
  });

  describe("InterpretedWriterStrategy validation errors", () => {
    it("throws when writing non-record value (async)", async () => {
      const type = createType(
        {
          type: "record",
          name: "ValidatedRecord",
          fields: [{ name: "id", type: "int" }],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: true },
      );

      try {
        await type.toBuffer(
          "not a record" as unknown as Record<string, unknown>,
        );
        throw new Error("Expected validation error");
      } catch (e) {
        assertEquals((e as Error).name, "ValidationError");
      }
    });

    it("throws when writing non-record value (sync)", () => {
      const type = createType(
        {
          type: "record",
          name: "ValidatedRecordSync",
          fields: [{ name: "id", type: "int" }],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: true },
      );

      try {
        type.toSyncBuffer("not a record" as unknown as Record<string, unknown>);
        throw new Error("Expected validation error");
      } catch (e) {
        assertEquals((e as Error).name, "ValidationError");
      }
    });

    it("throws when missing required field without default (async)", async () => {
      const type = createType(
        {
          type: "record",
          name: "RequiredFieldRecord",
          fields: [
            { name: "id", type: "int" },
            { name: "required", type: "string" },
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: true },
      );

      try {
        await type.toBuffer({ id: 1 });
        throw new Error("Expected validation error");
      } catch (e) {
        assertEquals((e as Error).name, "ValidationError");
      }
    });

    it("throws when missing required field without default (sync)", () => {
      const type = createType(
        {
          type: "record",
          name: "RequiredFieldRecordSync",
          fields: [
            { name: "id", type: "int" },
            { name: "required", type: "string" },
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: true },
      );

      try {
        type.toSyncBuffer({ id: 1 });
        throw new Error("Expected validation error");
      } catch (e) {
        assertEquals((e as Error).name, "ValidationError");
      }
    });
  });

  describe("custom strategy using getRecordWriter callback for non-RecordType", () => {
    /**
     * A custom strategy that always calls getRecordWriter for all field types,
     * including non-RecordTypes. This exercises the fallback path in
     * RecordType's compiled writer callbacks.
     */
    class CallbackForAllStrategy implements RecordWriterStrategy {
      compileFieldWriter(
        fieldType: Type,
        validate: boolean,
        getRecordWriter: (type: Type, validate: boolean) => CompiledWriter,
      ): CompiledWriter {
        // Always call getRecordWriter, even for non-RecordTypes
        return getRecordWriter(fieldType, validate);
      }

      compileSyncFieldWriter(
        fieldType: Type,
        validate: boolean,
        getRecordWriter: (type: Type, validate: boolean) => CompiledSyncWriter,
      ): CompiledSyncWriter {
        // Always call getRecordWriter, even for non-RecordTypes
        return getRecordWriter(fieldType, validate);
      }

      assembleRecordWriter(
        context: RecordWriterContext,
        fieldWriters: CompiledWriter[],
      ): CompiledWriter {
        // Use same implementation as CompiledWriterStrategy
        return new CompiledWriterStrategy().assembleRecordWriter(
          context,
          fieldWriters,
        );
      }

      assembleSyncRecordWriter(
        context: RecordWriterContext,
        fieldWriters: CompiledSyncWriter[],
      ): CompiledSyncWriter {
        // Use same implementation as CompiledWriterStrategy
        return new CompiledWriterStrategy().assembleSyncRecordWriter(
          context,
          fieldWriters,
        );
      }
    }

    it("exercises fallback for non-RecordType in async callback", async () => {
      const type = createType(
        {
          type: "record",
          name: "CallbackTest",
          fields: [{ name: "value", type: "int" }],
        },
        { writerStrategy: new CallbackForAllStrategy() },
      );

      const value = { value: 42 };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("respects validate=false for non-RecordType in async callback", async () => {
      const type = createType(
        {
          type: "record",
          name: "CallbackUncheckedAsync",
          fields: [{ name: "value", type: "float" }],
        },
        { writerStrategy: new CallbackForAllStrategy(), validate: false },
      );

      const buffer = await type.toBuffer({ value: "1.5" as unknown as number });
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, { value: 1.5 });
    });

    it("respects validate=true for non-RecordType in async callback", async () => {
      const type = createType(
        {
          type: "record",
          name: "CallbackCheckedAsync",
          fields: [{ name: "value", type: "float" }],
        },
        { writerStrategy: new CallbackForAllStrategy(), validate: true },
      );

      try {
        await type.toBuffer({ value: "1.5" as unknown as number });
        throw new Error("Expected validation error");
      } catch (e) {
        assertEquals((e as Error).name, "ValidationError");
      }
    });

    it("exercises fallback for non-RecordType in sync callback", () => {
      const type = createType(
        {
          type: "record",
          name: "CallbackTestSync",
          fields: [{ name: "value", type: "int" }],
        },
        { writerStrategy: new CallbackForAllStrategy() },
      );

      const value = { value: 42 };
      const buffer = type.toSyncBuffer(value);
      const decoded = type.fromSyncBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("respects validate=false for non-RecordType in sync callback", () => {
      const type = createType(
        {
          type: "record",
          name: "CallbackUncheckedSync",
          fields: [{ name: "value", type: "float" }],
        },
        { writerStrategy: new CallbackForAllStrategy(), validate: false },
      );

      const buffer = type.toSyncBuffer({ value: "1.5" as unknown as number });
      const decoded = type.fromSyncBuffer(buffer);
      assertEquals(decoded, { value: 1.5 });
    });

    it("respects validate=true for non-RecordType in sync callback", () => {
      const type = createType(
        {
          type: "record",
          name: "CallbackCheckedSync",
          fields: [{ name: "value", type: "float" }],
        },
        { writerStrategy: new CallbackForAllStrategy(), validate: true },
      );

      try {
        type.toSyncBuffer({ value: "1.5" as unknown as number });
        throw new Error("Expected validation error");
      } catch (e) {
        assertEquals((e as Error).name, "ValidationError");
      }
    });
  });

  describe("InterpretedWriterStrategy assembled writer validation (via write method)", () => {
    it("validates non-record value via write() async", async () => {
      // Create a type with InterpretedWriterStrategy
      const type = createType(
        {
          type: "record",
          name: "WriteMethodValidation",
          fields: [{ name: "id", type: "int" }],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: true },
      ) as RecordType;

      const tap = new CountingWritableTap();

      // Call write() directly with non-record - this should hit the validation
      // in assembleRecordWriter (line 353)
      try {
        await type.write(
          tap as unknown as WritableTapLike,
          "not a record" as unknown as Record<string, unknown>,
        );
        throw new Error("Expected validation error");
      } catch (e) {
        assertEquals((e as Error).name, "ValidationError");
      }
    });

    it("validates non-record value via writeSync()", () => {
      // Create a type with InterpretedWriterStrategy
      const type = createType(
        {
          type: "record",
          name: "WriteSyncMethodValidation",
          fields: [{ name: "id", type: "int" }],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: true },
      ) as RecordType;

      const tap = new SyncCountingWritableTap();

      // Call writeSync() directly with non-record - this should hit the validation
      // in assembleSyncRecordWriter (line 396)
      try {
        type.writeSync(
          tap as unknown as SyncWritableTapLike,
          "not a record" as unknown as Record<string, unknown>,
        );
        throw new Error("Expected validation error");
      } catch (e) {
        assertEquals((e as Error).name, "ValidationError");
      }
    });

    it("validates missing required field via write() async", async () => {
      // Create a type with InterpretedWriterStrategy and a required field
      const type = createType(
        {
          type: "record",
          name: "WriteMethodMissingField",
          fields: [
            { name: "id", type: "int" },
            { name: "required", type: "string" }, // No default
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: true },
      ) as RecordType;

      const tap = new CountingWritableTap();

      // Call write() directly missing the required field - this should hit the
      // validation in assembleRecordWriter (line 370)
      try {
        await type.write(
          tap as unknown as WritableTapLike,
          { id: 1 } as Record<string, unknown>,
        );
        throw new Error("Expected validation error");
      } catch (e) {
        assertEquals((e as Error).name, "ValidationError");
      }
    });

    it("validates missing required field via writeSync()", () => {
      // Create a type with InterpretedWriterStrategy and a required field
      const type = createType(
        {
          type: "record",
          name: "WriteSyncMethodMissingField",
          fields: [
            { name: "id", type: "int" },
            { name: "required", type: "string" }, // No default
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: true },
      ) as RecordType;

      const tap = new SyncCountingWritableTap();

      // Call writeSync() directly missing the required field - this should hit
      // the validation in assembleSyncRecordWriter (line 413)
      try {
        type.writeSync(
          tap as unknown as SyncWritableTapLike,
          { id: 1 } as Record<string, unknown>,
        );
        throw new Error("Expected validation error");
      } catch (e) {
        assertEquals((e as Error).name, "ValidationError");
      }
    });

    it("allows missing required field in unchecked mode via write() async", async () => {
      // Create a type with InterpretedWriterStrategy, validate: false
      // Use a "null" type field - null type's writeUnchecked accepts any value
      const type = createType(
        {
          type: "record",
          name: "WriteMethodMissingFieldUnchecked",
          fields: [
            { name: "id", type: "int" },
            // Note: We intentionally don't specify a default to trigger the
            // "no getter" path in assembleRecordWriter. The null type's
            // writeUnchecked doesn't validate the value, so passing undefined works.
            { name: "alwaysNull", type: "null" },
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: false },
      ) as RecordType;

      const tap = new CountingWritableTap();

      // Call write() directly missing the required field - this should NOT throw
      // in unchecked mode (line 370 - toWrite = undefined branch)
      await type.write(
        tap as unknown as WritableTapLike,
        { id: 1 } as Record<string, unknown>,
      );

      // If we got here without error, the test passes (the unchecked path was exercised)
    });

    it("allows missing required field in unchecked mode via writeSync()", () => {
      // Create a type with InterpretedWriterStrategy, validate: false
      // Use a "null" type field - null type's writeSyncUnchecked accepts any value
      const type = createType(
        {
          type: "record",
          name: "WriteSyncMethodMissingFieldUnchecked",
          fields: [
            { name: "id", type: "int" },
            // Note: We intentionally don't specify a default to trigger the
            // "no getter" path in assembleSyncRecordWriter. The null type's
            // writeSyncUnchecked doesn't validate the value, so passing undefined works.
            { name: "alwaysNull", type: "null" },
          ],
        },
        { writerStrategy: new InterpretedWriterStrategy(), validate: false },
      ) as RecordType;

      const tap = new SyncCountingWritableTap();

      // Call writeSync() directly missing the required field - this should NOT throw
      // in unchecked mode (line 413 - toWrite = undefined branch)
      type.writeSync(
        tap as unknown as SyncWritableTapLike,
        { id: 1 } as Record<string, unknown>,
      );

      // If we got here without error, the test passes (the unchecked path was exercised)
    });
  });
});
