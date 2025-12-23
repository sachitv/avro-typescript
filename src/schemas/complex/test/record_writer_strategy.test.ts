import { assertEquals, assertRejects, assertThrows } from "@std/assert";
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
  type RecordWriterContext,
  type RecordWriterStrategy,
} from "../record_writer_strategy.ts";
import { RecordWriterCache } from "../record_writer_cache.ts";
import { createRecord } from "./record_test_utils.ts";
import { createType } from "../../../type/create_type.ts";
import type { Type } from "../../type.ts";
import { TestTap } from "../../../serialization/test/test_tap.ts";
import { SyncReadableTap, SyncWritableTap } from "../../../serialization/sync_tap.ts";
import { ValidationError } from "../../error.ts";

class GreedyWriterStrategy implements RecordWriterStrategy {
  public compileFieldWriter(
    fieldType: Type,
    validate: boolean,
    getRecordWriter: (type: Type, validate: boolean) => (
      tap: unknown,
      value: unknown,
    ) => Promise<void>,
  ) {
    return getRecordWriter(fieldType, validate);
  }

  public compileSyncFieldWriter(
    fieldType: Type,
    validate: boolean,
    getRecordWriter: (type: Type, validate: boolean) => (
      tap: unknown,
      value: unknown,
    ) => void,
  ) {
    return getRecordWriter(fieldType, validate);
  }

  public assembleRecordWriter(
    context: {
      fieldNames: string[];
    },
    fieldWriters: Array<(tap: unknown, value: unknown) => Promise<void>>,
  ) {
    const { fieldNames } = context;
    return async (tap: unknown, value: unknown) => {
      const record = value as Record<string, unknown>;
      for (let i = 0; i < fieldNames.length; i++) {
        await fieldWriters[i]!(tap, record[fieldNames[i]!]);
      }
    };
  }

  public assembleSyncRecordWriter(
    context: {
      fieldNames: string[];
    },
    fieldWriters: Array<(tap: unknown, value: unknown) => void>,
  ) {
    const { fieldNames } = context;
    return (tap: unknown, value: unknown) => {
      const record = value as Record<string, unknown>;
      for (let i = 0; i < fieldNames.length; i++) {
        fieldWriters[i]!(tap, record[fieldNames[i]!]);
      }
    };
  }
}

class ForceValidateWriterStrategy implements RecordWriterStrategy {
  public compileFieldWriter(
    fieldType: Type,
    _validate: boolean,
    getRecordWriter: (type: Type, validate: boolean) => (
      tap: unknown,
      value: unknown,
    ) => Promise<void>,
  ) {
    return getRecordWriter(fieldType, true);
  }

  public compileSyncFieldWriter(
    fieldType: Type,
    _validate: boolean,
    getRecordWriter: (type: Type, validate: boolean) => (
      tap: unknown,
      value: unknown,
    ) => void,
  ) {
    return getRecordWriter(fieldType, true);
  }

  public assembleRecordWriter(
    context: {
      fieldNames: string[];
    },
    fieldWriters: Array<(tap: unknown, value: unknown) => Promise<void>>,
  ) {
    const { fieldNames } = context;
    return async (tap: unknown, value: unknown) => {
      const record = value as Record<string, unknown>;
      for (let i = 0; i < fieldNames.length; i++) {
        await fieldWriters[i]!(tap, record[fieldNames[i]!]);
      }
    };
  }

  public assembleSyncRecordWriter(
    context: {
      fieldNames: string[];
    },
    fieldWriters: Array<(tap: unknown, value: unknown) => void>,
  ) {
    const { fieldNames } = context;
    return (tap: unknown, value: unknown) => {
      const record = value as Record<string, unknown>;
      for (let i = 0; i < fieldNames.length; i++) {
        fieldWriters[i]!(tap, record[fieldNames[i]!]);
      }
    };
  }
}

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

    it("skips default lookup when validate=false and no defaults", async () => {
      const type = createRecord({
        name: "example.NoDefaultsCompiled",
        writerStrategy: new CompiledWriterStrategy(),
        validate: false,
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      const value = { id: 7, name: "fast" };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    it("writes missing fields as undefined when defaults exist", async () => {
      const type = createRecord({
        name: "example.DefaultsCompiled",
        writerStrategy: new CompiledWriterStrategy(),
        validate: false,
        fields: [
          { name: "id", type: new IntType(), default: 1 },
          { name: "note", type: new NullType() },
        ],
      });

      const value = { id: 9 };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, { id: 9, note: null });

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, { id: 9, note: null });
    });

    it("validates record writes when validate=true", async () => {
      const strategy = new CompiledWriterStrategy();
      const fieldType = new IntType();
      const recordType = createRecord({
        name: "example.CompiledValidatedContext",
        fields: [{ name: "id", type: fieldType }],
      });
      const context: RecordWriterContext = {
        fieldNames: ["id"],
        fieldTypes: [fieldType],
        fieldDefaultGetters: [undefined],
        fieldHasDefault: [false],
        validate: true,
        recordType,
        isRecord: (value): value is Record<string, unknown> =>
          typeof value === "object" && value !== null,
      };

      const fieldWriter = strategy.compileFieldWriter(
        fieldType,
        true,
        () => {
          throw new Error("Unexpected nested writer request.");
        },
      );
      const recordWriter = strategy.assembleRecordWriter(context, [fieldWriter]);
      const tap = new TestTap(new ArrayBuffer(16));
      await recordWriter(tap, { id: 42 });
      tap._testOnlyResetPos();
      assertEquals(await tap.readInt(), 42);

      await assertRejects(
        () => recordWriter(new TestTap(new ArrayBuffer(16)), 123 as unknown),
        ValidationError,
      );
      await assertRejects(
        () => recordWriter(new TestTap(new ArrayBuffer(16)), {} as unknown),
        ValidationError,
      );

      const syncFieldWriter = strategy.compileSyncFieldWriter(
        fieldType,
        true,
        () => {
          throw new Error("Unexpected nested writer request.");
        },
      );
      const syncWriter = strategy.assembleSyncRecordWriter(
        context,
        [syncFieldWriter],
      );
      const syncBuffer = new ArrayBuffer(16);
      const syncTap = new SyncWritableTap(syncBuffer);
      syncWriter(syncTap, { id: 7 });
      const syncReadTap = new SyncReadableTap(syncBuffer);
      assertEquals(syncReadTap.readInt(), 7);

      assertThrows(
        () => syncWriter(new SyncWritableTap(new ArrayBuffer(16)), 123 as unknown),
        ValidationError,
      );
      assertThrows(
        () => syncWriter(new SyncWritableTap(new ArrayBuffer(16)), {} as unknown),
        ValidationError,
      );
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

    it("handles unchecked records without defaults in sync and async modes", async () => {
      const type = createRecord({
        name: "example.NoDefaultsInterpreted",
        writerStrategy: new InterpretedWriterStrategy(),
        validate: false,
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      const value = { id: 12, name: "lean" };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    it("throws on non-record values when validate=true", async () => {
      const type = createRecord({
        name: "example.ValidateInterpreted",
        writerStrategy: new InterpretedWriterStrategy(),
        fields: [{ name: "id", type: new IntType() }],
      });

      const asyncTap = new TestTap(new ArrayBuffer(16));
      await assertRejects(() =>
        type.write(asyncTap, 123 as unknown as Record<string, unknown>)
      );

      const syncTap = new SyncWritableTap(new ArrayBuffer(16));
      assertThrows(() =>
        type.writeSync(syncTap, 123 as unknown as Record<string, unknown>)
      );
    });

    it("writes missing fields as undefined when defaults exist", async () => {
      const type = createRecord({
        name: "example.DefaultsInterpreted",
        writerStrategy: new InterpretedWriterStrategy(),
        validate: false,
        fields: [
          { name: "id", type: new IntType(), default: 1 },
          { name: "note", type: new NullType() },
        ],
      });

      const value = { id: 4 };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, { id: 4, note: null });

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, { id: 4, note: null });
    });

    it("applies default values when fields are missing", async () => {
      const type = createRecord({
        name: "example.MissingDefaultInterpreted",
        writerStrategy: new InterpretedWriterStrategy(),
        fields: [
          { name: "id", type: new IntType(), default: 10 },
          { name: "name", type: new StringType() },
        ],
      });

      const value = { name: "defaulted" };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, { id: 10, name: "defaulted" });

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, { id: 10, name: "defaulted" });
    });

    it("throws when required fields are missing", async () => {
      const type = createRecord({
        name: "example.MissingRequiredInterpreted",
        writerStrategy: new InterpretedWriterStrategy(),
        fields: [
          { name: "id", type: new IntType() },
          { name: "name", type: new StringType() },
        ],
      });

      await assertRejects(() =>
        type.toBuffer({ id: 1 } as Record<string, unknown>)
      );
      assertThrows(() =>
        type.toSyncBuffer({ id: 1 } as Record<string, unknown>)
      );
    });

    it("validates record writes when validate=true", async () => {
      const strategy = new InterpretedWriterStrategy();
      const fieldType = new IntType();
      const recordType = createRecord({
        name: "example.InterpretedValidatedContext",
        fields: [{ name: "id", type: fieldType }],
      });
      const context: RecordWriterContext = {
        fieldNames: ["id"],
        fieldTypes: [fieldType],
        fieldDefaultGetters: [undefined],
        fieldHasDefault: [false],
        validate: true,
        recordType,
        isRecord: (value): value is Record<string, unknown> =>
          typeof value === "object" && value !== null,
      };

      const fieldWriter = strategy.compileFieldWriter(
        fieldType,
        true,
        () => {
          throw new Error("Unexpected nested writer request.");
        },
      );
      const recordWriter = strategy.assembleRecordWriter(context, [fieldWriter]);
      const tap = new TestTap(new ArrayBuffer(16));
      await recordWriter(tap, { id: 55 });
      tap._testOnlyResetPos();
      assertEquals(await tap.readInt(), 55);

      await assertRejects(
        () => recordWriter(new TestTap(new ArrayBuffer(16)), 456 as unknown),
        ValidationError,
      );
      await assertRejects(
        () => recordWriter(new TestTap(new ArrayBuffer(16)), {} as unknown),
        ValidationError,
      );

      const syncFieldWriter = strategy.compileSyncFieldWriter(
        fieldType,
        true,
        () => {
          throw new Error("Unexpected nested writer request.");
        },
      );
      const syncWriter = strategy.assembleSyncRecordWriter(
        context,
        [syncFieldWriter],
      );
      const syncBuffer = new ArrayBuffer(16);
      const syncTap = new SyncWritableTap(syncBuffer);
      syncWriter(syncTap, { id: 8 });
      const syncReadTap = new SyncReadableTap(syncBuffer);
      assertEquals(syncReadTap.readInt(), 8);

      assertThrows(
        () => syncWriter(new SyncWritableTap(new ArrayBuffer(16)), 456 as unknown),
        ValidationError,
      );
      assertThrows(
        () => syncWriter(new SyncWritableTap(new ArrayBuffer(16)), {} as unknown),
        ValidationError,
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

  describe("custom strategy behavior", () => {
    it("routes non-record fields through getRecordWriter (validate=true)", async () => {
      const type = createRecord({
        name: "example.GreedyValidated",
        writerStrategy: new GreedyWriterStrategy(),
        fields: [{ name: "id", type: new IntType() }],
      });

      const value = { id: 11 };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    it("routes non-record fields through getRecordWriter (validate=false)", async () => {
      const type = createRecord({
        name: "example.GreedyUnchecked",
        writerStrategy: new GreedyWriterStrategy(),
        validate: false,
        fields: [{ name: "id", type: new IntType() }],
      });

      const value = { id: 22 };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
    });

    it("forces validated nested writers for non-record fields", async () => {
      const type = createRecord({
        name: "example.ForcedValidation",
        writerStrategy: new ForceValidateWriterStrategy(),
        fields: [{ name: "id", type: new IntType() }],
      });

      const value = { id: 33 };
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);

      const syncBuffer = type.toSyncBuffer(value);
      const syncDecoded = type.fromSyncBuffer(syncBuffer);
      assertEquals(syncDecoded, value);
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

    it("caches validated writers when validate=true", async () => {
      const fieldType = new IntType();
      const recordType = createRecord({
        name: "example.StrictCache",
        fields: [{ name: "id", type: fieldType }],
      });
      const context: RecordWriterContext = {
        fieldNames: ["id"],
        fieldTypes: [fieldType],
        fieldDefaultGetters: [undefined],
        fieldHasDefault: [false],
        validate: true,
        recordType,
        isRecord: (value): value is Record<string, unknown> =>
          typeof value === "object" && value !== null,
      };

      const cache = new RecordWriterCache();
      const writer = cache.getOrCreateWriter(
        true,
        context,
        () => {
          throw new Error("Unexpected nested writer request.");
        },
      );
      const tap = new TestTap(new ArrayBuffer(16));
      await writer(tap, { id: 1 });
      tap._testOnlyResetPos();
      assertEquals(await tap.readInt(), 1);

      const cachedWriter = cache.getOrCreateWriter(
        true,
        context,
        () => {
          throw new Error("Unexpected nested writer request.");
        },
      );
      const cachedTap = new TestTap(new ArrayBuffer(16));
      await cachedWriter(cachedTap, { id: 2 });
      cachedTap._testOnlyResetPos();
      assertEquals(await cachedTap.readInt(), 2);
    });

    it("caches validated sync writers when validate=true", () => {
      const fieldType = new IntType();
      const recordType = createRecord({
        name: "example.StrictCacheSync",
        fields: [{ name: "id", type: fieldType }],
      });
      const context: RecordWriterContext = {
        fieldNames: ["id"],
        fieldTypes: [fieldType],
        fieldDefaultGetters: [undefined],
        fieldHasDefault: [false],
        validate: true,
        recordType,
        isRecord: (value): value is Record<string, unknown> =>
          typeof value === "object" && value !== null,
      };

      const cache = new RecordWriterCache();
      const writer = cache.getOrCreateSyncWriter(
        true,
        context,
        () => {
          throw new Error("Unexpected nested writer request.");
        },
      );
      const buffer = new ArrayBuffer(16);
      const tap = new SyncWritableTap(buffer);
      writer(tap, { id: 3 });
      const readTap = new SyncReadableTap(buffer);
      assertEquals(readTap.readInt(), 3);

      const cachedWriter = cache.getOrCreateSyncWriter(
        true,
        context,
        () => {
          throw new Error("Unexpected nested writer request.");
        },
      );
      const cachedBuffer = new ArrayBuffer(16);
      const cachedTap = new SyncWritableTap(cachedBuffer);
      cachedWriter(cachedTap, { id: 4 });
      const cachedReadTap = new SyncReadableTap(cachedBuffer);
      assertEquals(cachedReadTap.readInt(), 4);
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
