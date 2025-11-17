import { assertEquals, assertInstanceOf, assertThrows } from "@std/assert";
import { afterAll, beforeAll, describe, it } from "@std/testing/bdd";
import { randomSeeded } from "@std/random";

import { createType } from "../create_type.ts";
import { TestTap as Tap } from "../../serialization/test/test_tap.ts";
import { Resolver } from "../../schemas/resolver.ts";
import type { Type } from "../../schemas/type.ts";

const namespace = "org.apache.avro.compiler.schema.evolve";

const RANDOM_SEED = 0x1f1f1f1fn;
const DEFAULT_BUFFER_SIZE = 256;

const originalRandom = Math.random;
let rng = randomSeeded(RANDOM_SEED);

const seededRandom = () => rng();

Math.random = seededRandom;

beforeAll(() => {
  rng = randomSeeded(RANDOM_SEED);
});

afterAll(() => {
  Math.random = originalRandom;
});

const cloneSchema = <T>(schema: T): T =>
  JSON.parse(JSON.stringify(schema)) as T;

const resolveWithResolver = async <TWriter, TReader>(
  writerType: Type<TWriter>,
  readerType: Type<TReader>,
  value: TWriter,
  bufferSize = DEFAULT_BUFFER_SIZE,
): Promise<TReader> => {
  const buffer = new ArrayBuffer(bufferSize);
  const writeTap = new Tap(buffer);
  await writerType.write(writeTap, value);
  const resolver = readerType.createResolver(writerType);
  const readTap = new Tap(buffer);
  return await resolver.read(readTap) as TReader;
};

const TestRecord1Schema = {
  type: "record",
  name: "TestRecord1",
  namespace,
  aliases: ["TestRecord2"],
  fields: [
    { name: "name", type: "string" },
    { name: "value", type: "long" },
  ],
};

const TestRecord2Schema = {
  type: "record",
  name: "TestRecord2",
  namespace,
  aliases: ["TestRecord1"],
  fields: [
    { name: "name", type: "string" },
    { name: "value", type: "long" },
    { name: "data", type: "string", default: "" },
  ],
};

const TestRecord3Schema = {
  type: "record",
  name: "TestRecord3",
  namespace,
  fields: [
    { name: "name", type: "string" },
    { name: "data", type: "string" },
  ],
};

const NestedEvolve1Schema = {
  type: "record",
  name: "NestedEvolve1",
  namespace,
  fields: [
    { name: "rootName", type: "string" },
    { name: "nested", type: cloneSchema(TestRecord1Schema) },
  ],
};

const NestedEvolve2Schema = {
  type: "record",
  name: "NestedEvolve2",
  namespace,
  aliases: ["NestedEvolve1"],
  fields: [
    { name: "rootName", type: "string" },
    { name: "nested", type: cloneSchema(TestRecord2Schema) },
  ],
};

const NestedEvolve3Schema = {
  type: "record",
  name: "NestedEvolve3",
  namespace,
  fields: [
    { name: "rootName", type: "string" },
    { name: "nested", type: cloneSchema(TestRecord3Schema) },
  ],
};

const AliasWriterSchema = {
  type: "record",
  name: "AliasTestRecord",
  namespace,
  fields: [
    { name: "name", type: "string" },
    { name: "oldValue", type: "long" },
  ],
};

const AliasReaderSchema = {
  type: "record",
  name: "AliasTestRecord",
  namespace,
  fields: [
    { name: "name", type: "string" },
    {
      name: "newValue",
      type: "long",
      aliases: ["oldValue"],
    },
  ],
};

const TypePromotionWriterSchema = {
  type: "record",
  name: "TypePromotionTest",
  namespace,
  fields: [{ name: "data", type: "string" }],
};

const TypePromotionReaderSchema = {
  type: "record",
  name: "TypePromotionTest",
  namespace,
  fields: [{ name: "data", type: "bytes" }],
};

const FieldOrderWriterSchema = {
  type: "record",
  name: "FieldOrderTest",
  namespace,
  fields: [
    { name: "first", type: "string" },
    { name: "second", type: "long" },
  ],
};

const FieldOrderReaderSchema = {
  type: "record",
  name: "FieldOrderTest",
  namespace,
  fields: [
    { name: "second", type: "long" },
    { name: "first", type: "string" },
  ],
};

const IncompatibleRecordWriterSchema = {
  type: "record",
  name: "Record1",
  namespace,
  fields: [{ name: "field1", type: "string" }],
};

const IncompatibleRecordReaderSchema = {
  type: "record",
  name: "Record2",
  namespace,
  fields: [{ name: "field1", type: "long" }],
};

const testRecord1Type = createType(cloneSchema(TestRecord1Schema));
const testRecord2Type = createType(cloneSchema(TestRecord2Schema));
const testRecord3Type = createType(cloneSchema(TestRecord3Schema));
const nestedEvolve1Type = createType(cloneSchema(NestedEvolve1Schema));
const nestedEvolve2Type = createType(cloneSchema(NestedEvolve2Schema));
const nestedEvolve3Type = createType(cloneSchema(NestedEvolve3Schema));
const aliasWriterType = createType(cloneSchema(AliasWriterSchema));
const aliasReaderType = createType(cloneSchema(AliasReaderSchema));
const typePromotionWriterType = createType(
  cloneSchema(TypePromotionWriterSchema),
);
const typePromotionReaderType = createType(
  cloneSchema(TypePromotionReaderSchema),
);
const fieldOrderWriterType = createType(cloneSchema(FieldOrderWriterSchema));
const fieldOrderReaderType = createType(cloneSchema(FieldOrderReaderSchema));
const compatibleRecordTypeA = createType(cloneSchema(TestRecord1Schema));
const compatibleRecordTypeB = createType(cloneSchema(TestRecord1Schema));
const incompatibleRecordWriterType = createType(cloneSchema(
  IncompatibleRecordWriterSchema,
));
const incompatibleRecordReaderType = createType(cloneSchema(
  IncompatibleRecordReaderSchema,
));
const stringType = createType("string");
const longType = createType("long");

describe("Schema Evolution Tests", () => {
  describe("Field compatibility", () => {
    it("should read evolved record with added field", async () => {
      const writerData = testRecord1Type.random() as {
        name: string;
        value: bigint;
      };
      const readerResult = await resolveWithResolver(
        testRecord1Type,
        testRecord2Type,
        writerData,
      ) as {
        name: string;
        value: bigint;
        data: string;
      };

      assertEquals(readerResult.name, writerData.name);
      assertEquals(readerResult.value, writerData.value);
      assertEquals(readerResult.data, ""); // New field should use default
    });

    it("should read evolved record with removed field", async () => {
      const writerData = testRecord2Type.random() as {
        name: string;
        value: bigint;
        data: string;
      };
      const readerResult = await resolveWithResolver(
        testRecord2Type,
        testRecord1Type,
        writerData,
      );

      assertEquals((readerResult as { name: string }).name, writerData.name);
      assertEquals((readerResult as { value: bigint }).value, writerData.value);
      assertEquals(
        (readerResult as { data?: string }).data,
        undefined,
      );
    });

    it("should reject a reader that drops a required field", () => {
      assertThrows(() => {
        testRecord2Type.createResolver(testRecord3Type);
      }, Error);
    });

    it("should support field evolution using aliases", async () => {
      const writerData = aliasWriterType.random() as {
        name: string;
        oldValue: bigint;
      };
      const readerResult = await resolveWithResolver(
        aliasWriterType,
        aliasReaderType,
        writerData,
      ) as {
        name: string;
        newValue: bigint;
      };

      assertEquals(readerResult.name, writerData.name);
      assertEquals(readerResult.newValue, writerData.oldValue);
    });
  });

  describe("Nested record compatibility", () => {
    it("should handle evolution in nested records", async () => {
      const writerData = nestedEvolve1Type.random() as {
        rootName: string;
        nested: { name: string; value: bigint };
      };
      const readerResult = await resolveWithResolver(
        nestedEvolve1Type,
        nestedEvolve2Type,
        writerData,
      ) as {
        rootName: string;
        nested: { name: string; value: bigint; data: string };
      };

      assertEquals(readerResult.rootName, writerData.rootName);
      assertEquals(readerResult.nested.name, writerData.nested.name);
      assertEquals(readerResult.nested.value, writerData.nested.value);
      assertEquals(readerResult.nested.data, undefined);
    });

    it("should reject readers that remove nested required fields", () => {
      assertThrows(() => {
        nestedEvolve2Type.createResolver(nestedEvolve3Type);
      }, Error);
    });
  });

  describe("Resolver compatibility", () => {
    it("should allow exact same types", () => {
      const resolver = compatibleRecordTypeA.createResolver(
        compatibleRecordTypeB,
      );
      assertInstanceOf(resolver, Resolver);
    });

    it("should reject incompatible types", () => {
      assertThrows(() => {
        stringType.createResolver(longType);
      }, Error);
    });

    it("should reject incompatible record schemas", () => {
      assertThrows(() => {
        incompatibleRecordWriterType.createResolver(
          incompatibleRecordReaderType,
        );
      }, Error);
    });
  });

  describe("Additional evolution scenarios", () => {
    it("should handle field type promotion (string to bytes compatibility)", async () => {
      const writerData = typePromotionWriterType.random();
      const result = await resolveWithResolver(
        typePromotionWriterType,
        typePromotionReaderType,
        writerData,
      );
      assertEquals(typeof result, "object");
    });

    it("should maintain field order compatibility", async () => {
      const writerData = fieldOrderWriterType.random() as {
        first: string;
        second: bigint;
      };
      const readerResult = await resolveWithResolver(
        fieldOrderWriterType,
        fieldOrderReaderType,
        writerData,
      ) as {
        first: string;
        second: bigint;
      };

      assertEquals(readerResult.first, writerData.first);
      assertEquals(readerResult.second, writerData.second);
    });
  });
});
