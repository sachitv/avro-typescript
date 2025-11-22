import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { createType } from "../create_type.ts";
import { AvroWriter } from "../../avro_writer.ts";
import { AvroReader } from "../../avro_reader.ts";
import { InMemoryReadableBuffer } from "../../serialization/buffers/in_memory_buffer.ts";

const FULL_SCHEMA = {
  type: "record",
  name: "TestRecord",
  fields: [
    { name: "nullField", type: "null", default: null },
    { name: "booleanField", type: "boolean", default: true },
    { name: "intField", type: "int", default: 1 },
    { name: "longField", type: "long", default: 1 },
    { name: "floatField", type: "float", default: 1.5 },
    { name: "doubleField", type: "double", default: 1.1 },
    { name: "bytesField", type: "bytes", default: "\u00FF" },
    { name: "stringField", type: "string", default: "foo" },
    {
      name: "recordField",
      type: {
        type: "record",
        name: "NestedRecord",
        fields: [{ name: "a", type: "int" }],
      },
      default: { a: 1 },
    },
    {
      name: "enumField",
      type: {
        type: "enum",
        name: "TestEnum",
        symbols: ["FOO", "BAR"],
      },
      default: "FOO",
    },
    { name: "arrayField", type: { type: "array", items: "int" }, default: [1] },
    {
      name: "mapField",
      type: { type: "map", values: "int" },
      default: { a: 1 },
    },
    {
      name: "fixedField",
      type: { type: "fixed", name: "TestFixed", size: 1 },
      default: "\u00ff",
    },
  ],
} as const;

const EXPECTED_DEFAULTS_RECORD = {
  nullField: null,
  booleanField: true,
  intField: 1,
  longField: 1n, // long is BigInt
  floatField: 1.5,
  doubleField: 1.1,
  bytesField: new Uint8Array([255]),
  stringField: "foo",
  recordField: { a: 1 },
  enumField: "FOO",
  arrayField: [1],
  mapField: new Map([["a", 1]]),
  fixedField: new Uint8Array([255]),
};

const MINIMAL_SCHEMA = {
  type: "record",
  name: "TestRecord",
  fields: [{ name: "id", type: "int" }],
} as const;

const MINIMAL_RECORDS = [{ id: 42 }];

describe("createType with defaults", () => {
  it("writes data with fixed defaults and reads it back", async () => {
    const type = createType(FULL_SCHEMA);

    // Write the record with defaults
    const buffer = await type.toBuffer(EXPECTED_DEFAULTS_RECORD);
    const decoded = await type.fromBuffer(buffer);

    assertEquals(decoded, EXPECTED_DEFAULTS_RECORD);
  });

  it("reads a file where values are missing and ensures defaults are set", async () => {
    // Create a buffer to collect the Avro data
    const chunks: Uint8Array[] = [];
    const writable = new WritableStream<Uint8Array>({
      write(chunk) {
        chunks.push(chunk.slice());
      },
    });

    // Write with minimal schema
    const writer = AvroWriter.toStream(writable, { schema: MINIMAL_SCHEMA });
    for (const record of MINIMAL_RECORDS) {
      await writer.append(record);
    }
    await writer.close();

    // Concatenate chunks into a buffer
    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const avroData = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      avroData.set(chunk, offset);
      offset += chunk.length;
    }

    // Read with full schema (which has defaults)
    const reader = AvroReader.fromBuffer(
      new InMemoryReadableBuffer(avroData.buffer),
      { readerSchema: FULL_SCHEMA },
    );

    const records = [];
    for await (const record of reader.iterRecords()) {
      records.push(record);
    }

    // The record should have the defaults
    assertEquals(records, [EXPECTED_DEFAULTS_RECORD]);
  });
});
