import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { WritableTap } from "../../serialization/tap.ts";
import { SyncWritableTap } from "../../serialization/sync_tap.ts";
import { createType, type SchemaLike } from "../create_type.ts";

describe("createType validate option", () => {
  it("produces identical bytes for strict vs validate=false (sync writeSync)", () => {
    const schema: SchemaLike = {
      type: "record",
      name: "TestRecord",
      fields: [
        { name: "id", type: "int" },
        { name: "name", type: "string" },
        { name: "value", type: "double" },
        { name: "active", type: "boolean" },
        { name: "data", type: "bytes" },
      ],
    };

    const record = {
      id: 12345,
      name: "benchmark test record",
      value: 3.14159,
      active: true,
      data: new Uint8Array([1, 2, 3, 4, 5]),
    };

    const strictType = createType(schema, { validate: true });
    const uncheckedType = createType(schema, { validate: false });

    const strictBuf = new ArrayBuffer(1024);
    const strictTap = new SyncWritableTap(strictBuf);
    strictType.writeSync(strictTap, record);
    const strictBytes = new Uint8Array(strictBuf, 0, strictTap.getPos());

    const uncheckedBuf = new ArrayBuffer(1024);
    const uncheckedTap = new SyncWritableTap(uncheckedBuf);
    uncheckedType.writeSync(uncheckedTap, record);
    const uncheckedBytes = new Uint8Array(
      uncheckedBuf,
      0,
      uncheckedTap.getPos(),
    );

    assertEquals(Array.from(uncheckedBytes), Array.from(strictBytes));
  });

  it("produces identical bytes for strict vs validate=false (async write)", async () => {
    const schema: SchemaLike = {
      type: "record",
      name: "OuterRecord",
      fields: [
        { name: "id", type: "int" },
        {
          name: "inner",
          type: {
            type: "record",
            name: "InnerRecord",
            fields: [
              { name: "name", type: "string" },
              { name: "data", type: "bytes" },
            ],
          },
        },
      ],
    };

    const record = {
      id: 7,
      inner: {
        name: "nested",
        data: new Uint8Array([9, 8, 7]),
      },
    };

    const strictType = createType(schema, { validate: true });
    const uncheckedType = createType(schema, { validate: false });

    const strictBuf = new ArrayBuffer(1024);
    const strictTap = new WritableTap(strictBuf);
    await strictType.write(strictTap, record);
    const strictBytes = new Uint8Array(strictBuf, 0, strictTap.getPos());

    const uncheckedBuf = new ArrayBuffer(1024);
    const uncheckedTap = new WritableTap(uncheckedBuf);
    await uncheckedType.write(uncheckedTap, record);
    const uncheckedBytes = new Uint8Array(
      uncheckedBuf,
      0,
      uncheckedTap.getPos(),
    );

    assertEquals(Array.from(uncheckedBytes), Array.from(strictBytes));
  });

  it("produces identical bytes for strict vs validate=false (arrays, maps, unions, enums, fixed, logical types)", () => {
    const schema: SchemaLike = {
      type: "record",
      name: "ComplexRecord",
      fields: [
        { name: "ids", type: { type: "array", items: "int" } },
        { name: "tags", type: { type: "map", values: "string" } },
        { name: "choice", type: ["null", "string"] },
        {
          name: "kind",
          type: {
            type: "enum",
            name: "Kind",
            symbols: ["A", "B"],
          },
        },
        {
          name: "hash",
          type: {
            type: "fixed",
            name: "Hash4",
            size: 4,
          },
        },
        {
          name: "uuid",
          type: {
            type: "string",
            logicalType: "uuid",
          },
        },
      ],
    };

    const tags = new Map<string, string>([
      ["env", "test"],
      ["region", "us-east-1"],
    ]);

    const record = {
      ids: [1, 2, 3],
      tags,
      choice: { string: "hello" },
      kind: "A",
      hash: new Uint8Array([1, 2, 3, 4]),
      uuid: "550e8400-e29b-41d4-a716-446655440000",
    };

    const strictType = createType(schema, { validate: true });
    const uncheckedType = createType(schema, { validate: false });

    const strictBuf = new ArrayBuffer(2048);
    const strictTap = new SyncWritableTap(strictBuf);
    strictType.writeSync(strictTap, record);
    const strictBytes = new Uint8Array(strictBuf, 0, strictTap.getPos());

    const uncheckedBuf = new ArrayBuffer(2048);
    const uncheckedTap = new SyncWritableTap(uncheckedBuf);
    uncheckedType.writeSync(uncheckedTap, record);
    const uncheckedBytes = new Uint8Array(
      uncheckedBuf,
      0,
      uncheckedTap.getPos(),
    );

    assertEquals(Array.from(uncheckedBytes), Array.from(strictBytes));
  });
});
