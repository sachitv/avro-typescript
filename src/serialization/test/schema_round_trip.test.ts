/**
 * Round trips through the schema a writer embeds in a container file.
 *
 * Each case writes a file, reads the schema back out of its header, and writes
 * the same records again with that schema. The second file must embed exactly
 * the same schema and be byte-for-byte identical to the first, and its records
 * must read back unchanged. This checks that the embedded schema is a
 * complete, valid description of the data, including named types that are
 * reused, recursive, namespaced, or wrapped in logical types.
 */
import { assertEquals, assertInstanceOf, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { AvroFileParser } from "../avro_file_parser.ts";
import { SyncAvroFileParser } from "../avro_file_parser_sync.ts";
import { AvroFileWriter } from "../avro_file_writer.ts";
import { SyncReadableTap } from "../tap_sync.ts";
import { createType } from "../../type/create_type.ts";
import { parseJSON } from "../../schemas/json.ts";
import { parseHeader } from "../container/parse_header.ts";
import { isNeedMore } from "../container/need_more.ts";
import { FixedType } from "../../schemas/complex/fixed_type.ts";
import type { RecordType } from "../../schemas/complex/record_type.ts";
import { DecimalLogicalType } from "../../schemas/logical/decimal_logical_type.ts";
import type { Type } from "../../schemas/type.ts";
import { SyncAvroFileWriter } from "../avro_file_writer_sync.ts";
import {
  InMemoryReadableBuffer,
  InMemoryWritableBuffer,
} from "../buffers/in_memory_buffer.ts";
import {
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "../buffers/in_memory_buffer_sync.ts";

const SYNC = Uint8Array.from({ length: 16 }, (_, i) => 0x60 + i);

function writeFile(schema: unknown, records: unknown[]): Uint8Array {
  const buffer = new SyncInMemoryWritableBuffer(new ArrayBuffer(64 * 1024));
  const writer = new SyncAvroFileWriter(buffer, {
    schema: schema as never,
    syncMarker: SYNC,
  });
  for (const record of records) {
    writer.append(record);
  }
  writer.close();
  return new Uint8Array(buffer.getBufferCopy());
}

function parser(bytes: Uint8Array): SyncAvroFileParser {
  return new SyncAvroFileParser(
    new SyncInMemoryReadableBuffer(bytes.slice().buffer),
  );
}

function embeddedSchema(bytes: Uint8Array): string {
  return new TextDecoder().decode(
    parser(bytes).getHeader().meta.get("avro.schema"),
  );
}

/** Writes, rewrites with the embedded schema, and checks both files agree. */
function assertRoundTrip(schema: unknown, records: unknown[]): void {
  const first = writeFile(schema, records);
  const firstSchema = embeddedSchema(first);

  const second = writeFile(parseJSON(firstSchema), records);

  assertEquals(embeddedSchema(second), firstSchema);
  assertEquals(second, first);
  assertEquals(Array.from(parser(second).iterRecords()), records);
}

const bytes = (...values: number[]) => Uint8Array.from(values);

/** A schema whose field defaults are not JSON values when parsed. */
const DEFAULTS_SCHEMA = {
  type: "record",
  name: "Settings",
  fields: [
    { name: "count", type: "long", default: 5 },
    {
      name: "limits",
      type: { type: "map", values: "long" },
      default: { max: 9 },
    },
    { name: "salt", type: "bytes", default: "\u0000\u00ff" },
    { name: "maybe", type: ["long", "null"], default: { long: -1 } },
  ],
};
const DEFAULTS_RECORDS = [{
  count: 1n,
  limits: new Map([["max", 2n]]),
  salt: Uint8Array.of(3),
  maybe: null,
}];

describe("schema round trip through a container file", () => {
  it("keeps reused named types", () => {
    assertRoundTrip({
      type: "record",
      name: "Shipment",
      fields: [
        { name: "origin", type: { type: "fixed", name: "Code", size: 3 } },
        { name: "destination", type: "Code" },
        {
          name: "mode",
          type: { type: "enum", name: "Mode", symbols: ["AIR", "SEA"] },
        },
        { name: "fallbacks", type: { type: "array", items: "Mode" } },
        {
          name: "legs",
          type: {
            type: "array",
            items: {
              type: "record",
              name: "Leg",
              fields: [{ name: "from", type: "Code" }, {
                name: "to",
                type: "Code",
              }],
            },
          },
        },
        { name: "longest", type: "Leg" },
      ],
    }, [{
      origin: bytes(1, 2, 3),
      destination: bytes(4, 5, 6),
      mode: "SEA",
      fallbacks: ["AIR", "SEA"],
      legs: [{ from: bytes(1, 2, 3), to: bytes(7, 8, 9) }],
      longest: { from: bytes(7, 8, 9), to: bytes(4, 5, 6) },
    }]);
  });

  it("keeps a recursive record", () => {
    assertRoundTrip({
      type: "record",
      name: "Node",
      fields: [
        { name: "value", type: "int" },
        { name: "next", type: ["null", "Node"] },
      ],
    }, [
      { value: 1, next: { Node: { value: 2, next: null } } },
      { value: 3, next: null },
    ]);
  });

  it("keeps mutually recursive records", () => {
    assertRoundTrip({
      type: "record",
      name: "Person",
      fields: [
        { name: "name", type: "string" },
        {
          name: "pet",
          type: ["null", {
            type: "record",
            name: "Pet",
            fields: [
              { name: "name", type: "string" },
              { name: "owner", type: ["null", "Person"] },
            ],
          }],
        },
      ],
    }, [{
      name: "Ada",
      pet: {
        Pet: {
          name: "Rex",
          owner: { Person: { name: "Ada", pet: null } },
        },
      },
    }]);
  });

  it("keeps namespaces, including an empty one under a namespaced record", () => {
    assertRoundTrip({
      type: "record",
      name: "Order",
      namespace: "org.example",
      fields: [
        {
          name: "status",
          type: {
            type: "enum",
            name: "Status",
            namespace: "",
            symbols: ["OPEN", "DONE"],
          },
        },
        {
          name: "inner",
          type: {
            type: "record",
            name: "Inner",
            namespace: "x.y",
            fields: [{ name: "id", type: "long" }],
          },
        },
        { name: "copy", type: "x.y.Inner" },
        {
          name: "hash",
          type: { type: "fixed", name: "Hash", namespace: "c", size: 2 },
        },
        { name: "hashes", type: { type: "map", values: "c.Hash" } },
      ],
    }, [{
      status: "DONE",
      inner: { id: 1n },
      copy: { id: 2n },
      hash: bytes(1, 2),
      hashes: new Map([["a", bytes(3, 4)]]),
    }]);
  });

  it("keeps logical types over named fixed types", () => {
    assertRoundTrip({
      type: "record",
      name: "Payment",
      fields: [
        {
          name: "amount",
          type: {
            type: "fixed",
            name: "Money",
            size: 8,
            logicalType: "decimal",
            precision: 10,
            scale: 2,
          },
        },
        { name: "fee", type: "Money" },
        {
          name: "id",
          type: { type: "fixed", name: "Id", size: 16, logicalType: "uuid" },
        },
        { name: "parent", type: "Id" },
        {
          name: "term",
          type: {
            type: "fixed",
            name: "Term",
            size: 12,
            logicalType: "duration",
          },
        },
        { name: "grace", type: "Term" },
      ],
    }, [{
      amount: 12345n,
      fee: 99n,
      id: "123e4567-e89b-12d3-a456-426614174000",
      parent: "00000000-0000-4000-8000-000000000001",
      term: { months: 1, days: 2, millis: 3 },
      grace: { months: 0, days: 7, millis: 0 },
    }]);
  });

  it("keeps named types inside unions and maps", () => {
    assertRoundTrip({
      type: "record",
      name: "Event",
      fields: [
        {
          name: "kind",
          type: ["null", {
            type: "enum",
            name: "Kind",
            symbols: ["A", "B"],
          }, { type: "fixed", name: "Tag", size: 1 }],
        },
        { name: "tags", type: { type: "map", values: "Tag" } },
        { name: "kinds", type: { type: "map", values: ["null", "Kind"] } },
      ],
    }, [
      {
        kind: { Kind: "B" },
        tags: new Map([["x", bytes(9)]]),
        kinds: new Map([["y", { Kind: "A" }], ["z", null]]),
      },
      { kind: { Tag: bytes(1) }, tags: new Map(), kinds: new Map() },
    ]);
  });

  it("keeps type and field aliases", () => {
    assertRoundTrip({
      type: "record",
      name: "Customer",
      namespace: "shop",
      aliases: ["Client", "legacy.Account"],
      fields: [
        { name: "id", type: "long", aliases: ["customerId"] },
        {
          name: "tier",
          type: {
            type: "enum",
            name: "Tier",
            aliases: ["Level"],
            symbols: ["GOLD", "BASIC"],
          },
        },
        {
          name: "key",
          type: { type: "fixed", name: "Key", aliases: ["x.Token"], size: 2 },
        },
        { name: "previous", type: ["null", "Tier"] },
      ],
    }, [{
      id: 7n,
      tier: "GOLD",
      key: bytes(1, 2),
      previous: { "shop.Tier": "BASIC" },
    }]);
  });

  it("keeps long, map, and bytes field defaults", () => {
    assertRoundTrip(DEFAULTS_SCHEMA, DEFAULTS_RECORDS);
    assertEquals(
      JSON.parse(embeddedSchema(writeFile(DEFAULTS_SCHEMA, []))).fields.map(
        (field: { default?: unknown }) => field.default,
      ),
      DEFAULTS_SCHEMA.fields.map((field) => field.default),
    );
  });

  it("keeps logical field defaults", () => {
    const schema = {
      type: "record",
      name: "Event",
      fields: [
        {
          name: "at",
          type: { type: "long", logicalType: "timestamp-millis" },
          default: 1000,
        },
        {
          name: "amount",
          type: {
            type: "bytes",
            logicalType: "decimal",
            precision: 6,
            scale: 2,
          },
          default: "\u0001\u00e2",
        },
        {
          name: "term",
          type: {
            type: "fixed",
            name: "Term",
            size: 12,
            logicalType: "duration",
          },
          default: "\u0001\u0000\u0000\u0000\u0002\u0000\u0000\u0000" +
            "\u0003\u0000\u0000\u0000",
        },
      ],
    };
    const records = [{
      at: new Date(5000),
      amount: 7n,
      term: { months: 0, days: 1, millis: 0 },
    }];
    assertRoundTrip(schema, records);
    assertEquals(
      JSON.parse(embeddedSchema(writeFile(schema, []))).fields.map(
        (field: { default?: unknown }) => field.default,
      ),
      schema.fields.map((field) => field.default),
    );
  });

  it("keeps long defaults outside the safe integer range exactly", async () => {
    const schema = {
      type: "record",
      name: "Big",
      fields: [
        { name: "id", type: "int" },
        { name: "big", type: "long", default: 9007199254740993n },
        { name: "min", type: "long", default: -9223372036854775808n },
        { name: "ratio", type: "double", default: 1e20 },
      ],
    };
    const records = [{
      id: 1,
      big: 2n,
      min: 3n,
      ratio: 0.5,
    }];
    assertRoundTrip(schema, records);

    const file = writeFile(schema, records);
    const text = embeddedSchema(file);
    assertEquals(text.includes('"default":9007199254740993'), true);
    assertEquals(text.includes('"default":-9223372036854775808'), true);
    const header = parseHeader(file);
    if (isNeedMore(header)) {
      throw new Error("unexpected NeedMore");
    }
    // The raw header schema holds every integer literal above 2^53 as a
    // bigint; the double's type turns its default back into a number.
    assertEquals(
      (header.schema as { fields: Array<{ default?: unknown }> }).fields.map(
        (field) => field.default,
      ),
      [
        undefined,
        9007199254740993n,
        -9223372036854775808n,
        100000000000000000000n,
      ],
    );
    const parsedType = createType(header.schema as never) as RecordType;
    assertEquals(
      parsedType.getFields().map((field) =>
        field.hasDefault() ? field.getDefault() : undefined
      ),
      [undefined, 9007199254740993n, -9223372036854775808n, 1e20],
    );

    // A reader schema given as a JSON string keeps the defaults exactly too,
    // through both parsers.
    const reader = JSON.stringify(createType({
      type: "record",
      name: "Big",
      fields: [
        { name: "id", type: "int" },
        { name: "extra", type: "long", default: 9223372036854775807n },
      ],
    } as never));
    const expected = [{ id: 1, extra: 9223372036854775807n }];
    assertEquals(
      Array.from(
        new SyncAvroFileParser(
          new SyncInMemoryReadableBuffer(file.slice().buffer),
          { readerSchema: reader },
        ).iterRecords(),
      ),
      expected,
    );
    const asyncParser = new AvroFileParser(
      new InMemoryReadableBuffer(file.slice().buffer),
      { readerSchema: reader },
    );
    const read: unknown[] = [];
    for await (const record of asyncParser.iterRecords()) {
      read.push(record);
    }
    assertEquals(read, expected);
  });

  it("writes field defaults through the async writer and parser", async () => {
    const buffer = new InMemoryWritableBuffer(new ArrayBuffer(64 * 1024));
    const writer = new AvroFileWriter(buffer, {
      schema: DEFAULTS_SCHEMA as never,
      syncMarker: SYNC,
    });
    for (const record of DEFAULTS_RECORDS) {
      await writer.append(record);
    }
    await writer.close();
    const written = new Uint8Array(buffer.getBufferCopy());

    assertEquals(written, writeFile(DEFAULTS_SCHEMA, DEFAULTS_RECORDS));
    const asyncParser = new AvroFileParser(
      new InMemoryReadableBuffer(written.slice().buffer),
    );
    const header = await asyncParser.getHeader();
    const schema = JSON.parse(
      new TextDecoder().decode(header.meta.get("avro.schema")),
    );
    assertEquals(
      schema.fields.map((field: { default?: unknown }) => field.default),
      DEFAULTS_SCHEMA.fields.map((field) => field.default),
    );
    const read: unknown[] = [];
    for await (const record of asyncParser.iterRecords()) {
      read.push(record);
    }
    assertEquals(read, DEFAULTS_RECORDS);
  });

  it("writes the same file through the async writer and parser", async () => {
    const schema = {
      type: "record",
      name: "Node",
      fields: [
        { name: "code", type: { type: "fixed", name: "Code", size: 2 } },
        { name: "alias", type: "Code" },
        { name: "next", type: ["null", "Node"] },
      ],
    };
    const records = [{
      code: bytes(1, 2),
      alias: bytes(3, 4),
      next: { Node: { code: bytes(5, 6), alias: bytes(7, 8), next: null } },
    }];
    const sync = writeFile(schema, records);

    const buffer = new InMemoryWritableBuffer(new ArrayBuffer(64 * 1024));
    const writer = new AvroFileWriter(buffer, { schema, syncMarker: SYNC });
    for (const record of records) {
      await writer.append(record);
    }
    await writer.close();
    const written = new Uint8Array(buffer.getBufferCopy());

    assertEquals(written, sync);
    const asyncParser = new AvroFileParser(
      new InMemoryReadableBuffer(written.slice().buffer),
    );
    const read: unknown[] = [];
    for await (const record of asyncParser.iterRecords()) {
      read.push(record);
    }
    assertEquals(read, records);
  });
});

describe("schemas that cannot be written", () => {
  // The parser accepts both schemas; only writing them is refused.
  const flat = {
    type: "record",
    name: "Ledger",
    fields: [
      { name: "raw", type: { type: "fixed", name: "Word", size: 8 } },
      {
        name: "amount",
        type: { type: "Word", logicalType: "decimal", precision: 6 },
      },
    ],
  };
  const nested = {
    type: "record",
    name: "Ledger",
    fields: [
      { name: "raw", type: { type: "fixed", name: "Word", size: 8 } },
      {
        name: "inner",
        type: {
          type: "record",
          name: "Inner",
          fields: [{ name: "x", type: "Word" }],
        },
      },
      {
        name: "amount",
        type: { type: "Word", logicalType: "decimal", precision: 6 },
      },
    ],
  };
  const fieldType = (record: Type, ...path: string[]): Type => {
    let type = record;
    for (const name of path) {
      type = (type as RecordType).getField(name)!.getType();
    }
    return type;
  };

  it("refuses a logical type applied to a plain fixed by reference", () => {
    // Avro annotates definitions, not references; this library's parser
    // accepts the form, but resolves it differently depending on when nested
    // records are built (see the next case), so it is not written.
    const parsed = createType(flat as never);
    assertInstanceOf(fieldType(parsed, "raw"), FixedType);
    assertInstanceOf(fieldType(parsed, "amount"), DecimalLogicalType);

    assertThrows(
      () => writeFile(flat, []),
      Error,
      "Cannot apply logicalType decimal to Word",
    );
  });

  it("refuses it when a nested record uses the fixed first", () => {
    // Parsed, `inner.x` and `amount` are both decimals: the nested record's
    // fields are built after `amount` has mapped Word to the decimal. Writing
    // an annotated reference here would read back with `amount` as a plain
    // fixed, so the write fails instead.
    const parsed = createType(nested as never);
    assertInstanceOf(fieldType(parsed, "inner", "x"), DecimalLogicalType);
    assertInstanceOf(fieldType(parsed, "amount"), DecimalLogicalType);

    assertThrows(
      () => writeFile(nested, []),
      Error,
      "Cannot apply logicalType decimal to Word",
    );
  });
});

describe("reader schema aliases after a toJSON round trip", () => {
  // A reader schema that was stored with toJSON() and parsed back must still
  // resolve data written under the old names.
  const reparse = (schema: unknown) =>
    createType(JSON.parse(JSON.stringify(createType(schema as never))));

  it("resolves an old record and field name", () => {
    const writer = createType({
      type: "record",
      name: "Old",
      fields: [{ name: "count", type: "int" }],
    });
    const reader = reparse({
      type: "record",
      name: "New",
      aliases: ["Old"],
      fields: [{ name: "total", aliases: ["count"], type: "int" }],
    });

    const resolver = reader.createResolver(writer);
    const value = resolver.readSync(
      new SyncReadableTap(writer.toSyncBuffer({ count: 5 })),
    );

    assertEquals(value, { total: 5 });
  });

  it("resolves an old enum name", () => {
    const writer = createType({
      type: "enum",
      name: "Grade",
      symbols: ["A", "B"],
    });
    const reader = reparse({
      type: "enum",
      name: "Level",
      aliases: ["Grade"],
      symbols: ["A", "B"],
    });

    const value = reader.createResolver(writer).readSync(
      new SyncReadableTap(writer.toSyncBuffer("B")),
    );

    assertEquals(value, "B");
  });

  it("resolves an old fixed name", () => {
    const writer = createType({ type: "fixed", name: "Hash", size: 2 });
    const reader = reparse({
      type: "fixed",
      name: "Digest",
      aliases: ["Hash"],
      size: 2,
    });

    const value = reader.createResolver(writer).readSync(
      new SyncReadableTap(writer.toSyncBuffer(bytes(3, 4))),
    );

    assertEquals(value, bytes(3, 4));
  });
});
