import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { createType } from "../../type/create_type.ts";
import { createRecord } from "../complex/test/record_test_utils.ts";
import type { RecordType } from "../complex/record_type.ts";
import { defaultToJSON } from "../default_json.ts";
import type { Type } from "../type.ts";

const FIELD = "R.f";

/** Encodes `value` as a default of the type parsed from `schema`. */
function encode(schema: unknown, value: unknown): unknown {
  return defaultToJSON(createType(schema as never), value, FIELD);
}

/** Bytes 0x00-0xff, and the string with the same code points. */
const ALL_BYTES = Uint8Array.from({ length: 256 }, (_, i) => i);
const ALL_BYTES_JSON = String.fromCharCode(...ALL_BYTES);

const record = (name: string, fields: unknown[]) => ({
  type: "record",
  name,
  fields,
});

describe("defaultToJSON", () => {
  it("returns values that are already JSON unchanged", () => {
    assertEquals(encode("null", null), null);
    assertEquals(encode("boolean", false), false);
    assertEquals(encode("int", -7), -7);
    assertEquals(encode("string", "hé ☃"), "hé ☃");
    assertEquals(
      encode({ type: "enum", name: "E", symbols: ["A", "B"] }, "B"),
      "B",
    );
  });

  it("writes a long as an integer", () => {
    assertEquals(encode("long", 5n), 5);
    assertEquals(encode("long", 0n), 0);
    assertEquals(
      encode("long", BigInt(Number.MAX_SAFE_INTEGER)),
      Number.MAX_SAFE_INTEGER,
    );
    assertEquals(
      encode("long", -BigInt(Number.MAX_SAFE_INTEGER)),
      -Number.MAX_SAFE_INTEGER,
    );
  });

  it("refuses a long outside the safe integer range", () => {
    for (
      const value of [
        BigInt(Number.MAX_SAFE_INTEGER) + 1n,
        -BigInt(Number.MAX_SAFE_INTEGER) - 1n,
        9223372036854775807n,
        -9223372036854775808n,
      ]
    ) {
      assertThrows(
        () => encode("long", value),
        Error,
        `Cannot write the default of field 'R.f' to schema JSON: long ${value} ` +
          "is outside the safe integer range",
      );
    }
  });

  it("writes float and double numbers", () => {
    assertEquals(encode("float", 1.5), 1.5);
    assertEquals(encode("double", -0.1), -0.1);
  });

  it("refuses non-finite float and double values", () => {
    for (const type of ["float", "double"]) {
      for (const value of [NaN, Infinity, -Infinity]) {
        assertThrows(
          () => encode(type, value),
          Error,
          `Cannot write the default of field 'R.f' to schema JSON: ${type} ` +
            `${value} is not finite`,
        );
      }
    }
  });

  it("writes bytes and fixed values as one code point per byte", () => {
    assertEquals(encode("bytes", ALL_BYTES), ALL_BYTES_JSON);
    assertEquals(encode("bytes", new Uint8Array(0)), "");
    assertEquals(
      encode({ type: "fixed", name: "F", size: 256 }, ALL_BYTES),
      ALL_BYTES_JSON,
    );
  });

  it("writes arrays item by item", () => {
    assertEquals(
      encode({ type: "array", items: "long" }, [1n, -2n]),
      [1, -2],
    );
    assertEquals(encode({ type: "array", items: "bytes" }, []), []);
  });

  it("writes maps as objects", () => {
    assertEquals(
      encode(
        { type: "map", values: "long" },
        new Map([["a", 1n], ["b", 2n]]),
      ),
      { a: 1, b: 2 },
    );
    assertEquals(encode({ type: "map", values: "int" }, new Map()), {});
  });

  it("keeps a __proto__ map key as a key", () => {
    const json = encode(
      { type: "map", values: "int" },
      new Map([["__proto__", 1]]),
    ) as Record<string, unknown>;

    assertEquals(Object.getPrototypeOf(json), Object.prototype);
    assertEquals(Object.keys(json), ["__proto__"]);
    assertEquals(JSON.stringify(json), '{"__proto__":1}');
  });

  it("writes records field by field, in field order", () => {
    const json = encode(
      record("Outer", [
        { name: "n", type: "long" },
        { name: "b", type: "bytes" },
        {
          name: "inner",
          type: record("Inner", [
            { name: "m", type: { type: "map", values: "long" } },
          ]),
        },
      ]),
      { inner: { m: new Map([["k", 3n]]) }, b: ALL_BYTES, n: 4n },
    );

    assertEquals(
      JSON.stringify(json),
      JSON.stringify({
        n: 4,
        b: ALL_BYTES_JSON,
        inner: { m: { k: 3 } },
      }),
    );
  });

  it("writes nested maps, arrays, and bytes", () => {
    assertEquals(
      encode(
        { type: "map", values: { type: "array", items: "bytes" } },
        new Map([["x", [Uint8Array.of(0, 255)]]]),
      ),
      { x: ["\u0000ÿ"] },
    );
  });

  it("writes union values wrapped in their branch", () => {
    assertEquals(encode(["null", "long"], null), null);
    assertEquals(encode(["long", "null"], { long: 5n }), { long: 5 });
    assertEquals(
      encode(["null", { type: "map", values: "long" }], {
        map: new Map([["k", 1n]]),
      }),
      { map: { k: 1 } },
    );
    assertEquals(
      encode(
        ["null", {
          ...record("In", [{ name: "n", type: "long" }]),
          namespace: "a.b",
        }],
        { "a.b.In": { n: 2n } },
      ),
      { "a.b.In": { n: 2 } },
    );
    assertEquals(
      encode(["null", { type: "fixed", name: "F", size: 2 }], {
        F: Uint8Array.of(1, 200),
      }),
      { F: "\u0001È" },
    );
  });

  it("refuses unsafe values nested in unions, arrays, maps, and records", () => {
    const unsafe = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    const cases: Array<[unknown, unknown]> = [
      [["long", "null"], { long: unsafe }],
      [{ type: "array", items: "long" }, [0n, unsafe]],
      [{ type: "map", values: "double" }, new Map([["k", NaN]])],
      [record("In", [{ name: "n", type: "long" }]), { n: unsafe }],
    ];
    for (const [schema, value] of cases) {
      assertThrows(
        () => encode(schema, value),
        Error,
        "Cannot write the default of field 'R.f' to schema JSON",
      );
    }
  });

  it("writes logical values as their underlying value", () => {
    assertEquals(encode({ type: "int", logicalType: "date" }, 19000), 19000);
    assertEquals(
      encode({ type: "int", logicalType: "time-millis" }, 1000),
      1000,
    );
    assertEquals(
      encode({ type: "long", logicalType: "time-micros" }, 1000n),
      1000,
    );
    assertEquals(
      encode(
        { type: "long", logicalType: "timestamp-millis" },
        new Date(1_700_000_000_000),
      ),
      1_700_000_000_000,
    );
    assertEquals(
      encode({ type: "long", logicalType: "timestamp-micros" }, -5n),
      -5,
    );
    assertEquals(
      encode({ type: "long", logicalType: "timestamp-nanos" }, 7n),
      7,
    );
    assertEquals(
      encode({ type: "long", logicalType: "local-timestamp-millis" }, 42),
      42,
    );
    assertEquals(
      encode({ type: "long", logicalType: "local-timestamp-micros" }, 43n),
      43,
    );
    assertEquals(
      encode({ type: "long", logicalType: "local-timestamp-nanos" }, 44n),
      44,
    );
    assertEquals(
      encode(
        { type: "string", logicalType: "uuid" },
        "123e4567-e89b-12d3-a456-426614174000",
      ),
      "123e4567-e89b-12d3-a456-426614174000",
    );
  });

  it("writes logical values over bytes and fixed as byte strings", () => {
    assertEquals(
      encode(
        { type: "bytes", logicalType: "decimal", precision: 5, scale: 2 },
        -1n,
      ),
      "ÿ",
    );
    assertEquals(
      encode(
        {
          type: "fixed",
          name: "D",
          size: 2,
          logicalType: "decimal",
          precision: 4,
        },
        256n,
      ),
      "\u0001\u0000",
    );
    assertEquals(
      encode(
        { type: "fixed", name: "U", size: 16, logicalType: "uuid" },
        "00ff0000-0000-0000-0000-000000000080",
      ),
      "\u0000ÿ" + "\u0000".repeat(13) + "\u0080",
    );
    assertEquals(
      encode(
        { type: "fixed", name: "Dur", size: 12, logicalType: "duration" },
        { months: 1, days: 2, millis: 3 },
      ),
      "\u0001\u0000\u0000\u0000\u0002\u0000\u0000\u0000\u0003\u0000\u0000\u0000",
    );
  });

  it("refuses a logical long default outside the safe integer range", () => {
    assertThrows(
      () =>
        encode(
          { type: "long", logicalType: "timestamp-micros" },
          BigInt(Number.MAX_SAFE_INTEGER) + 1n,
        ),
      Error,
      "outside the safe integer range",
    );
  });
});

describe("record field defaults in schema JSON", () => {
  /** The field `f` of the one-field record `R` with the given type and default. */
  const withDefault = (type: unknown, value: unknown) =>
    record("R", [{ name: "f", type, default: value }]);

  /**
   * Parses `schema`, writes it, parses the JSON again, and checks both writes
   * agree and the field default survives unchanged.
   */
  function assertDefaultRoundTrip(schema: unknown, expected: unknown): void {
    const first = createType(schema as never) as RecordType;
    const json = JSON.stringify(first);
    const second = createType(JSON.parse(json)) as RecordType;

    assertEquals(JSON.stringify(second), json);
    assertEquals(
      (JSON.parse(json) as { fields: Array<{ default: unknown }> }).fields[0]
        .default,
      expected,
    );
    assertEquals(
      second.getField("f")!.getDefault(),
      first.getField("f")!.getDefault(),
    );
  }

  it("writes a long default, which JSON.stringify could not before", () => {
    assertEquals(
      JSON.stringify(createType(withDefault("long", 5) as never)),
      '{"name":"R","type":"record","fields":[{"name":"f","type":"long","default":5}]}',
    );
  });

  it("writes a map default, which was written as {} before", () => {
    assertEquals(
      JSON.stringify(
        createType(
          withDefault({ type: "map", values: "int" }, { k: 1 }) as never,
        ),
      ),
      '{"name":"R","type":"record","fields":[{"name":"f","type":{"type":"map","values":"int"},"default":{"k":1}}]}',
    );
  });

  it("round trips defaults of every JSON-readable type", () => {
    const cases: Array<[unknown, unknown]> = [
      ["null", null],
      ["boolean", true],
      ["int", -3],
      ["long", 5],
      ["long", -Number.MAX_SAFE_INTEGER],
      ["float", 0.5],
      ["double", 1e-300],
      ["bytes", ALL_BYTES_JSON],
      [{ type: "fixed", name: "F", size: 256 }, ALL_BYTES_JSON],
      ["string", "\u0000ÿ☃"],
      [{ type: "enum", name: "E", symbols: ["A", "B"] }, "B"],
      [{ type: "array", items: "long" }, [1, 2]],
      [{ type: "map", values: "long" }, { a: 1, b: -2 }],
      [{ type: "map", values: { type: "map", values: "bytes" } }, {
        outer: { inner: "\u0000ÿ" },
      }],
      [{ type: "array", items: { type: "map", values: "long" } }, [{ k: 1 }]],
      [
        record("In", [
          { name: "n", type: "long" },
          { name: "m", type: { type: "map", values: "int" } },
          { name: "b", type: "bytes" },
        ]),
        { n: 1, m: { k: 2 }, b: "ÿ" },
      ],
      [["null", "long"], null],
      [["long", "null"], { long: 9 }],
      [["null", { type: "map", values: "long" }], { map: { k: 1 } }],
      [{ type: "int", logicalType: "date" }, 19000],
      [{ type: "int", logicalType: "time-millis" }, 1000],
      [{ type: "long", logicalType: "local-timestamp-millis" }, 42],
      [
        { type: "string", logicalType: "uuid" },
        "123e4567-e89b-12d3-a456-426614174000",
      ],
    ];
    for (const [type, value] of cases) {
      assertDefaultRoundTrip(withDefault(type, value), value);
    }
  });

  it("round trips a record default that fills in nested field defaults", () => {
    assertDefaultRoundTrip(
      withDefault(
        record("In", [
          { name: "n", type: "long" },
          {
            name: "m",
            type: { type: "map", values: "long" },
            default: { d: 7 },
          },
        ]),
        { n: 1 },
      ),
      { n: 1, m: { d: 7 } },
    );
  });

  it("names the field when a long default is outside the safe range", () => {
    const type = createType({
      type: "record",
      name: "R",
      namespace: "a.b",
      fields: [{
        name: "big",
        type: "long",
        default: 9007199254740993n,
      }],
    } as never);

    assertThrows(
      () => JSON.stringify(type),
      Error,
      "Cannot write the default of field 'a.b.R.big' to schema JSON: long " +
        "9007199254740993 is outside the safe integer range (±9007199254740991)",
    );
  });

  it("names the field when a double default is not finite", () => {
    const type = createType(withDefault("double", NaN) as never);

    assertThrows(
      () => JSON.stringify(type),
      Error,
      "Cannot write the default of field 'R.f' to schema JSON: double NaN " +
        "is not finite",
    );
  });

  it("refuses same-named records that differ only in a map default", () => {
    const config = (value: Record<string, number>): Type =>
      createType(
        record("Config", [{
          name: "limits",
          type: { type: "map", values: "int" },
          default: value,
        }]) as never,
      );
    const outer = createRecord({
      name: "Outer",
      fields: [
        { name: "a", type: config({ max: 1 }) },
        { name: "b", type: config({ max: 2 }) },
      ],
    });

    assertThrows(
      () => outer.toJSON(),
      Error,
      "Duplicate Avro type name: Config",
    );
  });

  it("still writes same-named records with equal map defaults once", () => {
    const config = (): Type =>
      createType(
        record("Config", [{
          name: "limits",
          type: { type: "map", values: "int" },
          default: { max: 1 },
        }]) as never,
      );
    const outer = createRecord({
      name: "Outer",
      fields: [
        { name: "a", type: config() },
        { name: "b", type: config() },
      ],
    });

    assertEquals(
      (outer.toJSON() as { fields: Array<{ type: unknown }> }).fields[1].type,
      "Config",
    );
  });
});
