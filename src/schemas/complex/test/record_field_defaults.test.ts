import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { createType } from "../../../type/create_type.ts";
import { createRecord } from "./record_test_utils.ts";
import type { RecordType } from "../record_type.ts";
import type { Type } from "../../type.ts";

/** Bytes 0x00-0xff as a JSON byte string. */
const ALL_BYTES_JSON = String.fromCharCode(
  ...Array.from({ length: 256 }, (_, i) => i),
);

const record = (name: string, fields: unknown[]) => ({
  type: "record",
  name,
  fields,
});

/** A valid UUID, and its 16 bytes as a JSON byte string. */
const UUID = "123e4567-e89b-12d3-a456-426614174000";
const UUID_JSON = String.fromCharCode(
  ...(UUID.replaceAll("-", "").match(/../g) ?? []).map((hex) =>
    parseInt(hex, 16)
  ),
);
/** Duration months 1, days 2, millis 3, and its 12 bytes as a byte string. */
const DURATION = { months: 1, days: 2, millis: 3 };
const DURATION_JSON = "\u0001\u0000\u0000\u0000\u0002\u0000\u0000\u0000" +
  "\u0003\u0000\u0000\u0000";

const TIMESTAMP = { type: "long", logicalType: "timestamp-millis" };
const DECIMAL_BYTES = {
  type: "bytes",
  logicalType: "decimal",
  precision: 6,
  scale: 2,
};
const DECIMAL_FIXED = {
  type: "fixed",
  name: "Money",
  size: 8,
  logicalType: "decimal",
  precision: 6,
};
const UUID_FIXED = { type: "fixed", name: "Id", size: 16, logicalType: "uuid" };
const DURATION_FIXED = {
  type: "fixed",
  name: "Term",
  size: 12,
  logicalType: "duration",
};

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

  it("rejects unsafe defaults before the public JSON round trip can round them", () => {
    for (const value of [9007199254740993n, -9007199254740993n]) {
      const type = createType(withDefault("long", value) as never);
      assertThrows(
        () => createType(JSON.parse(JSON.stringify(type))),
        Error,
        "Cannot write the default of field 'R.f' to schema JSON: long " +
          `${value} is outside the safe integer range`,
      );
    }
  });

  it("names the field when a long default cannot be written exactly", () => {
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
        "9007199254740993 is outside the safe integer range",
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

  it("round trips logical defaults given as runtime values", () => {
    assertDefaultRoundTrip(withDefault(TIMESTAMP, new Date(1000)), 1000);
    assertDefaultRoundTrip(withDefault(DECIMAL_BYTES, 482n), "\u0001\u00e2");
    assertDefaultRoundTrip(
      withDefault(DECIMAL_FIXED, 482n),
      "\u0000".repeat(6) + "\u0001\u00e2",
    );
    assertDefaultRoundTrip(withDefault(UUID_FIXED, UUID), UUID_JSON);
    assertDefaultRoundTrip(
      withDefault(DURATION_FIXED, DURATION),
      DURATION_JSON,
    );
  });

  it("round trips logical defaults given as their underlying JSON", () => {
    assertDefaultRoundTrip(withDefault(TIMESTAMP, 1000), 1000);
    assertDefaultRoundTrip(
      withDefault(DECIMAL_BYTES, "\u0001\u00e2"),
      "\u0001\u00e2",
    );
    assertDefaultRoundTrip(withDefault(UUID_FIXED, UUID_JSON), UUID_JSON);
    assertDefaultRoundTrip(
      withDefault(DURATION_FIXED, DURATION_JSON),
      DURATION_JSON,
    );
  });

  it("round trips logical defaults nested in a union and an array", () => {
    assertDefaultRoundTrip(
      withDefault(["null", TIMESTAMP], { long: new Date(1000) }),
      { long: 1000 },
    );
    assertDefaultRoundTrip(
      withDefault({ type: "array", items: DECIMAL_BYTES }, [482n]),
      ["\u0001\u00e2"],
    );
  });

  it("rejects unsafe JSON long literals in every numeric spelling", () => {
    for (
      const literal of [
        "9007199254740993",
        "9007199254740993.0",
        "9007199254740993e0",
        "-9007199254740993",
      ]
    ) {
      const json = '{"type":"record","name":"R","fields":[' +
        `{"name":"f","type":"long","default":${literal}}]}`;
      assertThrows(
        () => createType(JSON.parse(json)).toJSON(),
        Error,
        "not a safe integer JSON number",
      );
    }
  });

  it("preserves __proto__ in nested record defaults, including filled defaults", () => {
    const inner = record("In", [{
      name: "__proto__",
      type: "int",
      default: 7,
    }]);
    assertDefaultRoundTrip(
      withDefault(inner, JSON.parse('{"__proto__":3}')),
      JSON.parse('{"__proto__":3}'),
    );
    assertDefaultRoundTrip(
      withDefault(inner, {}),
      JSON.parse('{"__proto__":7}'),
    );
  });

  it("compares nested map defaults independently of insertion order", () => {
    const config = (value: unknown): Type =>
      createType(record("Config", [{
        name: "m",
        type: { type: "map", values: { type: "map", values: "int" } },
        default: value,
      }]) as never);
    const first = config(
      new Map([["b", new Map([["y", 2], ["x", 1]])], [
        "a",
        new Map([["z", 3]]),
      ]]),
    );
    const second = config(
      new Map([["a", new Map([["z", 3]])], [
        "b",
        new Map([["x", 1], ["y", 2]]),
      ]]),
    );
    const outer = createRecord({
      name: "Outer",
      fields: [{ name: "a", type: first }, { name: "b", type: second }],
    });
    assertEquals(
      (outer.toJSON() as { fields: Array<{ type: unknown }> }).fields[1].type,
      "Config",
    );
    assertEquals(
      JSON.stringify(first).includes(
        '"default":{"b":{"y":2,"x":1},"a":{"z":3}}',
      ),
      true,
    );
  });

  it("compares distinct Unicode map keys independently of locale collation", () => {
    const config = (value: Record<string, number>): Type =>
      createType(
        record("Config", [{
          name: "m",
          type: { type: "map", values: "int" },
          default: value,
        }]) as never,
      );
    const first = config({ "é": 1, "e\u0301": 2 });
    const second = config({ "e\u0301": 2, "é": 1 });
    const outer = createRecord({
      name: "Outer",
      fields: [{ name: "a", type: first }, { name: "b", type: second }],
    });
    assertEquals(
      (outer.toJSON() as { fields: Array<{ type: unknown }> }).fields[1].type,
      "Config",
    );
  });

  it("keeps array-default order significant when comparing named types", () => {
    const config = (value: number[]): Type =>
      createType(
        record("Config", [{
          name: "a",
          type: { type: "array", items: "int" },
          default: value,
        }]) as never,
      );
    const outer = createRecord({
      name: "Outer",
      fields: [{ name: "a", type: config([1, 2]) }, {
        name: "b",
        type: config([2, 1]),
      }],
    });
    assertThrows(
      () => outer.toJSON(),
      Error,
      "Duplicate Avro type name: Config",
    );
  });

  it("rejects numeric JSON decimal defaults while accepting runtime bigints", () => {
    const decimal = { type: "bytes", logicalType: "decimal", precision: 20 };
    const json = JSON.stringify(withDefault(decimal, 9007199254740992));
    assertThrows(() => createType(JSON.parse(json)).toJSON());
    const type = createType(
      withDefault(decimal, 9007199254740993n) as never,
    ) as RecordType;
    assertEquals(type.getField("f")!.getDefault(), 9007199254740993n);
  });

  it("reads back an integral double default above 2^53 unchanged", () => {
    for (const type of ["double", "float"]) {
      const first = createType(withDefault(type, 1e20) as never);
      const json = JSON.stringify(first);
      assertEquals(json.includes('"default":100000000000000000000'), true);
      const second = createType(JSON.parse(json)) as RecordType;

      assertEquals(JSON.stringify(second), json);
      assertEquals(
        second.getField("f")!.getDefault(),
        (first as RecordType).getField("f")!.getDefault(),
      );
    }
  });

  it("rejects unsafe defaults nested in arrays, maps, records, unions and logical types", () => {
    const value = 9007199254740993n;
    const cases: Array<[unknown, unknown]> = [
      [{ type: "array", items: "long" }, [value]],
      [{ type: "map", values: "long" }, { k: value }],
      [record("In", [{ name: "n", type: "long" }]), { n: value }],
      [["long", "null"], { long: value }],
      [{ type: "long", logicalType: "timestamp-micros" }, value],
    ];
    for (const [type, defaultValue] of cases) {
      const schema = createType(withDefault(type, defaultValue) as never);
      assertThrows(
        () => schema.toJSON(),
        Error,
        "Cannot write the default of field 'R.f' to schema JSON: long " +
          `${value} is outside the safe integer range`,
      );
    }
  });
});
