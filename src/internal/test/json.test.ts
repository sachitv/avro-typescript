import { assertEquals, assertStrictEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { parseJSON } from "../json.ts";

describe("parseJSON", () => {
  it("parses like JSON.parse when every number is safe", () => {
    const texts = [
      "null",
      "true",
      '"text"',
      "0",
      "-0",
      "1.5",
      "1e3",
      "[]",
      "{}",
      '{"a":[1,-2.5,1e3,"9007199254740993"],"b":null,"c":{"d":false}}',
    ];
    for (const text of texts) {
      assertEquals(parseJSON(text), JSON.parse(text));
    }
  });

  it("keeps integers at the edge of the safe range as numbers", () => {
    assertStrictEquals(parseJSON("9007199254740991"), 9007199254740991);
    assertStrictEquals(parseJSON("-9007199254740991"), -9007199254740991);
  });

  it("reads integer literals outside the safe range as exact bigints", () => {
    assertStrictEquals(parseJSON("9007199254740992"), 9007199254740992n);
    assertStrictEquals(parseJSON("9007199254740993"), 9007199254740993n);
    assertStrictEquals(parseJSON("-9007199254740993"), -9007199254740993n);
    assertStrictEquals(
      parseJSON("9223372036854775807"),
      9223372036854775807n,
    );
    assertStrictEquals(
      parseJSON("-9223372036854775808"),
      -9223372036854775808n,
    );
    // JSON numbers have no size limit; digits beyond int64 are kept too.
    assertStrictEquals(
      parseJSON("123456789012345678901234567890"),
      123456789012345678901234567890n,
    );
  });

  it("reads them wherever they are nested", () => {
    assertEquals(
      parseJSON('{"a":[9007199254740993,{"b":-9007199254740993}],"c":1}'),
      { a: [9007199254740993n, { b: -9007199254740993n }], c: 1 },
    );
  });

  it("keeps non-integer literals as numbers, however large", () => {
    assertStrictEquals(parseJSON("1e20"), 1e20);
    assertStrictEquals(parseJSON("1E+20"), 1e20);
    assertStrictEquals(parseJSON("9007199254740993.5"), 9007199254740993.5);
    assertStrictEquals(parseJSON("9007199254740993.0"), 9007199254740992);
  });

  it("leaves strings of digits as strings", () => {
    assertStrictEquals(parseJSON('"9007199254740993"'), "9007199254740993");
    assertEquals(parseJSON('{"9007199254740993":1}'), {
      "9007199254740993": 1,
    });
  });

  it("throws on invalid JSON as JSON.parse does", () => {
    assertThrows(() => parseJSON("{"), SyntaxError);
    assertThrows(() => parseJSON("NaN"), SyntaxError);
  });

  it("reads numbers as JSON.parse does without the reviver source", () => {
    const parse = JSON.parse;
    // A runtime without ES2025 source text access passes no context.
    JSON.parse = (text: string, reviver?: (k: string, v: unknown) => unknown) =>
      parse(text, reviver && ((k: string, v: unknown) => reviver(k, v)));
    try {
      assertStrictEquals(parseJSON("9007199254740993"), 9007199254740992);
      assertEquals(parseJSON("[1,2]"), [1, 2]);
    } finally {
      JSON.parse = parse;
    }
  });
});
