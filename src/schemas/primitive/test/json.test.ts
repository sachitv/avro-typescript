import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { _safeJSONStringify, parseJSON, safeStringify } from "../../json.ts";

describe("_safeJSONStringify", () => {
  it("should stringify normal objects", () => {
    const obj = { a: 1, b: "test" };
    expect(_safeJSONStringify(obj)).toBe(
      `{
  "a": 1,
  "b": "test"
}`,
    );
  });

  it("should handle circular references", () => {
    // deno-lint-ignore no-explicit-any
    const obj: any = { a: 1 };
    obj.self = obj;
    const result = _safeJSONStringify(obj);
    expect(result).toContain('"self": "[Circular]"');
  });

  it("should handle bigint", () => {
    expect(_safeJSONStringify(123n)).toBe('"123"');
  });

  it("should return undefined for functions", () => {
    const func = () => {};
    expect(_safeJSONStringify(func)).toBeUndefined();
  });
});

describe("safeStringify", () => {
  it("should stringify normal objects", () => {
    const obj = { a: 1, b: "test" };
    expect(safeStringify(obj)).toBe(
      `
{
  "a": 1,
  "b": "test"
}
`,
    );
  });

  it("should return string as is", () => {
    expect(safeStringify("hello world")).toBe("hello world");
  });

  it("should handle circular references", () => {
    // deno-lint-ignore no-explicit-any
    const obj: any = { a: 1 };
    obj.self = obj;
    const result = safeStringify(obj);
    expect(result).toContain('"self": "[Circular]"');
  });

  it("should handle bigint", () => {
    expect(safeStringify(123n)).toBe("123");
  });

  it("should handle number", () => {
    expect(safeStringify(42)).toBe("42");
  });

  it("should handle boolean", () => {
    expect(safeStringify(true)).toBe("true");
    expect(safeStringify(false)).toBe("false");
  });

  it("should handle null", () => {
    expect(safeStringify(null)).toBe("null");
  });

  it("should handle undefined", () => {
    expect(safeStringify(undefined)).toBe("undefined");
  });

  it("should handle functions", () => {
    const func = () => {};
    const result = safeStringify(func);
    expect(result).toBe(String(func));
  });
});

describe("parseJSON", () => {
  it("parses like JSON.parse when every number is safe", () => {
    const text = '{"a":[1,-2.5,1e3,"9007199254740993"],"b":null,"c":true}';
    expect(parseJSON(text)).toEqual(JSON.parse(text));
  });

  it("reads integer literals outside the safe range as exact bigints", () => {
    expect(parseJSON("9007199254740993")).toBe(9007199254740993n);
    expect(parseJSON("-9223372036854775808")).toBe(-9223372036854775808n);
    expect(parseJSON("9007199254740992")).toBe(9007199254740992n);
    expect(parseJSON('{"d":[9007199254740993,{"e":-9007199254740993}]}'))
      .toEqual({ d: [9007199254740993n, { e: -9007199254740993n }] });
  });

  it("keeps safe integers and non-integer literals as numbers", () => {
    expect(parseJSON("9007199254740991")).toBe(9007199254740991);
    expect(parseJSON("1e20")).toBe(1e20);
    expect(parseJSON("9007199254740993.5")).toBe(9007199254740993.5);
  });

  it("reads numbers as JSON.parse does without the reviver source", () => {
    const parse = JSON.parse;
    // A runtime without ES2025 source text access passes no context.
    JSON.parse = (text: string, reviver?: (k: string, v: unknown) => unknown) =>
      parse(text, reviver && ((k: string, v: unknown) => reviver(k, v)));
    try {
      expect(parseJSON("9007199254740993")).toBe(9007199254740992);
    } finally {
      JSON.parse = parse;
    }
  });
});
