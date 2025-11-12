import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { _safeJSONStringify, safeStringify } from "./json.ts";

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
