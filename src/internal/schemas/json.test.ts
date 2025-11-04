import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { safeJsonStringify, safeStringify } from "./json.ts";

describe("safeJsonStringify", () => {
  it("should stringify normal objects", () => {
    const obj = { a: 1, b: "test" };
    expect(safeJsonStringify(obj)).toBe(
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
    const result = safeJsonStringify(obj);
    expect(result).toContain('"self": "[Circular]"');
  });

  it("should throw for bigint", () => {
    expect(() => safeJsonStringify(123n)).toThrow();
  });

  it("should return undefined for functions", () => {
    const func = () => {};
    expect(safeJsonStringify(func)).toBeUndefined();
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

  it("should handle functions", () => {
    const func = () => {};
    const result = safeStringify(func);
    expect(result).toBe(String(func));
  });
});
