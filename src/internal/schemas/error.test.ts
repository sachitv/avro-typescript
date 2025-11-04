import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  renderPathAsTree,
  throwInvalidError,
  ValidationError,
} from "./error.ts";
import { LongType } from "./long_type.ts";
import { DoubleType } from "./double_type.ts";

describe("ValidationError", () => {
  it("includes bigint value with n suffix", () => {
    const type = new LongType();
    const err = new ValidationError([], 123n, type);
    assertEquals(err.message, "Invalid value: '123' for type: long");
  });

  it("falls back to string representation when JSON serialization fails", () => {
    const circular: { self?: unknown } = {};
    circular.self = circular;
    const type = new DoubleType();
    const err = new ValidationError([], circular, type);
    assertEquals(
      err.message,
      `Invalid value: '
{
  "self": "[Circular]"
}
' for type: double`,
    );
  });

  it("uses String(value) when JSON serialization yields undefined", () => {
    const type = new DoubleType();
    const symbolValue = Symbol("token");
    const err = new ValidationError([], symbolValue, type);
    assertEquals(
      err.message,
      "Invalid value: 'Symbol(token)' for type: double",
    );
  });

  it("serializes valid JSON objects", () => {
    const type = new DoubleType();
    const obj = { a: 1, b: "test" };
    const err = new ValidationError([], obj, type);
    assertEquals(
      err.message,
      `Invalid value: '
{
  "a": 1,
  "b": "test"
}
' for type: double`,
    );
  });
});

describe("throwInvalidError", () => {
  it("throws a ValidationError carrying path, value, and type", () => {
    const type = new DoubleType();
    const err = assertThrows(
      () => {
        throwInvalidError(["field"], "bad", type);
      },
      ValidationError,
      `Invalid value: 'bad' for type: double at path: field`,
    );

    assertEquals(err.path, ["field"]);
    assertEquals(err.value, "bad");
    assertEquals(err.type, type);
  });
});

describe("renderPathAsTree", () => {
  it("renders empty path as empty string", () => {
    assertEquals(renderPathAsTree([]), "");
  });

  it("renders single element path", () => {
    assertEquals(renderPathAsTree(["root"]), "root");
  });

  it("renders multi-element path with indentation", () => {
    assertEquals(
      renderPathAsTree(["root", "child", "leaf"]),
      `
root
  child
    leaf`,
    );
  });
});
