import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { ValidationError, throwInvalidError } from "./error.ts";
import { LongType } from "./long_type.ts";
import { DoubleType } from "./double_type.ts";

describe("ValidationError", () => {
  it("includes bigint value with n suffix", () => {
    const type = new LongType();
    const err = new ValidationError([], 123n, type);
    assertEquals(err.message, "Invalid value: 123n for type: LongType");
  });

  it("falls back to string representation when JSON serialization fails", () => {
    const circular: { self?: unknown } = {};
    circular.self = circular;
    const type = new DoubleType();
    const err = new ValidationError([], circular, type);
    assertEquals(err.message, "Invalid value: [object Object] for type: DoubleType");
  });

  it("uses String(value) when JSON serialization yields undefined", () => {
    const type = new DoubleType();
    const symbolValue = Symbol("token");
    const err = new ValidationError([], symbolValue, type);
    assertEquals(err.message, "Invalid value: Symbol(token) for type: DoubleType");
  });
});

describe("throwInvalidError", () => {
  it("throws a ValidationError carrying path, value, and type", () => {
    const type = new DoubleType();
    const err = assertThrows(() => {
      throwInvalidError(["field"], "bad", type);
    }, ValidationError, 'Invalid value: "bad" for type: DoubleType');

    assertEquals(err.path, ["field"]);
    assertEquals(err.value, "bad");
    assertEquals(err.type, type);
  });
});
