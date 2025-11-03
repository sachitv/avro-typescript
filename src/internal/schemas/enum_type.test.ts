import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { Tap } from "../serialization/tap.ts";
import { EnumType } from "./enum_type.ts";
import { resolveNames } from "./resolve_names.ts";
import { IntType } from "./int_type.ts";

function createEnum(params: {
  name: string;
  namespace?: string;
  aliases?: string[];
  symbols: string[];
}): EnumType {
  const { symbols, ...nameInfo } = params;
  const resolved = resolveNames(nameInfo);
  return new EnumType({ ...resolved, symbols });
}

describe("EnumType", () => {
  it("requires a non-empty symbols array", () => {
    assertThrows(
      () =>
        createEnum({
          name: "Empty",
          symbols: [],
        }),
      Error,
      "EnumType requires a non-empty symbols array.",
    );
  });

  it("rejects invalid symbol names", () => {
    assertThrows(
      () =>
        createEnum({
          name: "InvalidSymbol",
          symbols: ["0BAD"],
        }),
      Error,
      "Invalid enum symbol",
    );
  });

  it("rejects duplicate symbols", () => {
    assertThrows(
      () =>
        createEnum({
          name: "Dup",
          symbols: ["A", "B", "A"],
        }),
      Error,
      "Duplicate enum symbol: A",
    );
  });

  it("serializes and deserializes values", () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A", "B", "C"],
    });

    const buffer = type.toBuffer("B");
    const value = type.fromBuffer(buffer);

    assertEquals(value, "B");
  });

  it("throws when writing an unknown value", () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A"],
    });
    assertThrows(() => type.toBuffer("B"));
  });

  it("throws when reading an out-of-range index", () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A"],
    });
    const tap = new Tap(new ArrayBuffer(1));
    tap.writeLong(2n);
    tap.resetPos();
    assertThrows(() => type.read(tap));
  });

  it("validates values and triggers error hooks", () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A", "B"],
    });

    let called = false;
    type.isValid("A");
    type.isValid("C", {
      errorHook: (path, value, schema) => {
        called = true;
        assertEquals(path, []);
        assertEquals(value, "C");
        assert(schema === type);
      },
    });
    assert(called);
  });

  it("returns defensive copies for symbols", () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A", "B"],
    });

    const symbols = type.getSymbols();
    assertEquals(symbols, ["A", "B"]);
    symbols.push("C");
    assertEquals(type.getSymbols(), ["A", "B"]);
  });

  it("creates resolvers when writer symbols are compatible", () => {
    const reader = createEnum({
      name: "Letter",
      symbols: ["A", "B", "C"],
      aliases: ["letters"],
    });
    const writer = createEnum({
      name: "letters",
      symbols: ["C", "A"],
    });

    const resolver = reader.createResolver(writer);
    const buffer = writer.toBuffer("A");
    const tap = new Tap(buffer);

    assertEquals(resolver.read(tap), "A");
  });

  it("throws when writer symbols are incompatible", () => {
    const reader = createEnum({
      name: "Letter",
      symbols: ["A", "B"],
    });
    const writer = createEnum({
      name: "Letter",
      symbols: ["A", "C"],
    });

    assertThrows(() => reader.createResolver(writer));
  });

  it("clones valid values", () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A", "B"],
    });
    assertEquals(type.clone("A"), "A");
    assertThrows(() => type.clone("C"));
  });

  it("compares according to symbol order", () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A", "B"],
    });
    assertEquals(type.compare("A", "B"), -1);
    assertEquals(type.compare("B", "A"), 1);
  });

  it("generates random values from the symbol set", () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A", "B", "C"],
    });
    const value = type.random();
    assert(type.getSymbols().includes(value));
  });

  it("throws when comparing invalid values", () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A", "B"],
    });
    assertThrows(
      () => type.compare("A", "C"),
      Error,
      "Cannot compare values not present in the enum.",
    );
  });

  it("throws when writing invalid value directly", () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A"],
    });
    const buffer = new ArrayBuffer(1);
    const tap = new Tap(buffer);
    assertThrows(
      () => type.write(tap, "B"),
      Error,
      'Invalid value: "B" for type: enum',
    );
  });

  it("calls super createResolver for non-enum types", () => {
    const type = createEnum({
      name: "Test",
      symbols: ["A"],
    });
    const intType = new IntType();
    assertThrows(() => type.createResolver(intType));
  });
});
