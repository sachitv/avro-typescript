import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { TestTap as Tap } from "../serialization/test_tap.ts";
import { EnumType } from "./enum_type.ts";
import { resolveNames } from "./resolve_names.ts";
import { IntType } from "./int_type.ts";

function createEnum(params: {
  name: string;
  namespace?: string;
  aliases?: string[];
  symbols: string[];
  default?: string;
}): EnumType {
  const { symbols, default: defaultValue, ...nameInfo } = params;
  const resolved = resolveNames(nameInfo);
  return new EnumType({ ...resolved, symbols, default: defaultValue });
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

  it("rejects default not in symbols", () => {
    assertThrows(
      () =>
        createEnum({
          name: "Test",
          symbols: ["A", "B"],
          default: "C",
        }),
      Error,
      "Default value must be a member of the symbols array.",
    );
  });

  it("returns default value when set", () => {
    const type = createEnum({
      name: "Test",
      symbols: ["A", "B"],
      default: "A",
    });
    assertEquals(type.getDefault(), "A");
  });

  it("returns undefined when no default", () => {
    const type = createEnum({
      name: "Test",
      symbols: ["A", "B"],
    });
    assertEquals(type.getDefault(), undefined);
  });

  it("serializes and deserializes values", async () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A", "B", "C"],
    });

    const buffer = await type.toBuffer("B");
    const value = await type.fromBuffer(buffer);

    assertEquals(value, "B");
  });

  it("throws when writing an unknown value", async () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A"],
    });
    await assertRejects(async () => await type.toBuffer("B"));
  });

  it("throws when reading an out-of-range index", async () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A"],
    });
    const tap = new Tap(new ArrayBuffer(1));
    await tap.writeLong(2n);
    tap.resetPos();
    await assertRejects(async () => await type.read(tap));
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

  it("skips encoded values", async () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A", "B", "C"],
    });
    const buffer = await type.toBuffer("C");
    const tap = new Tap(buffer);
    await type.skip(tap);
    assertEquals(tap._testOnlyPos, buffer.byteLength);
  });

  it("creates resolvers when writer symbols are compatible", async () => {
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
    const buffer = await writer.toBuffer("A");
    const tap = new Tap(buffer);

    assertEquals(await resolver.read(tap), "A");
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

  it("throws when writer name is not acceptable", () => {
    const reader = createEnum({
      name: "Letter",
      symbols: ["A", "B"],
    });
    const writer = createEnum({
      name: "Number",
      symbols: ["One", "Two"],
    });

    assertThrows(() => reader.createResolver(writer));
  });

  it("creates resolvers with default when writer symbols are partially compatible", async () => {
    const reader = createEnum({
      name: "Letter",
      symbols: ["A", "B", "D"],
      default: "D",
    });
    const writer = createEnum({
      name: "Letter",
      symbols: ["A", "C"],
    });

    const resolver = reader.createResolver(writer);
    const bufferA = await writer.toBuffer("A");
    const tapA = new Tap(bufferA);
    assertEquals(await resolver.read(tapA), "A");

    const bufferC = await writer.toBuffer("C");
    const tapC = new Tap(bufferC);
    assertEquals(await resolver.read(tapC), "D");
  });

  it("uses default for all unknown writer symbols", async () => {
    const reader = createEnum({
      name: "Letter",
      symbols: ["A", "B"],
      default: "B",
    });
    const writer = createEnum({
      name: "Letter",
      symbols: ["X", "Y"],
    });

    const resolver = reader.createResolver(writer);
    const bufferX = await writer.toBuffer("X");
    const tapX = new Tap(bufferX);
    assertEquals(await resolver.read(tapX), "B");

    const bufferY = await writer.toBuffer("Y");
    const tapY = new Tap(bufferY);
    assertEquals(await resolver.read(tapY), "B");
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

  it("throws when writing invalid value directly", async () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A"],
    });
    const buffer = new ArrayBuffer(1);
    const tap = new Tap(buffer);
    await assertRejects(
      async () => await type.write(tap, "B"),
      Error,
      "Invalid value: 'B' for type: enum",
    );
  });

  it("should match encoded enum buffers correctly", async () => {
    const type = createEnum({
      name: "Letter",
      symbols: ["A", "B", "C"],
    });

    const buf1 = await type.toBuffer("A"); // index 0
    const buf2 = await type.toBuffer("B"); // index 1
    const buf3 = await type.toBuffer("C"); // index 2

    assertEquals(await type.match(new Tap(buf1), new Tap(buf1)), 0); // A == A
    assertEquals(await type.match(new Tap(buf1), new Tap(buf2)), -1); // A < B
    assertEquals(await type.match(new Tap(buf2), new Tap(buf1)), 1); // B > A
    assertEquals(await type.match(new Tap(buf1), new Tap(buf3)), -1); // A < C
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
