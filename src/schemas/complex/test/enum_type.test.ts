import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../../serialization/sync_tap.ts";
import { EnumType } from "../enum_type.ts";
import { resolveNames } from "../resolve_names.ts";
import { IntType } from "../../primitive/int_type.ts";

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
  describe("constructor validation", () => {
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
  });

  describe("default values", () => {
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
  });

  describe("serialization", () => {
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
      tap._testOnlyResetPos();
      await assertRejects(async () => await type.read(tap));
    });

    it("throws when readSync encounters an invalid index", async () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A"],
      });
      const buffer = new ArrayBuffer(8);
      const writeTap = new Tap(buffer);
      await writeTap.writeLong(5n);
      const encoded = buffer.slice(0, writeTap.getPos());
      const syncTap = new SyncReadableTap(encoded);
      assertThrows(
        () => type.readSync(syncTap),
        Error,
        "Invalid enum index",
      );
    });
  });

  describe("validation", () => {
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
  });

  describe("skip operations", () => {
    it("skips encoded values", async () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B", "C"],
      });
      const buffer = await type.toBuffer("C");
      const tap = new Tap(buffer);
      await type.skip(tap);
      assertEquals(tap.getPos(), buffer.byteLength);
    });

    it("skips encoded values via sync taps", () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B", "C"],
      });
      const buffer = type.toSyncBuffer("C");
      const tap = new SyncReadableTap(buffer);
      type.skipSync(tap);
      assertEquals(tap.getPos(), buffer.byteLength);
    });
  });

  describe("sync operations", () => {
    it("round-trips via sync buffer", () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B", "C"],
      });
      const buffer = type.toSyncBuffer("B");
      assertEquals(type.fromSyncBuffer(buffer), "B");
    });

    it("reads and writes via sync taps", () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B", "C"],
      });
      const buffer = new ArrayBuffer(16);
      const writeTap = new SyncWritableTap(buffer);
      type.writeSync(writeTap, "A");
      const readTap = new SyncReadableTap(buffer);
      assertEquals(type.readSync(readTap), "A");
      assertEquals(readTap.getPos(), writeTap.getPos());
    });

    it("throws when writeSync receives an unknown symbol", () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B", "C"],
      });
      const buffer = new ArrayBuffer(16);
      const tap = new SyncWritableTap(buffer);
      assertThrows(
        () => type.writeSync(tap, "Z"),
        Error,
        "Invalid value",
      );
    });

    it("matches encoded enum buffers via sync taps", () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B", "C"],
      });
      const bufA = type.toSyncBuffer("A");
      const bufB = type.toSyncBuffer("B");
      assertEquals(
        type.matchSync(new SyncReadableTap(bufA), new SyncReadableTap(bufB)),
        -1,
      );
      assertEquals(
        type.matchSync(new SyncReadableTap(bufA), new SyncReadableTap(bufA)),
        0,
      );
    });
  });

  describe("schema resolution", () => {
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

    it("reads resolver values synchronously when symbols match", () => {
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
      const buffer = writer.toSyncBuffer("A");
      assertEquals(resolver.readSync(new SyncReadableTap(buffer)), "A");
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

    it("reads resolver values synchronously using default branch", () => {
      const reader = createEnum({
        name: "Letter",
        symbols: ["A", "B", "D"],
        default: "D",
      });
      const writer = createEnum({
        name: "Letter",
        symbols: ["X", "Y"],
      });
      const resolver = reader.createResolver(writer);
      const bufferX = writer.toSyncBuffer("X");
      assertEquals(resolver.readSync(new SyncReadableTap(bufferX)), "D");
    });

    it("reads resolver values synchronously with mixed symbol support", () => {
      const reader = createEnum({
        name: "Letter",
        symbols: ["A", "B", "D"],
        default: "D",
      });
      const writer = createEnum({
        name: "Letter",
        symbols: ["A", "X"],
      });
      const resolver = reader.createResolver(writer);

      // Test case where writer symbol IS in reader indices (A)
      const bufferA = writer.toSyncBuffer("A");
      assertEquals(resolver.readSync(new SyncReadableTap(bufferA)), "A");

      // Test case where writer symbol is NOT in reader indices (X) - falls back to default
      const bufferX = writer.toSyncBuffer("X");
      assertEquals(resolver.readSync(new SyncReadableTap(bufferX)), "D");
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
  });

  describe("cloning and comparison", () => {
    it("clones valid values", () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B"],
      });
      assertEquals(type.cloneFromValue("A"), "A");
      assertThrows(() => type.cloneFromValue("C"));
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
  });

  describe("match operations", () => {
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
  });

  describe("error handling", () => {
    it("calls super createResolver for non-enum types", () => {
      const type = createEnum({
        name: "Test",
        symbols: ["A"],
      });
      const intType = new IntType();
      assertThrows(() => type.createResolver(intType));
    });
  });

  describe("validation disabled", () => {
    it("writes valid values without validation", async () => {
      const resolved = resolveNames({ name: "Letter" });
      const type = new EnumType({
        ...resolved,
        symbols: ["A", "B", "C"],
        validate: false,
      });
      const buffer = new ArrayBuffer(16);
      const tap = new Tap(buffer);
      await type.write(tap, "B");
      tap._testOnlyResetPos();
      const value = await type.read(tap);
      assertEquals(value, "B");
    });

    it("throws when writing invalid values even without validation", async () => {
      const resolved = resolveNames({ name: "Letter" });
      const type = new EnumType({
        ...resolved,
        symbols: ["A", "B"],
        validate: false,
      });
      const buffer = new ArrayBuffer(16);
      const tap = new Tap(buffer);
      await assertRejects(
        async () => await type.write(tap, "Z"),
        Error,
        "Invalid value",
      );
    });

    it("writes valid values synchronously without validation", () => {
      const resolved = resolveNames({ name: "Letter" });
      const type = new EnumType({
        ...resolved,
        symbols: ["A", "B", "C"],
        validate: false,
      });
      const buffer = new ArrayBuffer(16);
      const writeTap = new SyncWritableTap(buffer);
      type.writeSync(writeTap, "B");
      const readTap = new SyncReadableTap(buffer);
      const value = type.readSync(readTap);
      assertEquals(value, "B");
    });

    it("throws when writeSync receives invalid value even without validation", () => {
      const resolved = resolveNames({ name: "Letter" });
      const type = new EnumType({
        ...resolved,
        symbols: ["A", "B"],
        validate: false,
      });
      const buffer = new ArrayBuffer(16);
      const tap = new SyncWritableTap(buffer);
      assertThrows(
        () => type.writeSync(tap, "Z"),
        Error,
        "Invalid value",
      );
    });

    it("toBuffer works without validation for valid values", async () => {
      const resolved = resolveNames({ name: "Letter" });
      const type = new EnumType({
        ...resolved,
        symbols: ["A", "B"],
        validate: false,
      });
      const buffer = await type.toBuffer("A");
      const value = await type.fromBuffer(buffer);
      assertEquals(value, "A");
    });

    it("toBuffer throws for invalid values even without validation", async () => {
      const resolved = resolveNames({ name: "Letter" });
      const type = new EnumType({
        ...resolved,
        symbols: ["A", "B"],
        validate: false,
      });
      await assertRejects(
        async () => await type.toBuffer("Z"),
        Error,
        "Invalid value",
      );
    });

    it("toSyncBuffer works without validation for valid values", () => {
      const resolved = resolveNames({ name: "Letter" });
      const type = new EnumType({
        ...resolved,
        symbols: ["A", "B"],
        validate: false,
      });
      const buffer = type.toSyncBuffer("A");
      const value = type.fromSyncBuffer(buffer);
      assertEquals(value, "A");
    });

    it("toSyncBuffer throws for invalid values even without validation", () => {
      const resolved = resolveNames({ name: "Letter" });
      const type = new EnumType({
        ...resolved,
        symbols: ["A", "B"],
        validate: false,
      });
      assertThrows(
        () => type.toSyncBuffer("Z"),
        Error,
        "Invalid value",
      );
    });
  });

  describe("unchecked write methods", () => {
    it("writeUnchecked writes valid values", async () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B", "C"],
      });
      const buffer = new ArrayBuffer(16);
      const writeTap = new Tap(buffer);
      await type.writeUnchecked(writeTap, "C");
      const readTap = new Tap(buffer);
      const value = await type.read(readTap);
      assertEquals(value, "C");
    });

    it("writeUnchecked throws for invalid values", async () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B"],
      });
      const buffer = new ArrayBuffer(16);
      const tap = new Tap(buffer);
      await assertRejects(
        async () => await type.writeUnchecked(tap, "Z"),
        Error,
        "Invalid value",
      );
    });

    it("writeSyncUnchecked writes valid values", () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B", "C"],
      });
      const buffer = new ArrayBuffer(16);
      const writeTap = new SyncWritableTap(buffer);
      type.writeSyncUnchecked(writeTap, "C");
      const readTap = new SyncReadableTap(buffer);
      const value = type.readSync(readTap);
      assertEquals(value, "C");
    });

    it("writeSyncUnchecked throws for invalid values", () => {
      const type = createEnum({
        name: "Letter",
        symbols: ["A", "B"],
      });
      const buffer = new ArrayBuffer(16);
      const tap = new SyncWritableTap(buffer);
      assertThrows(
        () => type.writeSyncUnchecked(tap, "Z"),
        Error,
        "Invalid value",
      );
    });
  });
});
