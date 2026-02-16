import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { ReadBufferError } from "../../../serialization/buffers/buffer_error.ts";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import { ReadableTap } from "../../../serialization/tap.ts";
import {
  SyncReadableTap,
  type SyncReadableTapLike,
  SyncWritableTap,
} from "../../../serialization/tap_sync.ts";
import { DirectSyncReadableTap } from "../../../serialization/direct_tap_sync.ts";
import { MapType, readMapInto, readMapIntoSync } from "../map_type.ts";
import { IntType } from "../../primitive/int_type.ts";
import { LongType } from "../../primitive/long_type.ts";
import { FloatType } from "../../primitive/float_type.ts";
import { StringType } from "../../primitive/string_type.ts";
import { BytesType } from "../../primitive/bytes_type.ts";
import { NullType } from "../../primitive/null_type.ts";
import { UnionType, type UnionValue } from "../union_type.ts";
import { createType } from "../../../type/create_type.ts";
import type { Type } from "../../type.ts";

function createMap<T>(values: Type<T>): MapType<T> {
  return new MapType({ values });
}

describe("MapType", () => {
  const intValues = new IntType();
  const intMap = createMap(intValues);

  describe("constructor", () => {
    it("throws error when constructor receives falsy values", () => {
      assertThrows(
        () =>
          new MapType({
            values: undefined as unknown as Type<unknown>,
          }),
        Error,
        "MapType requires a values type.",
      );
    });
  });

  describe("check()", () => {
    it("validates valid maps", () => {
      const map = new Map([["a", 1], ["b", 2]]);
      assert(intMap.check(map));
    });

    it("validates valid nested maps", () => {
      const nestedMap = createMap(intMap);
      const map = new Map([
        ["outer1", new Map([["inner1", 1], ["inner2", 2]])],
        ["outer2", new Map([["inner3", 3]])],
      ]);
      assert(nestedMap.check(map));
    });

    it("invalidates non-map values", () => {
      assert(!intMap.check("hello"));
    });

    it("invalidates invalid nested maps", () => {
      const nestedMap = createMap(intMap);
      const map = new Map([
        [
          "outer1",
          new Map([["inner1", 1], ["inner2", "invalid" as unknown as number]]),
        ],
      ]);
      assert(!nestedMap.check(map));
    });

    it("invalidates maps with non-string keys", () => {
      const map = new Map([[1 as unknown as string, 1]]);
      assert(!intMap.check(map));
    });

    it("invalidates maps with invalid values", () => {
      const map = new Map([["a", "invalid" as unknown as number]]);
      assert(!intMap.check(map));
    });

    // Tests that the error hook reports the path to an invalid map value
    it("reports path to invalid map value in error hook", () => {
      const paths: string[][] = [];
      const map = new Map([["a", "bad" as unknown as number]]);
      intMap.check(map, (path, value, type) => {
        assertEquals(type, intValues);
        assertEquals(value, "bad");
        paths.push(path);
      });
      assertEquals(paths, [["a"]]);
    });

    // Tests that the error hook reports an empty path for invalid map keys at the top level
    it("reports empty path for invalid map keys in error hook", () => {
      const paths: string[][] = [];
      const map = new Map([[1 as unknown as string, 1]]);
      intMap.check(map, (path, value, type) => {
        assertEquals(type, intMap);
        assertEquals(value, map);
        paths.push(path);
      });
      assertEquals(paths, [[]]);
    });

    // Tests that the error hook reports the full path to an invalid value in a nested map
    it("reports full path to invalid value in nested map via error hook", () => {
      const nestedMap = createMap(intMap);
      const paths: string[][] = [];
      const map = new Map([
        ["outer", new Map([["inner", "invalid" as unknown as number]])],
      ]);
      nestedMap.check(map, (path, value, type) => {
        assertEquals(type, intValues);
        assertEquals(value, "invalid");
        paths.push(path);
      });
      assertEquals(paths, [["outer", "inner"]]);
    });

    // Tests that the error hook reports the path to a nested map with invalid keys
    it("reports path to nested map with invalid keys via error hook", () => {
      const nestedMap = createMap(intMap);
      const paths: string[][] = [];
      const map = new Map([
        ["outer", new Map([[1 as unknown as string, 1]])],
      ]);
      nestedMap.check(map, (path, value, type) => {
        assertEquals(type, intMap);
        assertEquals(value, new Map([[1 as unknown as string, 1]]));
        paths.push(path);
      });
      assertEquals(paths, [["outer"]]);
    });

    it("calls error hook when checking non-map value", () => {
      const paths: string[][] = [];
      intMap.check("not a map", (path, value, type) => {
        assertEquals(type, intMap);
        assertEquals(value, "not a map");
        paths.push(path);
      });
      assertEquals(paths, [[]]);
    });

    it("returns false early when value check fails and no error hook", () => {
      const map = new Map([["a", "invalid" as unknown as number]]);
      assert(!intMap.check(map));
    });
  });

  describe("serialization", () => {
    it("serializes and deserializes using blocks", async () => {
      const map = new Map([["x", 1]]);
      const buffer = await intMap.toBuffer(map);
      const result = await intMap.fromBuffer(buffer);
      assertEquals(result, map);
    });

    it("writes without validation when validate=false", async () => {
      const noValidateMap = new MapType({ values: intValues, validate: false });
      const map = new Map([["a", 1], ["b", 2]]);
      const buffer = new ArrayBuffer(50);
      const tap = new Tap(buffer);
      await noValidateMap.write(tap, map);
      const readTap = new Tap(buffer);
      const result = await noValidateMap.read(readTap);
      assertEquals(result, map);
    });

    it("writes sync without validation when validate=false", () => {
      const noValidateMap = new MapType({ values: intValues, validate: false });
      const map = new Map([["a", 1], ["b", 2]]);
      const buffer = new ArrayBuffer(50);
      const tap = new SyncWritableTap(buffer);
      noValidateMap.writeSync(tap, map);
      const readTap = new SyncReadableTap(buffer);
      const result = noValidateMap.readSync(readTap);
      assertEquals(result, map);
    });

    it("round-trips via toBuffer/fromBuffer", async () => {
      const map = new Map([["key1", 4], ["key2", 5]]);
      const buffer = await intMap.toBuffer(map);
      assertEquals(await intMap.fromBuffer(buffer), map);
    });

    it("serializes and deserializes nested maps using blocks", async () => {
      const nestedMap = createMap(intMap);
      const map = new Map([
        ["outer1", new Map([["inner1", 1]])],
        ["outer2", new Map([["inner2", 2], ["inner3", 3]])],
      ]);
      const buffer = await nestedMap.toBuffer(map);
      const result = await nestedMap.fromBuffer(buffer);
      assertEquals(result, map);
    });

    it("round-trips nested maps via toBuffer/fromBuffer", async () => {
      const nestedMap = createMap(intMap);
      const map = new Map([
        ["a", new Map([["x", 10], ["y", 20]])],
        ["b", new Map([["z", 30]])],
      ]);
      const buffer = await nestedMap.toBuffer(map);
      assertEquals(await nestedMap.fromBuffer(buffer), map);
    });

    it("skips encoded map blocks", async () => {
      const map = new Map([["a", 7], ["b", 8]]);
      const buffer = await intMap.toBuffer(map);
      const tap = new Tap(buffer);
      await intMap.skip(tap);
      assertEquals(tap.getPos(), buffer.byteLength);
    });

    it("skips encoded nested map blocks", async () => {
      const nestedMap = createMap(intMap);
      const map = new Map([
        ["outer", new Map([["inner1", 1], ["inner2", 2]])],
      ]);
      const buffer = await nestedMap.toBuffer(map);
      const tap = new Tap(buffer);
      await nestedMap.skip(tap);
      assertEquals(tap.getPos(), buffer.byteLength);
    });

    it("round-trips via sync buffer", () => {
      const map = new Map([["a", 1], ["b", 2]]);
      const buffer = intMap.toSyncBuffer(map);
      assertEquals(intMap.fromSyncBuffer(buffer), map);
    });

    it("reads and writes via sync taps", () => {
      const map = new Map([["a", 1], ["b", 2]]);
      const buffer = new ArrayBuffer(256);
      const writeTap = new SyncWritableTap(buffer);
      intMap.writeSync(writeTap, map);
      const readTap = new SyncReadableTap(buffer);
      assertEquals(intMap.readSync(readTap), map);
      assertEquals(readTap.getPos(), writeTap.getPos());
    });

    it("throws when writeSync receives non-map", () => {
      const buffer = new ArrayBuffer(16);
      const tap = new SyncWritableTap(buffer);
      assertThrows(
        () =>
          intMap.writeSync(tap, "not a map" as unknown as Map<string, number>),
        Error,
        "Invalid value",
      );
    });

    it("throws when writeSync encounters non-string map keys", () => {
      const buffer = new ArrayBuffer(64);
      const tap = new SyncWritableTap(buffer);
      const map = new Map([[1 as unknown as string, 1]]);
      assertThrows(
        () => intMap.writeSync(tap, map),
        Error,
        "Invalid value",
      );
    });

    it("skips encoded map blocks via sync taps", () => {
      const map = new Map([["a", 1]]);
      const buffer = intMap.toSyncBuffer(map);
      const tap = new SyncReadableTap(buffer);
      intMap.skipSync(tap);
      assertEquals(tap.getPos(), buffer.byteLength);
    });

    it("skips size-prefixed map blocks via sync taps", async () => {
      const builderBuffer = new ArrayBuffer(32);
      const builderTap = new Tap(builderBuffer);
      await builderTap.writeString("key");
      await builderTap.writeLong(42n);
      const blockSize = builderTap.getPos();

      const buffer = new ArrayBuffer(64);
      const writeTap = new Tap(buffer);
      await writeTap.writeLong(-1n);
      await writeTap.writeLong(BigInt(blockSize));
      await writeTap.writeString("key");
      await writeTap.writeLong(42n);
      await writeTap.writeLong(0n);

      const encoded = buffer.slice(0, writeTap.getPos());
      const tap = new SyncReadableTap(encoded);
      intMap.skipSync(tap);
      assertEquals(tap.getPos(), encoded.byteLength);
    });

    it("throws when matching via sync taps", () => {
      const map = new Map([["a", 1]]);
      const buffer = intMap.toSyncBuffer(map);
      assertThrows(
        () =>
          intMap.matchSync(
            new SyncReadableTap(buffer),
            new SyncReadableTap(buffer),
          ),
        Error,
        "maps cannot be compared",
      );
    });

    it("handles empty maps via sync buffer", () => {
      const emptyMap = new Map<string, number>();
      const buffer = intMap.toSyncBuffer(emptyMap);
      assertEquals(intMap.fromSyncBuffer(buffer), emptyMap);
    });

    it("throws when toSyncBuffer receives invalid value", () => {
      assertThrows(
        () => intMap.toSyncBuffer("invalid" as unknown as Map<string, number>),
        Error,
        "Invalid value",
      );
    });

    it("serializes with toSyncBuffer when validation is disabled", () => {
      const mapNoValidate = new MapType({
        values: intValues,
        validate: false,
      });
      const map = new Map([["a", 1]]);
      const buffer = mapNoValidate.toSyncBuffer(map);
      assertEquals(mapNoValidate.fromSyncBuffer(buffer), map);
    });

    it("throws when toSyncBuffer map contains non-string key", () => {
      const map = new Map([[1 as unknown as string, 1]]);
      assertThrows(
        () => intMap.toSyncBuffer(map),
        Error,
        "Invalid value",
      );
    });

    it("writes empty map correctly", async () => {
      const buffer = await intMap.toBuffer(new Map());
      const tap = new Tap(buffer);
      assertEquals(await tap.readLong(), 0n);
    });

    it("round-trips empty map via toBuffer/fromBuffer", async () => {
      const map: Map<string, number> = new Map();
      const buffer = await intMap.toBuffer(map);
      assertEquals(await intMap.fromBuffer(buffer), map);
    });

    it("writes maps correctly via write method", async () => {
      const buffer = new ArrayBuffer(50);
      const writeTap = new Tap(buffer);
      const map = new Map([["a", 10], ["b", 20]]);
      await intMap.write(writeTap, map);

      const readTap = new Tap(buffer);
      const result = await intMap.read(readTap);
      assertEquals(result, map);
    });

    it("throws error in write when value is not a map", async () => {
      const buffer = new ArrayBuffer(10);
      const tap = new Tap(buffer);
      await assertRejects(
        async () =>
          await intMap.write(
            tap,
            "not a map" as unknown as Map<string, number>,
          ),
        Error,
        "Invalid value",
      );
    });

    it("throws error in write when key is not a string", async () => {
      const buffer = new ArrayBuffer(10);
      const tap = new Tap(buffer);
      const map = new Map([[1 as unknown as string, 1]]);
      await assertRejects(
        async () => await intMap.write(tap, map),
        Error,
        "Invalid value",
      );
    });

    it("throws error in toBuffer when value is not a map", async () => {
      await assertRejects(
        async () =>
          await intMap.toBuffer("not a map" as unknown as Map<string, number>),
        Error,
        "Invalid value",
      );
    });

    it("throws error in toBuffer when key is not a string", async () => {
      const map = new Map([[1 as unknown as string, 1]]);
      await assertRejects(
        async () => await intMap.toBuffer(map),
        Error,
        "Invalid value",
      );
    });
  });

  describe("cloneFromValue()", () => {
    it("clones maps deeply", () => {
      const original = new Map([["a", 9], ["b", 10]]);
      const cloned = intMap.cloneFromValue(original);
      assertEquals(cloned, original);
      cloned.set("a", 99);
      assertEquals(original.get("a"), 9);
    });

    it("clones nested maps deeply", () => {
      const nestedMap = createMap(intMap);
      const original = new Map([
        ["outer", new Map([["inner", 1]])],
      ]);
      const cloned = nestedMap.cloneFromValue(original);
      assertEquals(cloned, original);
      cloned.get("outer")!.set("inner", 999);
      assertEquals(original.get("outer")!.get("inner"), 1);
    });

    it("clones plain object defaults into a map", () => {
      const values = { a: 1, b: 2 };
      const cloned = intMap.cloneFromValue(values);
      assertEquals(cloned, new Map([["a", 1], ["b", 2]]));
    });

    it("throws error in clone when value is not map", () => {
      assertThrows(
        () =>
          intMap.cloneFromValue("not a map" as unknown as Map<string, number>),
        Error,
        "Cannot clone non-map value.",
      );
    });

    it("throws error in clone when key is not string", () => {
      const map = new Map([[1 as unknown as string, 1]]);
      assertThrows(
        () => intMap.cloneFromValue(map),
        Error,
        "Map keys must be strings to clone.",
      );
    });
  });

  describe("compare()", () => {
    it("throws error when comparing maps", () => {
      const map1 = new Map([["a", 1]]);
      const map2 = new Map([["b", 2]]);
      assertThrows(
        () => intMap.compare(map1, map2),
        Error,
        "maps cannot be compared",
      );
    });

    it("throws error when comparing nested maps", () => {
      const nestedMap = createMap(intMap);
      const map1 = new Map([["outer", new Map([["inner", 1]])]]);
      const map2 = new Map([["outer", new Map([["inner", 2]])]]);
      assertThrows(
        () => nestedMap.compare(map1, map2),
        Error,
        "maps cannot be compared",
      );
    });
  });

  describe("match()", () => {
    it("throws error when matching maps", async () => {
      const buffer1 = await intMap.toBuffer(new Map([["a", 1]]));
      const buffer2 = await intMap.toBuffer(new Map([["b", 2]]));
      await assertRejects(
        async () => await intMap.match(new Tap(buffer1), new Tap(buffer2)),
        Error,
        "maps cannot be compared",
      );
    });
  });

  describe("random()", () => {
    it("generates random maps", () => {
      const randomValue = intMap.random();
      assert(randomValue instanceof Map);
      for (const [key, value] of randomValue) {
        assert(typeof key === "string");
        assert(typeof value === "number");
      }
    });
  });

  describe("getValuesType()", () => {
    it("exposes values type", () => {
      assertEquals(intMap.getValuesType(), intValues);
    });
  });

  describe("toJSON()", () => {
    it("serializes to JSON correctly", () => {
      assertEquals(intMap.toJSON(), {
        type: "map",
        values: intValues.toJSON(),
      });
    });
  });

  describe("union values", () => {
    const unionValues = new UnionType({
      types: [new NullType(), new StringType(), new IntType()],
    });
    const mapWithUnion = createMap(unionValues);

    it("round-trips maps that contain union branches", async () => {
      const value: Map<string, UnionValue> = new Map([
        ["none", null],
        ["asString", { string: "hello" }],
        ["asInt", { int: 7 }],
      ]);

      const buffer = await mapWithUnion.toBuffer(value);
      const result = await mapWithUnion.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it("constructs map with union values from schema", async () => {
      const schema = { type: "map", values: ["null", "string", "int"] };
      const mapType = createType(schema);
      assert(mapType instanceof MapType);
      assert(mapType.getValuesType() instanceof UnionType);

      const value: Map<string, UnionValue> = new Map([
        ["nullable", null],
        ["wrapped", { string: "value" }],
      ]);

      const buffer = await mapType.toBuffer(value);
      const result = await mapType.fromBuffer(buffer);
      assertEquals(result, value);
    });
  });

  describe("integration", () => {
    it("round-trips, validates, and clones nested maps correctly", async () => {
      // Create map of maps of ints
      const intMapOfInts = createMap(intMap);

      // Test data: nested maps
      const nestedData = new Map([
        ["outer1", new Map([["inner1", 1], ["inner2", 2]])],
        ["outer2", new Map([["inner3", 3]])],
      ]);

      // Round-trip via toBuffer/fromBuffer
      const buffer = await intMapOfInts.toBuffer(nestedData);
      const result = await intMapOfInts.fromBuffer(buffer);
      assertEquals(result, nestedData);

      // Test validation
      assert(intMapOfInts.check(nestedData));
      assert(!intMapOfInts.check("not a map"));
      assert(!intMapOfInts.check(new Map([["a", 1]]))); // inner values must be maps

      // Test cloning
      const cloned = intMapOfInts.cloneFromValue(nestedData);
      assertEquals(cloned, nestedData);
      cloned.get("outer1")!.set("inner1", 99);
      assertEquals(nestedData.get("outer1")!.get("inner1"), 1); // deep clone
    });
  });

  describe("createResolver()", () => {
    it("creates resolver for promotable writer maps -> string to bytes", async () => {
      // Writer schema: map of strings
      const stringMap = createMap(new StringType());
      // Reader schema: map of byte arrays (promotable evolution)
      const bytesMap = createMap(new BytesType());
      // Create resolver to adapt from string map to byte array map
      const resolver = bytesMap.createResolver(stringMap);
      // Write a map with binary data as strings
      const map = new Map([["key1", "\x01\x02"], ["key2", "\x03\x04"]]);
      const buffer = await stringMap.toBuffer(map);
      const tap = new Tap(buffer);
      // Read using resolver: strings evolve to Uint8Arrays
      const result = (await resolver.read(tap)) as Map<string, Uint8Array>;
      // Result should be a map with Uint8Arrays
      assertEquals(result.size, 2);
      assertEquals([...result.get("key1")!], [1, 2]);
      assertEquals([...result.get("key2")!], [3, 4]);
    });

    it("creates resolver for promotable writer maps -> int to long", async () => {
      // Writer schema: map of ints
      const writerSchema = createMap(new IntType());

      // Reader schema: map of longs
      const readerSchema = createMap(new LongType());

      // Create resolver
      const resolver = readerSchema.createResolver(writerSchema);

      // Test data: map of ints
      const testData = new Map([["a", 1], ["b", 2]]);

      // Write with writer schema
      const buffer = await writerSchema.toBuffer(testData);
      const tap = new Tap(buffer);

      // Read with resolver (evolves ints to longs)
      const result = (await resolver.read(tap)) as Map<string, bigint>;

      // Expected: map of bigints
      const expected = new Map([["a", 1n], ["b", 2n]]);
      assertEquals(result, expected);
    });

    it("creates resolver for identical writer/reader map types", async () => {
      // Both writer and reader are maps of ints
      const writerMap = createMap(new IntType());
      const readerMap = createMap(new IntType());
      // Create resolver for identical types
      const resolver = readerMap.createResolver(writerMap);
      // Test data
      const map = new Map([["a", 1], ["b", 2]]);
      const buffer = await writerMap.toBuffer(map);
      const tap = new Tap(buffer);
      // Read using resolver
      const result = (await resolver.read(tap)) as Map<string, number>;
      assertEquals(result, map);
    });

    it("reads map resolver synchronously", () => {
      const writerMap = createMap(new IntType());
      const readerMap = createMap(new LongType());
      const resolver = readerMap.createResolver(writerMap);
      const map = new Map([["a", 1]]);
      const buffer = writerMap.toSyncBuffer(map);
      const result = resolver.readSync(new SyncReadableTap(buffer)) as Map<
        string,
        bigint
      >;
      assertEquals(result, new Map([["a", 1n]]));
    });

    it("creates resolver for nested identical map types", async () => {
      // Nested maps of ints
      const writerNested = createMap(createMap(new IntType()));
      const readerNested = createMap(createMap(new IntType()));
      // Create resolver
      const resolver = readerNested.createResolver(writerNested);
      // Test data
      const map = new Map([["outer", new Map([["inner", 42]])]]);
      const buffer = await writerNested.toBuffer(map);
      const tap = new Tap(buffer);
      // Read using resolver
      const result = (await resolver.read(tap)) as Map<
        string,
        Map<string, number>
      >;
      assertEquals(result, map);
    });

    it("creates resolver for nested promotable map types", async () => {
      // Writer: nested map of strings
      const writerNested = createMap(createMap(new StringType()));
      // Reader: nested map of byte arrays
      const readerNested = createMap(createMap(new BytesType()));
      // Create resolver
      const resolver = readerNested.createResolver(writerNested);
      // Test data: nested map with binary strings
      const map = new Map([["outer", new Map([["inner", "\x01\x02"]])]]);
      const buffer = await writerNested.toBuffer(map);
      const tap = new Tap(buffer);
      // Read using resolver: strings evolve to Uint8Arrays
      const result = (await resolver.read(tap)) as Map<
        string,
        Map<string, Uint8Array>
      >;
      assertEquals(result.size, 1);
      const innerMap = result.get("outer")!;
      assertEquals([...innerMap.get("inner")!], [1, 2]);
    });

    it("throws resolver error for incompatible writer maps", () => {
      const stringMap = createMap(new StringType());
      const intMapReader = createMap(new IntType());
      assertThrows(() => intMapReader.createResolver(stringMap));
    });

    it("throws resolver error for nested incompatible writer maps", () => {
      // Writer: nested map of strings
      const writerNested = createMap(createMap(new StringType()));
      // Reader: nested map of ints (incompatible with strings)
      const readerNested = createMap(createMap(new IntType()));
      assertThrows(() => readerNested.createResolver(writerNested));
    });

    it("falls back to super createResolver for non-map writer types", () => {
      const stringType = new StringType();
      assertThrows(
        () => intMap.createResolver(stringType),
        Error,
        `Schema evolution not supported from writer type: string to reader type: 
{
  "type": "map",
  "values": "int"
}
`,
      );
    });
  });
});

describe("readMapInto", () => {
  const intValues = new IntType();
  const intMap = createMap(intValues);

  it("reads positive block count", async () => {
    const buffer = new ArrayBuffer(50);
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(2n); // block count
    await writeTap.writeString("key1");
    await writeTap.writeLong(10n);
    await writeTap.writeString("key2");
    await writeTap.writeLong(20n);
    await writeTap.writeLong(0n); // terminator

    const readTap = new Tap(buffer);
    const results = new Map<string, bigint>();
    await readMapInto(
      readTap,
      async (t) => await t.readLong(),
      (key, value) => results.set(key, value),
    );
    assertEquals(results, new Map([["key1", 10n], ["key2", 20n]]));
  });

  it("reads negative block count (size-prefixed)", async () => {
    const buffer = new ArrayBuffer(50);
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(-2n); // negative block count
    await writeTap.writeLong(100n); // block size, ignored
    await writeTap.writeString("key1");
    await writeTap.writeLong(30n);
    await writeTap.writeString("key2");
    await writeTap.writeLong(40n);
    await writeTap.writeLong(0n);

    const readTap = new Tap(buffer);
    const results = new Map<string, bigint>();
    await readMapInto(
      readTap,
      async (t) => await t.readLong(),
      (key, value) => results.set(key, value),
    );
    assertEquals(results, new Map([["key1", 30n], ["key2", 40n]]));
  });

  it("stops at terminator", async () => {
    const buffer = new ArrayBuffer(50);
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(1n);
    await writeTap.writeString("key1");
    await writeTap.writeLong(50n);
    await writeTap.writeLong(0n); // terminator
    await writeTap.writeLong(1n); // more, but should stop
    await writeTap.writeString("key2");
    await writeTap.writeLong(60n);

    const readTap = new Tap(buffer);
    const results = new Map<string, bigint>();
    await readMapInto(
      readTap,
      async (t) => await t.readLong(),
      (key, value) => results.set(key, value),
    );
    assertEquals(results, new Map([["key1", 50n]]));
  });

  it("throws on bigint outside safe integer range", async () => {
    const buffer = new ArrayBuffer(15);
    const writeTap = new Tap(buffer);
    // Write a large bigint > MAX_SAFE_INTEGER
    const largeBigInt = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    await writeTap.writeLong(largeBigInt);

    const readTap = new Tap(buffer);
    await assertRejects(
      async () => {
        await readMapInto(
          readTap,
          async (t) => await t.readLong(),
          () => {},
        );
      },
      RangeError,
      "Map block length value 9007199254740992 is outside the safe integer range.",
    );
  });

  // This test verifies that map key read failures throw ReadBufferError, as the tap throws on read failures instead of returning undefined.
  it("throws when readString fails for map key", async () => {
    let callCount = 0;
    const mockBuffer = {
      read: (offset: number, size: number) => {
        callCount++;
        if (callCount === 1) {
          // For readLong varint, return some bytes for 1n
          return Promise.resolve(new Uint8Array([2])); // varint for 1
        }
        return Promise.reject(
          new ReadBufferError(
            "Operation exceeds buffer bounds",
            offset,
            size,
            0,
          ),
        );
      },
      // This is unused here.
      canReadMore: (_offset: number) => Promise.resolve(true),
    };
    const tap = new ReadableTap(mockBuffer);
    await assertRejects(
      async () => {
        await intMap.read(tap);
      },
      ReadBufferError,
      "Operation exceeds buffer bounds",
    );
  });

  it("skips size-prefixed map blocks", async () => {
    // Create a buffer with negative count manually
    const buffer = new ArrayBuffer(20);
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(-2n); // negative count
    await writeTap.writeLong(6n); // block size: "a"(2) + int7(1) + "b"(2) + int8(1)
    await writeTap.writeString("a");
    await intValues.write(writeTap, 7);
    await writeTap.writeString("b");
    await intValues.write(writeTap, 8);
    await writeTap.writeLong(0n); // terminator

    const skipTap = new Tap(buffer);
    await intMap.skip(skipTap);
    // After skipping, should be at end
    assertEquals(skipTap.getPos(), writeTap.getPos());
  });

  it("throws on insufficient data for map key", async () => {
    // Create a buffer with count=1 but no string data
    const buffer = new ArrayBuffer(1); // Only space for the count
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(1n); // count = 1

    const readTap = new Tap(buffer);
    await assertRejects(
      async () => {
        await readMapInto(
          readTap,
          async (t) => await t.readLong(),
          () => {},
        );
      },
      ReadBufferError,
      "Operation exceeds buffer bounds",
    );
  });
});

describe("readMapIntoSync", () => {
  it("reads positive block count", async () => {
    const buffer = new ArrayBuffer(64);
    const tap = new Tap(buffer);
    await tap.writeLong(1n);
    await tap.writeString("key");
    await tap.writeLong(42n);
    await tap.writeLong(0n);

    const encoded = buffer.slice(0, tap.getPos());
    const syncTap = new SyncReadableTap(encoded);
    const results: [string, number][] = [];
    readMapIntoSync(
      syncTap,
      (innerTap: SyncReadableTapLike) => Number(innerTap.readLong()),
      (key: string, value: number) => {
        results.push([key, value]);
      },
    );

    assertEquals(results, [["key", 42]]);
  });

  it("reads negative block count (size-prefixed)", async () => {
    const entryBuffer = new ArrayBuffer(64);
    const entryTap = new Tap(entryBuffer);
    await entryTap.writeString("neg");
    await entryTap.writeLong(7n);
    const blockSize = entryTap.getPos();

    const buffer = new ArrayBuffer(128);
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(-1n);
    await writeTap.writeLong(BigInt(blockSize));
    await writeTap.writeString("neg");
    await writeTap.writeLong(7n);
    await writeTap.writeLong(0n);

    const encoded = buffer.slice(0, writeTap.getPos());
    const syncTap = new SyncReadableTap(encoded);
    const results: [string, number][] = [];
    readMapIntoSync(
      syncTap,
      (innerTap: SyncReadableTapLike) => Number(innerTap.readLong()),
      (key: string, value: number) => {
        results.push([key, value]);
      },
    );

    assertEquals(results, [["neg", 7]]);
  });
});

describe("MapType large map writeLong fallback", () => {
  const intValues = new IntType();
  const intMap = new MapType({ values: intValues, validate: false });

  it("uses writeLong for map size > 0x7FFFFFFF in writeUnchecked (async)", async () => {
    // Create a mock Map-like object with a huge size but no actual entries
    const hugeSize = 0x80000000; // 2^31, just over the threshold
    const mockMap = {
      size: hugeSize,
      [Symbol.iterator]: function* () {
        // Yield nothing - we just need to test the writeLong branch
      },
    } as unknown as Map<string, number>;

    // Track which write methods are called
    const calls: Array<{ method: string; value: number | bigint }> = [];
    const mockTap = {
      // deno-lint-ignore require-await
      writeInt: async (value: number) => {
        calls.push({ method: "writeInt", value });
      },
      // deno-lint-ignore require-await
      writeLong: async (value: bigint) => {
        calls.push({ method: "writeLong", value });
      },
      writeString: (_value: string) => {
        // Not called since iterator yields nothing
      },
    };

    await intMap.writeUnchecked(
      mockTap as unknown as Parameters<typeof intMap.writeUnchecked>[0],
      mockMap,
    );

    // Should have called writeLong for the block count (huge size)
    // and writeInt for the terminal 0
    assertEquals(calls.length, 2);
    assertEquals(calls[0].method, "writeLong");
    assertEquals(calls[0].value, BigInt(hugeSize));
    assertEquals(calls[1].method, "writeInt");
    assertEquals(calls[1].value, 0);
  });

  it("uses writeLong for map size > 0x7FFFFFFF in writeSyncUnchecked", () => {
    // Create a mock Map-like object with a huge size but no actual entries
    const hugeSize = 0x80000000; // 2^31, just over the threshold
    const mockMap = {
      size: hugeSize,
      [Symbol.iterator]: function* () {
        // Yield nothing - we just need to test the writeLong branch
      },
    } as unknown as Map<string, number>;

    // Track which write methods are called
    const calls: Array<{ method: string; value: number | bigint }> = [];
    const mockTap = {
      writeInt: (value: number) => {
        calls.push({ method: "writeInt", value });
      },
      writeLong: (value: bigint) => {
        calls.push({ method: "writeLong", value });
      },
      writeString: (_value: string) => {
        // Not called since iterator yields nothing
      },
    };

    intMap.writeSyncUnchecked(
      mockTap as unknown as Parameters<typeof intMap.writeSyncUnchecked>[0],
      mockMap,
    );

    // Should have called writeLong for the block count (huge size)
    // and writeInt for the terminal 0
    assertEquals(calls.length, 2);
    assertEquals(calls[0].method, "writeLong");
    assertEquals(calls[0].value, BigInt(hugeSize));
    assertEquals(calls[1].method, "writeInt");
    assertEquals(calls[1].value, 0);
  });
});

describe("MapType size-prefixed blocks (negative count)", () => {
  describe("readSync non-bulk path with size-prefixed blocks", () => {
    it("handles size-prefixed blocks in readSync without DirectSyncReadableTap", async () => {
      const longMap = createMap(new LongType());
      const _values = new Map([["a", 100n], ["b", 200n]]);

      const tempBuffer = new ArrayBuffer(30);
      const tempTap = new Tap(tempBuffer);
      await tempTap.writeString("a");
      await tempTap.writeLong(100n);
      await tempTap.writeString("b");
      await tempTap.writeLong(200n);
      const blockSize = tempTap.getPos();

      const buffer = new ArrayBuffer(50);
      const writeTap = new Tap(buffer);
      await writeTap.writeLong(-2n);
      await writeTap.writeLong(BigInt(blockSize));
      await writeTap.writeString("a");
      await writeTap.writeLong(100n);
      await writeTap.writeString("b");
      await writeTap.writeLong(200n);
      await writeTap.writeLong(0n);

      const encoded = buffer.slice(0, writeTap.getPos());
      const tap = new SyncReadableTap(encoded);
      const result = longMap.readSync(tap);
      assertEquals(result.get("a"), 100n);
      assertEquals(result.get("b"), 200n);
    });
  });

  describe("#readSyncDirect IntType path with size-prefixed blocks", () => {
    it("handles size-prefixed blocks in IntType map bulk read", async () => {
      const intMap = createMap(new IntType());
      const _values = new Map([["x", 10], ["y", 20]]);

      const tempBuffer = new ArrayBuffer(20);
      const tempTap = new Tap(tempBuffer);
      await tempTap.writeString("x");
      await tempTap.writeLong(10n);
      await tempTap.writeString("y");
      await tempTap.writeLong(20n);
      const blockSize = tempTap.getPos();

      const buffer = new ArrayBuffer(40);
      const writeTap = new Tap(buffer);
      await writeTap.writeLong(-2n);
      await writeTap.writeLong(BigInt(blockSize));
      await writeTap.writeString("x");
      await writeTap.writeLong(10n);
      await writeTap.writeString("y");
      await writeTap.writeLong(20n);
      await writeTap.writeLong(0n);

      const encoded = buffer.slice(0, writeTap.getPos());
      const directTap = new DirectSyncReadableTap(new Uint8Array(encoded));
      const result = intMap.readSync(directTap);
      assertEquals(result.get("x"), 10);
      assertEquals(result.get("y"), 20);
    });
  });

  describe("#readSyncDirect StringType path with size-prefixed blocks", () => {
    it("handles size-prefixed blocks in StringType map bulk read", async () => {
      const stringMap = createMap(new StringType());

      const tempBuffer = new ArrayBuffer(30);
      const tempTap = new Tap(tempBuffer);
      await tempTap.writeString("k1");
      await tempTap.writeString("v1");
      await tempTap.writeString("k2");
      await tempTap.writeString("v2");
      const blockSize = tempTap.getPos();

      const buffer = new ArrayBuffer(50);
      const writeTap = new Tap(buffer);
      await writeTap.writeLong(-2n);
      await writeTap.writeLong(BigInt(blockSize));
      await writeTap.writeString("k1");
      await writeTap.writeString("v1");
      await writeTap.writeString("k2");
      await writeTap.writeString("v2");
      await writeTap.writeLong(0n);

      const encoded = buffer.slice(0, writeTap.getPos());
      const directTap = new DirectSyncReadableTap(new Uint8Array(encoded));
      const result = stringMap.readSync(directTap);
      assertEquals(result.get("k1"), "v1");
      assertEquals(result.get("k2"), "v2");
    });
  });

  describe("#readSyncDirect fallback path (non-IntType/StringType)", () => {
    it("reads maps with non-bulk-optimized value types via DirectSyncReadableTap", () => {
      const floatMap = createMap(new FloatType());
      const values = new Map([["a", 1.5], ["b", 2.5]]);
      const buffer = floatMap.toSyncBuffer(values);

      const directTap = new DirectSyncReadableTap(new Uint8Array(buffer));
      const result = floatMap.readSync(directTap);
      assertEquals(result.size, 2);
      assert(Math.abs(result.get("a")! - 1.5) < 0.001);
      assert(Math.abs(result.get("b")! - 2.5) < 0.001);
    });

    it("handles size-prefixed blocks in fallback path via DirectSyncReadableTap", async () => {
      const floatMap = createMap(new FloatType());

      const tempBuffer = new ArrayBuffer(30);
      const tempTap = new Tap(tempBuffer);
      await tempTap.writeString("p");
      await tempTap.writeFloat(3.14);
      await tempTap.writeString("q");
      await tempTap.writeFloat(2.71);
      const blockSize = tempTap.getPos();

      const buffer = new ArrayBuffer(50);
      const writeTap = new Tap(buffer);
      await writeTap.writeLong(-2n);
      await writeTap.writeLong(BigInt(blockSize));
      await writeTap.writeString("p");
      await writeTap.writeFloat(3.14);
      await writeTap.writeString("q");
      await writeTap.writeFloat(2.71);
      await writeTap.writeLong(0n);

      const encoded = buffer.slice(0, writeTap.getPos());
      const directTap = new DirectSyncReadableTap(new Uint8Array(encoded));
      const result = floatMap.readSync(directTap);
      assertEquals(result.size, 2);
      assert(Math.abs(result.get("p")! - 3.14) < 0.01);
      assert(Math.abs(result.get("q")! - 2.71) < 0.01);
    });
  });

  describe("Resolver readSync with size-prefixed blocks", () => {
    it("handles size-prefixed blocks in resolver readSync", async () => {
      const stringMap = createMap(new StringType());
      const bytesMap = createMap(new BytesType());
      const resolver = bytesMap.createResolver(stringMap);

      const tempBuffer = new ArrayBuffer(30);
      const tempTap = new Tap(tempBuffer);
      await tempTap.writeString("k1");
      await tempTap.writeString("\x01\x02");
      await tempTap.writeString("k2");
      await tempTap.writeString("\x03\x04");
      const blockSize = tempTap.getPos();

      const buffer = new ArrayBuffer(50);
      const writeTap = new Tap(buffer);
      await writeTap.writeLong(-2n);
      await writeTap.writeLong(BigInt(blockSize));
      await writeTap.writeString("k1");
      await writeTap.writeString("\x01\x02");
      await writeTap.writeString("k2");
      await writeTap.writeString("\x03\x04");
      await writeTap.writeLong(0n);

      const encoded = buffer.slice(0, writeTap.getPos());
      const tap = new SyncReadableTap(encoded);
      const result = resolver.readSync(tap) as Map<string, Uint8Array>;
      assertEquals(result.size, 2);
      assertEquals([...result.get("k1")!], [1, 2]);
      assertEquals([...result.get("k2")!], [3, 4]);
    });
  });
});
