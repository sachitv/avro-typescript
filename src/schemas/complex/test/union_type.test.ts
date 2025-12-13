import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { UnionType } from "../union_type.ts";
import { StringType } from "../../primitive/string_type.ts";
import { IntType } from "../../primitive/int_type.ts";
import { LongType } from "../../primitive/long_type.ts";
import { FloatType } from "../../primitive/float_type.ts";
import { DoubleType } from "../../primitive/double_type.ts";
import { BooleanType } from "../../primitive/boolean_type.ts";
import { NullType } from "../../primitive/null_type.ts";
import { BytesType } from "../../primitive/bytes_type.ts";
import { RecordType } from "../record_type.ts";
import { ArrayType } from "../array_type.ts";
import { EnumType } from "../enum_type.ts";
import { MapType } from "../map_type.ts";
import { FixedType } from "../fixed_type.ts";
import { Type } from "../../type.ts";
import type { Resolver } from "../../resolver.ts";
import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../../serialization/sync_tap.ts";

describe("UnionType", () => {
  describe("null union handling", () => {
    /*
     * Union Structure:
     * Union
     * ├── null
     * └── string
     */
    const nullType = new NullType();
    const stringType = new StringType();
    const unionType = new UnionType({ types: [nullType, stringType] });

    describe("check", () => {
      it("handles null values", () => {
        assertEquals(unionType.check(null), true);
        assertEquals(unionType.check({ string: "test" }), true);
        assertEquals(unionType.check({ null: null }), false); // null is not wrapped
        assertEquals(unionType.check({ null: "value" }), false); // {null: value} should be rejected
      });
    });

    describe("toBuffer", () => {
      it("serializes and deserializes null", async () => {
        const buf = await unionType.toBuffer(null);
        const tap = new Tap(buf);
        const result = await unionType.read(tap);
        assertEquals(result, null);
      });
    });

    describe("clone", () => {
      it("handles null", () => {
        const cloned = unionType.cloneFromValue(null);
        assertEquals(cloned, null);
      });

      it("handles null branch with undefined value", () => {
        const cloned = unionType.cloneFromValue({ null: undefined });
        assertEquals(cloned, null);
      });
    });

    describe("compare", () => {
      it("handles null", () => {
        assertEquals(unionType.compare(null, { string: "a" }), -1);
        assertEquals(unionType.compare({ string: "a" }, null), 1);
        assertEquals(unionType.compare(null, null), 0);
      });
    });

    describe("random", () => {
      it("generates null for null branch", () => {
        const originalRandom = Math.random;
        Math.random = () => 0; // Select first branch, which is null
        try {
          const result = unionType.random();
          assertEquals(result, null);
        } finally {
          Math.random = originalRandom;
        }
      });
    });

    describe("write", () => {
      it("throws for non-undefined value in null branch", async () => {
        const buf = new ArrayBuffer(10);
        const tap = new Tap(buf);
        await assertRejects(
          // deno-lint-ignore no-explicit-any
          () => unionType.write(tap, { null: "value" } as any),
          Error,
          "Invalid value:",
        );
      });
    });

    describe("createResolver", () => {
      it("creates resolver for null branch", async () => {
        const nullType = new NullType();
        const resolver = unionType.createResolver(nullType);
        const buf = await unionType.toBuffer(null);
        const tap = new Tap(buf);
        const result = await resolver.read(tap);
        assertEquals(result, null);
      });
    });

    describe("match", () => {
      it("handles null in union buffers", async () => {
        const buf1 = await unionType.toBuffer(null);
        const buf2 = await unionType.toBuffer(null);
        const buf3 = await unionType.toBuffer({ string: "a" });
        assertEquals(await unionType.match(new Tap(buf1), new Tap(buf2)), 0);
        assertEquals(await unionType.match(new Tap(buf1), new Tap(buf3)), -1);
        assertEquals(await unionType.match(new Tap(buf3), new Tap(buf1)), 1);
      });
    });
  });
  describe("basic union with string and int", () => {
    /*
     * Union Structure:
     * Union
     * ├── string
     * └── int
     */
    const stringType = new StringType();
    const intType = new IntType();
    const unionType = new UnionType({ types: [stringType, intType] });

    describe("check", () => {
      it("validates union values", () => {
        assertEquals(unionType.check({ string: "hello" }), true);
        assertEquals(unionType.check({ int: 42 }), true);
        assertEquals(unionType.check(null), false); // no null branch
        assertEquals(unionType.check("hello"), false); // not wrapped
        assertEquals(unionType.check({}), false); // empty object
        assertEquals(unionType.check({ unknown: "value" }), false); // unknown branch
        assertEquals(unionType.check({ string: 123 }), false); // wrong type in branch
      });

      it("calls errorHook for invalid values", () => {
        let errorCalled = false;
        const errorHook = () => {
          errorCalled = true;
        };
        assertEquals(unionType.check("invalid", errorHook), false);
        assertEquals(errorCalled, true);
      });

      it("calls errorHook for null in union without null", () => {
        let errorCalled = false;
        const errorHook = () => {
          errorCalled = true;
        };
        assertEquals(unionType.check(null, errorHook), false);
        assertEquals(errorCalled, true);
      });

      it("calls errorHook for objects with multiple keys", () => {
        let errorCalled = false;
        const errorHook = () => {
          errorCalled = true;
        };
        assertEquals(
          unionType.check({ string: "test", int: 123 }, errorHook),
          false,
        );
        assertEquals(errorCalled, true);
      });

      it("calls errorHook for objects with 'null' key", () => {
        let errorCalled = false;
        const errorHook = () => {
          errorCalled = true;
        };
        assertEquals(unionType.check({ null: "value" }, errorHook), false);
        assertEquals(errorCalled, true);
      });

      it("calls errorHook for unknown branch names", () => {
        let errorCalled = false;
        const errorHook = () => {
          errorCalled = true;
        };
        assertEquals(unionType.check({ unknown: "value" }, errorHook), false);
        assertEquals(errorCalled, true);
      });

      it("calls errorHook with extended path for invalid branch value", () => {
        let capturedPath: string[] | undefined;
        let capturedValue: unknown;
        const errorHook = (path: string[], value: unknown) => {
          capturedPath = path;
          capturedValue = value;
        };
        assertEquals(unionType.check({ string: 123 }, errorHook), false);
        assertEquals(capturedPath, ["string"]);
        assertEquals(capturedValue, 123);
      });
    });

    describe("toBuffer", () => {
      it("serializes with toBuffer and deserializes with read", async () => {
        const testValues = [
          { string: "hello" },
          { int: 42 },
          { string: "" },
          { int: 0 },
        ];

        for (const value of testValues) {
          const buf = await unionType.toBuffer(value);
          const tap = new Tap(buf);
          const result = await unionType.read(tap);
          assertEquals(result, value);
        }
      });
    });

    describe("write", () => {
      it("round-trips with read", async () => {
        const buf = new ArrayBuffer(100);
        const writeTap = new Tap(buf);
        const value = { string: "test" };
        await unionType.write(writeTap, value);
        const readTap = new Tap(buf);
        const result = await unionType.read(readTap);
        assertEquals(result, value);
      });

      it("throws for null in union without null", async () => {
        const buf = new ArrayBuffer(10);
        const tap = new Tap(buf);
        await assertRejects(
          () => unionType.write(tap, null),
          Error,
          "Invalid value: 'null' for type:",
        );
      });

      it("throws for array value", async () => {
        const buf = new ArrayBuffer(10);
        const tap = new Tap(buf);
        await assertRejects(
          // deno-lint-ignore no-explicit-any
          () => unionType.write(tap, [1, 2, 3] as any),
          Error,
          "Invalid value:",
        );
      });

      it("throws for object with multiple keys", async () => {
        const buf = new ArrayBuffer(10);
        const tap = new Tap(buf);
        await assertRejects(
          // deno-lint-ignore no-explicit-any
          () => unionType.write(tap, { string: "test", int: 123 } as any),
          Error,
          "Invalid value:",
        );
      });

      it("throws for unknown branch name", async () => {
        const buf = new ArrayBuffer(10);
        const tap = new Tap(buf);
        await assertRejects(
          // deno-lint-ignore no-explicit-any
          () => unionType.write(tap, { unknown: "value" } as any),
          Error,
          "Invalid value:",
        );
      });

      it("throws for undefined branch value in non-null branch", async () => {
        const buf = new ArrayBuffer(10);
        const tap = new Tap(buf);
        await assertRejects(
          // deno-lint-ignore no-explicit-any
          () => unionType.write(tap, { string: undefined } as any),
          Error,
          "Invalid value:",
        );
      });
    });

    describe("read", () => {
      it("throws for invalid union index", async () => {
        // Create a buffer with an invalid index (e.g., 999)
        const buf = new ArrayBuffer(10);
        const writeTap = new Tap(buf);
        await writeTap.writeLong(999n); // Invalid index
        const readTap = new Tap(buf);
        await assertRejects(
          async () => await unionType.read(readTap),
          Error,
          "Invalid union index: 999",
        );
      });
    });

    describe("skip", () => {
      it("skips union values", async () => {
        const value = { int: 123 };
        const buffer = await unionType.toBuffer(value);
        const tap = new Tap(buffer);
        const posBefore = tap.getPos();
        await unionType.skip(tap);
        const posAfter = tap.getPos();
        assertEquals(posAfter - posBefore, buffer.byteLength);
      });
    });

    describe("clone", () => {
      it("deep clones union values", () => {
        const original = { string: "test" };
        const cloned = unionType.cloneFromValue(original);
        assertEquals(cloned, original);
        assertEquals(cloned === original, false); // different objects
      });

      it("throws for null in union without null", () => {
        assertThrows(
          () => unionType.cloneFromValue(null),
          Error,
          "Cannot clone null for a union without null branch.",
        );
      });
    });

    describe("compare", () => {
      it("compares union values", () => {
        assertEquals(unionType.compare({ string: "a" }, { string: "b" }), -1);
        assertEquals(unionType.compare({ int: 1 }, { int: 2 }), -1);
        assertEquals(unionType.compare({ string: "a" }, { int: 1 }), -1); // string before int
        assertEquals(unionType.compare({ int: 1 }, { string: "a" }), 1);
        assertEquals(unionType.compare({ string: "a" }, { string: "a" }), 0);
      });
    });

    describe("random", () => {
      it("generates valid union values", () => {
        for (let i = 0; i < 10; i++) {
          const rand = unionType.random();
          assertEquals(unionType.check(rand), true);
        }
      });
    });

    describe("toJSON", () => {
      it("returns array of type JSONs", () => {
        assertEquals(unionType.toJSON(), ["string", "int"]);
      });
    });

    describe("getTypes", () => {
      it("returns the branch types", () => {
        const types = unionType.getTypes();
        assertEquals(types.length, 2);
        assertEquals(types[0], stringType);
        assertEquals(types[1], intType);
      });
    });

    describe("match", () => {
      it("compares encoded union buffers", async () => {
        const buf1 = await unionType.toBuffer({ string: "a" });
        const buf2 = await unionType.toBuffer({ string: "a" });
        const buf3 = await unionType.toBuffer({ int: 1 });
        assertEquals(await unionType.match(new Tap(buf1), new Tap(buf2)), 0);
        assertEquals(await unionType.match(new Tap(buf1), new Tap(buf3)), -1);
        assertEquals(await unionType.match(new Tap(buf3), new Tap(buf1)), 1);
      });
    });

    describe("createResolver", () => {
      it("creates resolver for same union type", async () => {
        const resolver = unionType.createResolver(unionType);
        const value = { string: "test" };
        const buffer = await unionType.toBuffer(value);
        const tap = new Tap(buffer);
        const result = await resolver.read(tap);
        assertEquals(result, value);
      });

      it("creates resolver for union to union", async () => {
        const writerUnion = new UnionType({ types: [intType, stringType] }); // different order
        const resolver = unionType.createResolver(writerUnion);
        const writerValue = { int: 42 }; // index 0 in writer
        const buffer = await writerUnion.toBuffer(writerValue);
        const tap = new Tap(buffer);
        const result = await resolver.read(tap);
        assertEquals(result, { int: 42 }); // should map to reader's int branch
      });

      it("throws for incompatible writer type", () => {
        const doubleType = new (class extends IntType {
          override toJSON() {
            return "double";
          }
        })();
        assertThrows(
          () => unionType.createResolver(doubleType),
          Error,
          "Schema evolution not supported",
        );
      });

      it("creates resolver for compatible branch type", async () => {
        const resolver = unionType.createResolver(stringType);
        const buf = await stringType.toBuffer("test");
        const tap = new Tap(buf);
        const result = await resolver.read(tap);
        assertEquals(result, { string: "test" });
      });

      it("reads branch resolver synchronously", () => {
        const resolver = unionType.createResolver(stringType);
        const buffer = stringType.toSyncBuffer("sync");
        assertEquals(resolver.readSync(new SyncReadableTap(buffer)), {
          string: "sync",
        });
      });

      it("UnionFromUnionResolver throws for invalid index", async () => {
        const resolver = unionType.createResolver(unionType);
        // Create buffer with invalid index 999
        const buf = new ArrayBuffer(10);
        const writeTap = new Tap(buf);
        await writeTap.writeLong(999n);
        const readTap = new Tap(buf);
        await assertRejects(
          async () => await resolver.read(readTap),
          Error,
          "Invalid union index: 999",
        );
      });

      it("reads union-to-union resolver synchronously", () => {
        const resolver = unionType.createResolver(unionType);
        const buffer = unionType.toSyncBuffer({ string: "sync" });
        assertEquals(resolver.readSync(new SyncReadableTap(buffer)), {
          string: "sync",
        });
      });
    });
  });

  describe("union with array and record of `array of ints` and string", () => {
    /*
     * Union Structure:
     * Union
     * ├── array<int>
     * └── Record
     *     ├── numbers: array<int>
     *     └── text: string
     */
    const arrayType = new ArrayType({ items: new IntType() });
    const recordType = new RecordType({
      fullName: "ComplexRecord",
      namespace: "",
      aliases: [],
      fields: [
        { name: "numbers", type: new ArrayType({ items: new IntType() }) },
        { name: "text", type: new StringType() },
      ],
    });
    const unionType = new UnionType({ types: [arrayType, recordType] });

    it("should validate complex union values", () => {
      assertEquals(unionType.check({ array: [1, 2, 3] }), true);
      assertEquals(
        unionType.check({ ComplexRecord: { numbers: [4, 5], text: "test" } }),
        true,
      );
    });

    it("should serialize and deserialize complex unions", async () => {
      const value = { ComplexRecord: { numbers: [1, 2], text: "hello" } };
      const buf = await unionType.toBuffer(value);
      const tap = new Tap(buf);
      const result = await unionType.read(tap);
      assertEquals(result, value);
    });
  });

  describe("union with record and integer", () => {
    /*
     * Union Structure:
     * Union
     * ├── Record
     * │   ├── id: int
     * │   └── name: string
     * └── int
     */
    const recordType = new RecordType({
      fullName: "TestRecord",
      namespace: "",
      aliases: [],
      fields: [
        { name: "id", type: new IntType() },
        { name: "name", type: new StringType() },
      ],
    });
    const intType = new IntType();
    const unionType = new UnionType({ types: [recordType, intType] });

    it("should validate record in union", () => {
      const recordValue = { id: 1, name: "test" };
      assertEquals(unionType.check({ TestRecord: recordValue }), true);
      assertEquals(unionType.check({ int: 42 }), true);
    });

    it("should serialize and deserialize record in union", async () => {
      const recordValue = { id: 1, name: "test" };
      const value = { TestRecord: recordValue };
      const buf = await unionType.toBuffer(value);
      const tap = new Tap(buf);
      const result = await unionType.read(tap);
      assertEquals(result, value);
    });
  });

  describe("union with array of ints and string", () => {
    /*
     * Union Structure:
     * Union
     * ├── array<int>
     * └── string
     */
    const arrayType = new ArrayType({ items: new IntType() });
    const stringType = new StringType();
    const unionType = new UnionType({ types: [arrayType, stringType] });

    it("should validate array in union", () => {
      assertEquals(unionType.check({ array: [1, 2, 3] }), true);
      assertEquals(unionType.check({ string: "test" }), true);
    });

    it("should serialize and deserialize array in union", async () => {
      const value = { array: [1, 2, 3] };
      const buf = await unionType.toBuffer(value);
      const tap = new Tap(buf);
      const result = await unionType.read(tap);
      assertEquals(result, value);
    });
  });

  describe("union of union", () => {
    /*
     * Note: Unions cannot be directly nested in Avro.
     * This test ensures that attempting to create a union containing another union throws an error.
     */
    it("should not be allowed", () => {
      const innerUnion = new UnionType({ types: [new StringType()] });
      assertThrows(
        () => new UnionType({ types: [innerUnion] }),
        Error,
        "Unions cannot be directly nested.",
      );
    });
  });

  describe("union with British and American addresses", () => {
    /*
     * This test block demonstrates a complex union structure for addresses.
     *
     * In the UK, postal codes (pin codes) are alphanumeric strings of fixed length,
     * typically 7 characters (e.g., "SW1A1AA" for London). Here, we model it as a
     * fixed binary type of 7 bytes.
     *
     * In the US, ZIP codes can be either 5 digits (standard) or 9 digits (ZIP+4).
     * We use a nested union to represent this: ShortZip (5 bytes) or LongZip (9 bytes).
     *
     * Union Structure:
     * AddressUnion
     * ├── BritishAddress (Record)
     * │   └── pinCode: Fixed(7)
     * └── AmericanAddress (Record)
     *     └── zip: ZipUnion
     *         ├── ShortZip: Fixed(5)
     *         └── LongZip: Fixed(9)
     */
    const shortZip = new FixedType({
      fullName: "ShortZip",
      namespace: "",
      aliases: [],
      size: 5,
    });
    const longZip = new FixedType({
      fullName: "LongZip",
      namespace: "",
      aliases: [],
      size: 9,
    });
    const zipUnion = new UnionType({ types: [shortZip, longZip] });
    const americanAddress = new RecordType({
      fullName: "AmericanAddress",
      namespace: "",
      aliases: [],
      fields: [
        { name: "zip", type: zipUnion },
      ],
    });
    const britishPin = new FixedType({
      fullName: "BritishPin",
      namespace: "",
      aliases: [],
      size: 7,
    });
    const britishAddress = new RecordType({
      fullName: "BritishAddress",
      namespace: "",
      aliases: [],
      fields: [
        { name: "pinCode", type: britishPin },
      ],
    });
    const addressUnion = new UnionType({
      types: [britishAddress, americanAddress],
    });

    it("should serialize and deserialize British address", async () => {
      const pinBytes = new Uint8Array([83, 87, 49, 65, 49, 65, 65]); // "SW1A1AA"
      const value = { BritishAddress: { pinCode: pinBytes } };
      const buf = await addressUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await addressUnion.read(tap);
      assertEquals(result, value);
    });

    it("should serialize and deserialize American address with short zip", async () => {
      const zipBytes = new Uint8Array([49, 50, 51, 52, 53]); // "12345"
      const value = { AmericanAddress: { zip: { ShortZip: zipBytes } } };
      const buf = await addressUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await addressUnion.read(tap);
      assertEquals(result, value);
    });

    it("should serialize and deserialize American address with long zip", async () => {
      const zipBytes = new Uint8Array([49, 50, 51, 52, 53, 54, 55, 56, 57]); // "123456789"
      const value = { AmericanAddress: { zip: { LongZip: zipBytes } } };
      const buf = await addressUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await addressUnion.read(tap);
      assertEquals(result, value);
    });
  });

  describe("union of all types", () => {
    /*
     * Union Structure:
     * Union
     * ├── int
     * ├── long
     * ├── float
     * ├── double
     * ├── string
     * ├── boolean
     * ├── null
     * ├── bytes
     * ├── Record
     * │   └── value: string
     * ├── array<int>
     * ├── Enum
     * ├── Map<string>
     * └── Fixed(4)
     */
    const intType = new IntType();
    const longType = new LongType();
    const floatType = new FloatType();
    const doubleType = new DoubleType();
    const stringType = new StringType();
    const booleanType = new BooleanType();
    const nullType = new NullType();
    const bytesType = new BytesType();
    const recordType = new RecordType({
      fullName: "TestRecord",
      namespace: "",
      aliases: [],
      fields: [{ name: "value", type: new StringType() }],
    });
    const arrayType = new ArrayType({ items: new IntType() });
    const enumType = new EnumType({
      fullName: "TestEnum",
      namespace: "",
      aliases: [],
      symbols: ["A", "B"],
    });
    const mapType = new MapType({ values: new StringType() });
    const fixedType = new FixedType({
      fullName: "TestFixed",
      namespace: "",
      aliases: [],
      size: 4,
    });
    const allTypesUnion = new UnionType({
      types: [
        intType,
        longType,
        floatType,
        doubleType,
        stringType,
        booleanType,
        nullType,
        bytesType,
        recordType,
        arrayType,
        enumType,
        mapType,
        fixedType,
      ],
    });

    it("round-trips int", async () => {
      const value = { int: 42 };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips long", async () => {
      const value = { long: 42n };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips float", async () => {
      const value = { float: 2.0 };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips double", async () => {
      const value = { double: 3.14 };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips string", async () => {
      const value = { string: "hello" };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips boolean", async () => {
      const value = { boolean: true };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips null", async () => {
      const value = null;
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips bytes", async () => {
      const value = { bytes: new Uint8Array([1, 2, 3]) };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips record", async () => {
      const value = { TestRecord: { value: "test" } };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips array", async () => {
      const value = { array: [1, 2, 3] };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips enum", async () => {
      const value = { TestEnum: "A" };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips map", async () => {
      const value = { map: new Map([["key", "value"]]) };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });

    it("round-trips fixed", async () => {
      const value = { TestFixed: new Uint8Array([1, 2, 3, 4]) };
      const buf = await allTypesUnion.toBuffer(value);
      const tap = new Tap(buf);
      const result = await allTypesUnion.read(tap);
      assertEquals(result, value);
    });
  });

  describe("other error cases", () => {
    it("should throw on empty types array", () => {
      assertThrows(
        () => new UnionType({ types: [] }),
        Error,
        "UnionType requires at least one branch type.",
      );
    });

    it("should throw on non-array types", () => {
      assertThrows(
        () =>
          new UnionType({
            types: "invalid" as unknown as typeof Array.prototype,
          }),
        Error,
        "UnionType requires an array of branch types.",
      );
    });

    it("should throw on non-Type branches", () => {
      assertThrows(
        () => new UnionType({ types: ["string" as unknown as Type] }),
        Error,
        "UnionType branches must be Avro types.",
      );
    });

    it("should throw on duplicate branch names", () => {
      const stringType1 = new StringType();
      const stringType2 = new StringType();
      assertThrows(
        () => new UnionType({ types: [stringType1, stringType2] }),
        Error,
        "Duplicate union branch of type name: string",
      );
    });

    it("should throw on type with invalid toJSON", () => {
      const invalidType = new (class extends Type<string> {
        check() {
          return true;
        }
        async write(): Promise<void> {
          await Promise.resolve();
        }
        async read(): Promise<string> {
          return await Promise.resolve("");
        }
        async skip(): Promise<void> {
          await Promise.resolve();
        }
        compare() {
          return 0;
        }
        random() {
          return "";
        }
        toJSON() {
          return { foo: "bar" };
        } // invalid, no "type" field
        async toBuffer(): Promise<ArrayBuffer> {
          return await Promise.resolve(new ArrayBuffer(0));
        }
        async fromBuffer(): Promise<string> {
          return await Promise.resolve("");
        }
        isValid() {
          return true;
        }
        cloneFromValue() {
          return "";
        }
        createResolver() {
          return null as unknown as Resolver<unknown>;
        }
        async match(): Promise<number> {
          return await Promise.resolve(0);
        }
        toSyncBuffer(): ArrayBuffer {
          return new ArrayBuffer(0);
        }

        fromSyncBuffer(): string {
          return "";
        }

        writeSync(): void {}

        readSync(): string {
          return "";
        }

        skipSync(): void {}

        matchSync(): number {
          return 0;
        }
      })();
      assertThrows(
        () => new UnionType({ types: [invalidType] }),
        Error,
        "Unable to determine union branch name.",
      );
    });

    it("throws when readSync encounters invalid union index", () => {
      const union = new UnionType({ types: [new StringType()] });
      const buffer = new ArrayBuffer(8);
      const tap = new SyncWritableTap(buffer);
      tap.writeLong(123n);
      const readTap = new SyncReadableTap(buffer);
      assertThrows(
        () => union.readSync(readTap),
        Error,
        "Invalid union index: 123",
      );
    });

    it("UnionFromUnionResolver throws for invalid index via sync tap", () => {
      const union = new UnionType({ types: [new StringType()] });
      const resolver = union.createResolver(union);
      const buffer = new ArrayBuffer(8);
      const tap = new SyncWritableTap(buffer);
      tap.writeLong(999n);
      const readTap = new SyncReadableTap(buffer);
      assertThrows(
        () => resolver.readSync(readTap),
        Error,
        "Invalid union index: 999",
      );
    });

    describe("sync helpers", () => {
      const syncUnion = new UnionType({
        types: [new NullType(), new StringType()],
      });

      it("round-trips via sync buffer", () => {
        const stringValue = { string: "sync" };
        const stringBuffer = syncUnion.toSyncBuffer(stringValue);
        assertEquals(syncUnion.fromSyncBuffer(stringBuffer), stringValue);

        const nullBuffer = syncUnion.toSyncBuffer(null);
        assertEquals(syncUnion.fromSyncBuffer(nullBuffer), null);
      });

      it("reads and writes via sync taps", () => {
        const value = { string: "sync" };
        const buffer = new ArrayBuffer(64);
        const writeTap = new SyncWritableTap(buffer);
        syncUnion.writeSync(writeTap, value);
        const readTap = new SyncReadableTap(buffer);
        assertEquals(syncUnion.readSync(readTap), value);
        assertEquals(readTap.getPos(), writeTap.getPos());
      });

      it("skips values via sync taps", () => {
        const buffer = syncUnion.toSyncBuffer(null);
        const tap = new SyncReadableTap(buffer);
        syncUnion.skipSync(tap);
        assertEquals(tap.getPos(), buffer.byteLength);
      });

      it("skips non-null values via sync taps", () => {
        const buffer = syncUnion.toSyncBuffer({ string: "skip" });
        const tap = new SyncReadableTap(buffer);
        syncUnion.skipSync(tap);
        assertEquals(tap.getPos(), buffer.byteLength);
      });

      it("matches encoded buffers via sync taps", () => {
        const bufNull = syncUnion.toSyncBuffer(null);
        const bufString = syncUnion.toSyncBuffer({ string: "a" });
        assertEquals(
          syncUnion.matchSync(
            new SyncReadableTap(bufNull),
            new SyncReadableTap(bufString),
          ),
          -1,
        );
        assertEquals(
          syncUnion.matchSync(
            new SyncReadableTap(bufString),
            new SyncReadableTap(bufString),
          ),
          0,
        );
        assertEquals(
          syncUnion.matchSync(
            new SyncReadableTap(bufNull),
            new SyncReadableTap(bufNull),
          ),
          0,
        );
      });

      it("matches identical non-null branches via sync taps", () => {
        const bufString = syncUnion.toSyncBuffer({ string: "same" });
        assertEquals(
          syncUnion.matchSync(
            new SyncReadableTap(bufString),
            new SyncReadableTap(bufString),
          ),
          0,
        );
      });

      it("compares different non-null branches via sync taps", () => {
        const multiUnion = new UnionType({
          types: [new IntType(), new StringType()],
        });
        const bufInt = multiUnion.toSyncBuffer({ int: 1 });
        const bufString = multiUnion.toSyncBuffer({ string: "a" });
        // int branch (index 0) < string branch (index 1)
        assertEquals(
          multiUnion.matchSync(
            new SyncReadableTap(bufInt),
            new SyncReadableTap(bufString),
          ),
          -1,
        );
        // string branch (index 1) > int branch (index 0)
        assertEquals(
          multiUnion.matchSync(
            new SyncReadableTap(bufString),
            new SyncReadableTap(bufInt),
          ),
          1,
        );
      });

      it("returns null from sync resolver when branch is null", () => {
        const resolver = syncUnion.createResolver(new NullType());
        assertEquals(
          resolver.readSync(new SyncReadableTap(new ArrayBuffer(0))),
          null,
        );
      });
    });
  });
});
