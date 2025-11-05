import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { UnionType } from "./union_type.ts";
import { StringType } from "./string_type.ts";
import { IntType } from "./int_type.ts";
import { NullType } from "./null_type.ts";
import { RecordType } from "./record_type.ts";
import { ArrayType } from "./array_type.ts";
import { Type } from "./type.ts";
import { Resolver } from "./resolver.ts";
import { Tap } from "../serialization/tap.ts";

describe("UnionType", () => {
  describe("union with null", () => {
    const nullType = new NullType();
    const stringType = new StringType();
    const unionType = new UnionType({ types: [nullType, stringType] });

    it("should handle null values", () => {
      assertEquals(unionType.check(null), true);
      assertEquals(unionType.check({ string: "test" }), true);
      assertEquals(unionType.check({ null: null }), false); // null is not wrapped
      assertEquals(unionType.check({ null: "value" }), false); // {null: value} should be rejected
    });

    it("should serialize and deserialize null", () => {
      const buf = unionType.toBuffer(null);
      const tap = new Tap(buf);
      const result = unionType.read(tap);
      assertEquals(result, null);
    });

    it("clone should handle null", () => {
      const cloned = unionType.clone(null);
      assertEquals(cloned, null);
    });

    it("clone should handle null branch with undefined value", () => {
      const cloned = unionType.clone({ null: undefined });
      assertEquals(cloned, null);
    });

    it("compare should handle null", () => {
      assertEquals(unionType.compare(null, { string: "a" }), -1);
      assertEquals(unionType.compare({ string: "a" }, null), 1);
      assertEquals(unionType.compare(null, null), 0);
    });

    it("random should generate null for null branch", () => {
      const originalRandom = Math.random;
      Math.random = () => 0; // Select first branch, which is null
      try {
        const result = unionType.random();
        assertEquals(result, null);
      } finally {
        Math.random = originalRandom;
      }
    });

    it("write should throw for non-undefined value in null branch", () => {
      const buf = new ArrayBuffer(10);
      const tap = new Tap(buf);
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => unionType.write(tap, { null: "value" } as any),
        Error,
        "Invalid value:",
      );
    });

    it("should create resolver for null branch", () => {
      const nullType = new NullType();
      const resolver = unionType.createResolver(nullType);
      const buf = unionType.toBuffer(null);
      const tap = new Tap(buf);
      const result = resolver.read(tap);
      assertEquals(result, null);
    });

    it("match should handle null in union buffers", () => {
      const buf1 = unionType.toBuffer(null);
      const buf2 = unionType.toBuffer(null);
      const buf3 = unionType.toBuffer({ string: "a" });
      assertEquals(unionType.match(new Tap(buf1), new Tap(buf2)), 0);
      assertEquals(unionType.match(new Tap(buf1), new Tap(buf3)), -1);
      assertEquals(unionType.match(new Tap(buf3), new Tap(buf1)), 1);
    });
  });
  describe("basic union with string and int", () => {
    const stringType = new StringType();
    const intType = new IntType();
    const unionType = new UnionType({ types: [stringType, intType] });

    it("check should validate union values", () => {
      assertEquals(unionType.check({ string: "hello" }), true);
      assertEquals(unionType.check({ int: 42 }), true);
      assertEquals(unionType.check(null), false); // no null branch
      assertEquals(unionType.check("hello"), false); // not wrapped
      assertEquals(unionType.check({}), false); // empty object
      assertEquals(unionType.check({ unknown: "value" }), false); // unknown branch
      assertEquals(unionType.check({ string: 123 }), false); // wrong type in branch
    });

    it("check should call errorHook for invalid values", () => {
      let errorCalled = false;
      const errorHook = () => {
        errorCalled = true;
      };
      assertEquals(unionType.check("invalid", errorHook), false);
      assertEquals(errorCalled, true);
    });

    it("check should call errorHook for null in union without null", () => {
      let errorCalled = false;
      const errorHook = () => {
        errorCalled = true;
      };
      assertEquals(unionType.check(null, errorHook), false);
      assertEquals(errorCalled, true);
    });

    it("check should call errorHook for objects with multiple keys", () => {
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

    it("check should call errorHook for objects with 'null' key", () => {
      let errorCalled = false;
      const errorHook = () => {
        errorCalled = true;
      };
      assertEquals(unionType.check({ null: "value" }, errorHook), false);
      assertEquals(errorCalled, true);
    });

    it("check should call errorHook for unknown branch names", () => {
      let errorCalled = false;
      const errorHook = () => {
        errorCalled = true;
      };
      assertEquals(unionType.check({ unknown: "value" }, errorHook), false);
      assertEquals(errorCalled, true);
    });

    it("check should call errorHook with extended path for invalid branch value", () => {
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

    it("toBuffer and read should serialize and deserialize", () => {
      const testValues = [
        { string: "hello" },
        { int: 42 },
        { string: "" },
        { int: 0 },
      ];

      for (const value of testValues) {
        const buf = unionType.toBuffer(value);
        const tap = new Tap(buf);
        const result = unionType.read(tap);
        assertEquals(result, value);
      }
    });

    it("write and read should work", () => {
      const buf = new ArrayBuffer(100);
      const writeTap = new Tap(buf);
      const value = { string: "test" };
      unionType.write(writeTap, value);
      const readTap = new Tap(buf);
      const result = unionType.read(readTap);
      assertEquals(result, value);
    });

    it("write should throw for null in union without null", () => {
      const buf = new ArrayBuffer(10);
      const tap = new Tap(buf);
      assertThrows(
        () => unionType.write(tap, null),
        Error,
        "Invalid value: 'null' for type:",
      );
    });

    it("write should throw for array value", () => {
      const buf = new ArrayBuffer(10);
      const tap = new Tap(buf);
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => unionType.write(tap, [1, 2, 3] as any),
        Error,
        "Invalid value:",
      );
    });

    it("write should throw for object with multiple keys", () => {
      const buf = new ArrayBuffer(10);
      const tap = new Tap(buf);
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => unionType.write(tap, { string: "test", int: 123 } as any),
        Error,
        "Invalid value:",
      );
    });

    it("write should throw for unknown branch name", () => {
      const buf = new ArrayBuffer(10);
      const tap = new Tap(buf);
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => unionType.write(tap, { unknown: "value" } as any),
        Error,
        "Invalid value:",
      );
    });

    it("write should throw for undefined branch value in non-null branch", () => {
      const buf = new ArrayBuffer(10);
      const tap = new Tap(buf);
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => unionType.write(tap, { string: undefined } as any),
        Error,
        "Invalid value:",
      );
    });

    it("read should throw for invalid union index", () => {
      // Create a buffer with an invalid index (e.g., 999)
      const buf = new ArrayBuffer(10);
      const writeTap = new Tap(buf);
      writeTap.writeLong(999n); // Invalid index
      const readTap = new Tap(buf);
      assertThrows(
        () => unionType.read(readTap),
        Error,
        "Invalid union index: 999",
      );
    });

    it("skip should skip union values", () => {
      const value = { int: 123 };
      const buffer = unionType.toBuffer(value);
      const tap = new Tap(buffer);
      const posBefore = tap._testOnlyPos;
      unionType.skip(tap);
      const posAfter = tap._testOnlyPos;
      assertEquals(posAfter - posBefore, buffer.byteLength);
    });

    it("clone should deep clone union values", () => {
      const original = { string: "test" };
      const cloned = unionType.clone(original);
      assertEquals(cloned, original);
      assertEquals(cloned === original, false); // different objects
    });

    it("clone should throw for null in union without null", () => {
      assertThrows(
        () => unionType.clone(null),
        Error,
        "Cannot clone null for a union without null branch.",
      );
    });

    it("compare should compare union values", () => {
      assertEquals(unionType.compare({ string: "a" }, { string: "b" }), -1);
      assertEquals(unionType.compare({ int: 1 }, { int: 2 }), -1);
      assertEquals(unionType.compare({ string: "a" }, { int: 1 }), -1); // string before int
      assertEquals(unionType.compare({ int: 1 }, { string: "a" }), 1);
      assertEquals(unionType.compare({ string: "a" }, { string: "a" }), 0);
    });

    it("random should generate valid union values", () => {
      for (let i = 0; i < 10; i++) {
        const rand = unionType.random();
        assertEquals(unionType.check(rand), true);
      }
    });

    it("toJSON should return array of type JSONs", () => {
      assertEquals(unionType.toJSON(), ["string", "int"]);
    });

    it("getTypes should return the branch types", () => {
      const types = unionType.getTypes();
      assertEquals(types.length, 2);
      assertEquals(types[0], stringType);
      assertEquals(types[1], intType);
    });

    it("match should compare encoded union buffers", () => {
      const buf1 = unionType.toBuffer({ string: "a" });
      const buf2 = unionType.toBuffer({ string: "a" });
      const buf3 = unionType.toBuffer({ int: 1 });
      assertEquals(unionType.match(new Tap(buf1), new Tap(buf2)), 0);
      assertEquals(unionType.match(new Tap(buf1), new Tap(buf3)), -1);
      assertEquals(unionType.match(new Tap(buf3), new Tap(buf1)), 1);
    });
  });

  describe("union with complex types", () => {
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

    it("should serialize and deserialize complex unions", () => {
      const value = { ComplexRecord: { numbers: [1, 2], text: "hello" } };
      const buf = unionType.toBuffer(value);
      const tap = new Tap(buf);
      const result = unionType.read(tap);
      assertEquals(result, value);
    });
  });

  describe("union with records", () => {
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

    it("should serialize and deserialize record in union", () => {
      const recordValue = { id: 1, name: "test" };
      const value = { TestRecord: recordValue };
      const buf = unionType.toBuffer(value);
      const tap = new Tap(buf);
      const result = unionType.read(tap);
      assertEquals(result, value);
    });
  });

  describe("union with arrays", () => {
    const arrayType = new ArrayType({ items: new IntType() });
    const stringType = new StringType();
    const unionType = new UnionType({ types: [arrayType, stringType] });

    it("should validate array in union", () => {
      assertEquals(unionType.check({ array: [1, 2, 3] }), true);
      assertEquals(unionType.check({ string: "test" }), true);
    });

    it("should serialize and deserialize array in union", () => {
      const value = { array: [1, 2, 3] };
      const buf = unionType.toBuffer(value);
      const tap = new Tap(buf);
      const result = unionType.read(tap);
      assertEquals(result, value);
    });
  });

  describe("error cases", () => {
    const stringType1 = new StringType();
    const stringType2 = new StringType();

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

    it("should throw on nested unions", () => {
      const innerUnion = new UnionType({ types: [new StringType()] });
      assertThrows(
        () => new UnionType({ types: [innerUnion] }),
        Error,
        "Unions cannot be directly nested.",
      );
    });

    it("should throw on duplicate branch names", () => {
      assertThrows(
        () => new UnionType({ types: [stringType1, stringType2] }),
        Error,
        "Duplicate union branch name: string",
      );
    });

    it("should throw on type with invalid toJSON", () => {
      const invalidType = new (class extends Type<string> {
        check() {
          return true;
        }
        write() {}
        read() {
          return "";
        }
        skip() {}
        compare() {
          return 0;
        }
        random() {
          return "";
        }
        toJSON() {
          return { foo: "bar" };
        } // invalid, no "type" field
        toBuffer() {
          return new ArrayBuffer(0);
        }
        fromBuffer() {
          return "";
        }
        isValid() {
          return true;
        }
        clone() {
          return "";
        }
        createResolver() {
          return null as unknown as Resolver<unknown>;
        }
        match() {
          return 0;
        }
      })();
      assertThrows(
        () => new UnionType({ types: [invalidType] }),
        Error,
        "Unable to determine union branch name.",
      );
    });
  });

  describe("createResolver", () => {
    const stringType = new StringType();
    const intType = new IntType();
    const unionType = new UnionType({ types: [stringType, intType] });

    it("should create resolver for same union type", () => {
      const resolver = unionType.createResolver(unionType);
      const value = { string: "test" };
      const buffer = unionType.toBuffer(value);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, value);
    });

    it("should create resolver for union to union", () => {
      const writerUnion = new UnionType({ types: [intType, stringType] }); // different order
      const resolver = unionType.createResolver(writerUnion);
      const writerValue = { int: 42 }; // index 0 in writer
      const buffer = writerUnion.toBuffer(writerValue);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, { int: 42 }); // should map to reader's int branch
    });

    it("should throw for incompatible writer type", () => {
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

    it("should create resolver for compatible branch type", () => {
      const stringType = new StringType();
      const resolver = unionType.createResolver(stringType);
      const buf = stringType.toBuffer("test");
      const tap = new Tap(buf);
      const result = resolver.read(tap);
      assertEquals(result, { string: "test" });
    });

    it("UnionFromUnionResolver should throw for invalid index", () => {
      const resolver = unionType.createResolver(unionType);
      // Create buffer with invalid index 999
      const buf = new ArrayBuffer(10);
      const writeTap = new Tap(buf);
      writeTap.writeLong(999n);
      const readTap = new Tap(buf);
      assertThrows(
        () => resolver.read(readTap),
        Error,
        "Invalid union index: 999",
      );
    });
  });
});
