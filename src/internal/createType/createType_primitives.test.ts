import { assertEquals, assertInstanceOf } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { createType } from "./mod.ts";
import { BooleanType } from "../schemas/boolean_type.ts";
import { BytesType } from "../schemas/bytes_type.ts";
import { DoubleType } from "../schemas/double_type.ts";
import { FloatType } from "../schemas/float_type.ts";
import { IntType } from "../schemas/int_type.ts";
import { LongType } from "../schemas/long_type.ts";
import { NullType } from "../schemas/null_type.ts";
import { StringType } from "../schemas/string_type.ts";

describe("createType", () => {
  it("returns the same Type instance when passed directly", () => {
    const originalType = new StringType();
    const result = createType(originalType);
    assertEquals(result, originalType);
  });

  describe("creates and round trips primitive types by name", () => {
    it("creates boolean type and round-trips values", async () => {
      const type = createType("boolean");
      assertInstanceOf(type, BooleanType);
      const value = true;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates int type and round-trips values", async () => {
      const type = createType("int");
      assertInstanceOf(type, IntType);
      const value = 42;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates long type and round-trips values", async () => {
      const type = createType("long");
      assertInstanceOf(type, LongType);
      const value = 1234567890123456789n;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates float type and round-trips values", async () => {
      const type = createType("float");
      assertInstanceOf(type, FloatType);
      const value = 3.5;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates double type and round-trips values", async () => {
      const type = createType("double");
      assertInstanceOf(type, DoubleType);
      const value = 2.718281828459045;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates string type and round-trips values", async () => {
      const type = createType("string");
      assertInstanceOf(type, StringType);
      const value = "hello world";
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates bytes type and round-trips values", async () => {
      const type = createType("bytes");
      assertInstanceOf(type, BytesType);
      const value = new Uint8Array([1, 2, 3, 4, 5]);
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("creates null type and round-trips values", async () => {
      const type = createType("null");
      assertInstanceOf(type, NullType);
      const value = null;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });
  });

  describe("creates primitive types from object schemas", () => {
    it("creates primitive types from object with type field", async () => {
      // Test that { type: "string" } creates a StringType
      const stringType = createType({ type: "string" });
      assertInstanceOf(stringType, StringType);

      // Test that { type: "int" } creates an IntType
      const intType = createType({ type: "int" });
      assertInstanceOf(intType, IntType);

      // Test that { type: "boolean" } creates a BooleanType
      const boolType = createType({ type: "boolean" });
      assertInstanceOf(boolType, BooleanType);

      // Test round-trip for all of them
      const stringValue = "hello world";
      const stringBuffer = await stringType.toBuffer(stringValue);
      const stringDecoded = await stringType.fromBuffer(stringBuffer);
      assertEquals(stringDecoded, stringValue);

      const intValue = 42;
      const intBuffer = await intType.toBuffer(intValue);
      const intDecoded = await intType.fromBuffer(intBuffer);
      assertEquals(intDecoded, intValue);

      const boolValue = true;
      const boolBuffer = await boolType.toBuffer(boolValue);
      const boolDecoded = await boolType.fromBuffer(boolBuffer);
      assertEquals(boolDecoded, boolValue);
    });
  });
});
