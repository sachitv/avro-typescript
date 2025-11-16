import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  cloneBytes,
  cloneMetadataMap,
  isIterableMetadata,
  type MetadataInit as _MetadataInit,
  type MetadataMap as _MetadataMap,
  type MetadataValue,
  toMetadataMap,
  toOptionalMetadataMap,
  toRequiredMetadataMap,
} from "./metadata.ts";

describe("metadata", () => {
  describe("isIterableMetadata", () => {
    it("returns false for undefined", () => {
      assertEquals(isIterableMetadata(undefined), false);
    });

    it("returns false for null", () => {
      assertEquals(isIterableMetadata(null), false);
    });

    it("returns true for Map", () => {
      const map = new Map<string, MetadataValue>();
      assertEquals(isIterableMetadata(map), true);
    });

    it("returns true for Iterable", () => {
      const iterable: Iterable<[string, MetadataValue]> = [[
        "key",
        new Uint8Array(1),
      ]];
      assertEquals(isIterableMetadata(iterable), true);
    });

    it("returns false for Record", () => {
      const record: Record<string, MetadataValue> = { key: new Uint8Array(1) };
      assertEquals(isIterableMetadata(record), false);
    });
  });

  describe("cloneBytes", () => {
    it("clones a Uint8Array", () => {
      const original = new Uint8Array([1, 2, 3]);
      const cloned = cloneBytes(original, "test");
      assertEquals(cloned, original);
      assertEquals(cloned !== original, true); // different reference
    });

    it("throws for non-Uint8Array", () => {
      assertThrows(
        () => cloneBytes("not bytes" as unknown as Uint8Array, "test"),
        TypeError,
        "test must be a Uint8Array.",
      );
    });
  });

  describe("toMetadataMap", () => {
    it("converts Map to MetadataMap", () => {
      const map = new Map([["key1", new Uint8Array([1])], [
        "key2",
        new Uint8Array([2]),
      ]]);
      const result = toMetadataMap(map);
      assertEquals(result.get("key1"), new Uint8Array([1]));
      assertEquals(result.get("key2"), new Uint8Array([2]));
    });

    it("converts Iterable to MetadataMap", () => {
      const iterable: Iterable<[string, MetadataValue]> = [[
        "key1",
        new Uint8Array([1]),
      ], ["key2", new Uint8Array([2])]];
      const result = toMetadataMap(iterable);
      assertEquals(result.get("key1"), new Uint8Array([1]));
      assertEquals(result.get("key2"), new Uint8Array([2]));
    });

    it("converts Record to MetadataMap", () => {
      const record: Record<string, MetadataValue> = {
        key1: new Uint8Array([1]),
        key2: new Uint8Array([2]),
      };
      const result = toMetadataMap(record);
      assertEquals(result.get("key1"), new Uint8Array([1]));
      assertEquals(result.get("key2"), new Uint8Array([2]));
    });

    it("throws for non-string key in Iterable", () => {
      const iterable: Iterable<[string | number, MetadataValue]> = [[
        123,
        new Uint8Array([1]),
      ]];
      assertThrows(
        () => toMetadataMap(iterable as unknown as _MetadataInit),
        TypeError,
        "Metadata keys must be strings.",
      );
    });

    it("throws for non-Uint8Array value in Map", () => {
      const map = new Map([["key", "not bytes" as unknown]]);
      assertThrows(
        () => toMetadataMap(map as unknown as _MetadataInit),
        TypeError,
        "Metadata value for 'key' must be a Uint8Array.",
      );
    });

    it("throws for non-Uint8Array value in Record", () => {
      const record = { key: "not bytes" as unknown };
      assertThrows(
        () => toMetadataMap(record as unknown as _MetadataInit),
        TypeError,
        "Metadata value for 'key' must be a Uint8Array.",
      );
    });
  });

  describe("toOptionalMetadataMap", () => {
    it("returns null for undefined", () => {
      assertEquals(toOptionalMetadataMap(undefined), null);
    });

    it("returns null for null", () => {
      assertEquals(toOptionalMetadataMap(null), null);
    });

    it("converts valid init to MetadataMap", () => {
      const record: Record<string, MetadataValue> = {
        key: new Uint8Array([1]),
      };
      const result = toOptionalMetadataMap(record);
      assertEquals(result?.get("key"), new Uint8Array([1]));
    });
  });

  describe("toRequiredMetadataMap", () => {
    it("returns empty Map for undefined", () => {
      const result = toRequiredMetadataMap(undefined);
      assertEquals(result.size, 0);
    });

    it("returns empty Map for null", () => {
      const result = toRequiredMetadataMap(null);
      assertEquals(result.size, 0);
    });

    it("converts valid init to MetadataMap", () => {
      const record: Record<string, MetadataValue> = {
        key: new Uint8Array([1]),
      };
      const result = toRequiredMetadataMap(record);
      assertEquals(result.get("key"), new Uint8Array([1]));
    });
  });

  describe("cloneMetadataMap", () => {
    it("clones empty MetadataMap", () => {
      const original = new Map<string, MetadataValue>();
      const cloned = cloneMetadataMap(original);
      assertEquals(cloned.size, 0);
      assertEquals(cloned !== original, true);
    });

    it("clones MetadataMap with entries", () => {
      const original = new Map([["key1", new Uint8Array([1])], [
        "key2",
        new Uint8Array([2]),
      ]]);
      const cloned = cloneMetadataMap(original);
      assertEquals(cloned.get("key1"), new Uint8Array([1]));
      assertEquals(cloned.get("key2"), new Uint8Array([2]));
      assertEquals(cloned !== original, true);
      assertEquals(cloned.get("key1") !== original.get("key1"), true);
    });
  });
});
