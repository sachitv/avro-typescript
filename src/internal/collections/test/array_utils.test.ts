import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { concatUint8Arrays, toUint8Array } from "../array_utils.ts";

describe("array_utils", () => {
  describe("concatUint8Arrays", () => {
    it("returns empty array for empty input", () => {
      const result = concatUint8Arrays([]);
      assertEquals(result, new Uint8Array(0));
    });

    it("concatenates multiple arrays", () => {
      const parts = [
        new Uint8Array([1, 2]),
        new Uint8Array([3, 4, 5]),
        new Uint8Array([6]),
      ];
      const result = concatUint8Arrays(parts);
      assertEquals(result, new Uint8Array([1, 2, 3, 4, 5, 6]));
    });

    it("handles single array", () => {
      const parts = [new Uint8Array([1, 2, 3])];
      const result = concatUint8Arrays(parts);
      assertEquals(result, new Uint8Array([1, 2, 3]));
    });

    it("handles empty arrays in parts", () => {
      const parts = [
        new Uint8Array([]),
        new Uint8Array([1, 2]),
        new Uint8Array([]),
      ];
      const result = concatUint8Arrays(parts);
      assertEquals(result, new Uint8Array([1, 2]));
    });
  });

  describe("toUint8Array", () => {
    it("returns Uint8Array unchanged", () => {
      const data = new Uint8Array([1, 2, 3]);
      const result = toUint8Array(data);
      assertEquals(result, data);
    });

    it("converts ArrayBuffer to Uint8Array", () => {
      const buffer = new ArrayBuffer(3);
      const view = new Uint8Array(buffer);
      view.set([1, 2, 3]);
      const result = toUint8Array(buffer);
      assertEquals(result, new Uint8Array([1, 2, 3]));
    });

    it("handles empty ArrayBuffer", () => {
      const buffer = new ArrayBuffer(0);
      const result = toUint8Array(buffer);
      assertEquals(result, new Uint8Array(0));
    });
  });
});
