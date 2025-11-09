import { describe, it } from "@std/testing/bdd";
import { assert, assertEquals } from "@std/assert";
import {
  InMemoryReadableBuffer,
  InMemoryWritableBuffer,
} from "./in_memory_buffer.ts";

describe("InMemoryReadableBuffer", () => {
  describe("constructor", () => {
    it("reports length", async () => {
      const buf = new ArrayBuffer(10);
      const buffer = new InMemoryReadableBuffer(buf);
      assertEquals(await buffer.length(), 10);
    });
  });

  describe("read", () => {
    it("within bounds", async () => {
      const buf = new ArrayBuffer(10);
      const uint8 = new Uint8Array(buf);
      uint8.set([1, 2, 3], 0);
      const buffer = new InMemoryReadableBuffer(buf);
      const result = await buffer.read(0, 3);
      assert(result !== undefined);
      assertEquals(result, new Uint8Array([1, 2, 3]));
    });

    it("out of bounds returns undefined", async () => {
      const buf = new ArrayBuffer(10);
      const buffer = new InMemoryReadableBuffer(buf);
      const result = await buffer.read(8, 5);
      assertEquals(result, undefined);
    });
  });
});

describe("InMemoryWritableBuffer", () => {
  describe("constructor", () => {
    it("reports length", async () => {
      const buf = new ArrayBuffer(10);
      const buffer = new InMemoryWritableBuffer(buf);
      assertEquals(await buffer.length(), 10);
    });

    it("clamps negative offset to 0", async () => {
      const buf = new ArrayBuffer(10);
      const buffer = new InMemoryWritableBuffer(buf, -5);
      // Since offset is clamped to 0, we can append
      const data = new Uint8Array([1, 2]);
      await buffer.appendBytes(data);
      const uint8 = new Uint8Array(buf);
      assertEquals(uint8[0], 1);
      assertEquals(uint8[1], 2);
    });

    it("clamps offset beyond length to length", async () => {
      const buf = new ArrayBuffer(10);
      const buffer = new InMemoryWritableBuffer(buf, 15);
      // Offset clamped to 10, so append should do nothing
      const data = new Uint8Array([1, 2]);
      await buffer.appendBytes(data);
      const uint8 = new Uint8Array(buf);
      assertEquals(uint8[9], 0); // No change
    });

    it("clamps NaN offset to 0", async () => {
      const buf = new ArrayBuffer(10);
      const buffer = new InMemoryWritableBuffer(buf, NaN);
      // Offset clamped to 0
      const data = new Uint8Array([1, 2]);
      await buffer.appendBytes(data);
      const uint8 = new Uint8Array(buf);
      assertEquals(uint8[0], 1);
      assertEquals(uint8[1], 2);
    });

    it("clamps infinite offset to length", async () => {
      const buf = new ArrayBuffer(10);
      const buffer = new InMemoryWritableBuffer(buf, Infinity);
      // Offset clamped to 10, so append should do nothing
      const data = new Uint8Array([1, 2]);
      await buffer.appendBytes(data);
      const uint8 = new Uint8Array(buf);
      assertEquals(uint8[9], 0); // No change
    });
  });

  describe("appendBytes", () => {
    it("within bounds", async () => {
      const buf = new ArrayBuffer(10);
      const buffer = new InMemoryWritableBuffer(buf);
      const data = new Uint8Array([4, 5, 6]);
      await buffer.appendBytes(data);
      const uint8 = new Uint8Array(buf);
      assertEquals(uint8[0], 4);
      assertEquals(uint8[1], 5);
      assertEquals(uint8[2], 6);
      assertEquals(uint8[3], 0);
    });

    it("empty data does nothing", async () => {
      const buf = new ArrayBuffer(10);
      const buffer = new InMemoryWritableBuffer(buf);
      const data = new Uint8Array(0);
      await buffer.appendBytes(data);
      const uint8 = new Uint8Array(buf);
      assertEquals(uint8[0], 0); // No change
    });

    it("beyond capacity is ignored", async () => {
      const buf = new ArrayBuffer(10);
      const buffer = new InMemoryWritableBuffer(buf, 9);
      const data = new Uint8Array([1, 2]);
      await buffer.appendBytes(data); // should not write since only 1 byte remains
      const uint8 = new Uint8Array(buf);
      assertEquals(uint8[8], 0);
      assertEquals(uint8[9], 0);
    });
  });

  describe("canAppendMore", () => {
    it("stays true when writes succeed", async () => {
      const buf = new ArrayBuffer(4);
      const buffer = new InMemoryWritableBuffer(buf);
      await buffer.appendBytes(new Uint8Array([1, 2]));
      assertEquals(await buffer.isValid(), true);
    });

    it("flips to false after overflow attempt", async () => {
      const buf = new ArrayBuffer(4);
      const buffer = new InMemoryWritableBuffer(buf);
      await buffer.appendBytes(new Uint8Array([1, 2, 3, 4, 5]));
      assertEquals(await buffer.isValid(), false);
    });
  });
});
