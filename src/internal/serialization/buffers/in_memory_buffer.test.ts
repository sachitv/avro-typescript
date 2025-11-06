import { describe, it } from "@std/testing/bdd";
import { assert, assertEquals } from "@std/assert";
import { InMemoryBuffer } from "./in_memory_buffer.ts";

describe("InMemoryBuffer", () => {
  it("constructor", async () => {
    const buf = new ArrayBuffer(10);
    const buffer = new InMemoryBuffer(buf);
    assertEquals(await buffer.length(), 10);
  });

  it("read within bounds", async () => {
    const buf = new ArrayBuffer(10);
    const uint8 = new Uint8Array(buf);
    uint8[0] = 1;
    uint8[1] = 2;
    uint8[2] = 3;
    const buffer = new InMemoryBuffer(buf);
    const result = await buffer.read(0, 3);
    assert(result !== undefined);
    assertEquals(result, new Uint8Array([1, 2, 3]));
  });

  it("read out of bounds", async () => {
    const buf = new ArrayBuffer(10);
    const buffer = new InMemoryBuffer(buf);
    const result = await buffer.read(8, 5);
    assertEquals(result, undefined);
  });

  it("write within bounds", async () => {
    const buf = new ArrayBuffer(10);
    const buffer = new InMemoryBuffer(buf);
    const data = new Uint8Array([4, 5, 6]);
    await buffer.write(1, data);
    const uint8 = new Uint8Array(buf);
    assertEquals(uint8[0], 0);
    assertEquals(uint8[1], 4);
    assertEquals(uint8[2], 5);
    assertEquals(uint8[3], 6);
  });

  it("write out of bounds", async () => {
    const buf = new ArrayBuffer(10);
    const buffer = new InMemoryBuffer(buf);
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    await buffer.write(8, data); // should not write since 8+5 > 10
    const uint8 = new Uint8Array(buf);
    // buffer should remain unchanged
    assertEquals(uint8[8], 0);
    assertEquals(uint8[9], 0);
  });

  it("read after write", async () => {
    const buf = new ArrayBuffer(10);
    const buffer = new InMemoryBuffer(buf);
    const data = new Uint8Array([7, 8]);
    await buffer.write(2, data);
    const result = await buffer.read(2, 2);
    assert(result !== undefined);
    assertEquals(result, new Uint8Array([7, 8]));
  });
});
