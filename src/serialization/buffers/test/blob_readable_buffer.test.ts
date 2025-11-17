import { describe, it } from "@std/testing/bdd";
import { assert, assertEquals } from "@std/assert";
import { BlobReadableBuffer } from "../blob_readable_buffer.ts";

describe("BlobReadableBuffer", () => {
  it("create from Blob", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3, 4])]);
    const buffer = new BlobReadableBuffer(blob);
    assertEquals(await buffer.length(), 4);
  });

  it("read within bounds", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3, 4, 5, 6])]);
    const buffer = new BlobReadableBuffer(blob);
    const result = await buffer.read(1, 3);
    assert(result !== undefined);
    assertEquals(result, new Uint8Array([2, 3, 4]));
  });

  it("read out of bounds", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3])]);
    const buffer = new BlobReadableBuffer(blob);
    const result = await buffer.read(2, 3);
    assertEquals(result, undefined);
  });
});
