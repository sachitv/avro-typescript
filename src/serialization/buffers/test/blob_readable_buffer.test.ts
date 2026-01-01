import { describe, it } from "@std/testing/bdd";
import { assertEquals, assertRejects } from "@std/assert";
import { BlobReadableBuffer } from "../blob_readable_buffer.ts";
import { ReadBufferError } from "../buffer_error.ts";

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
    assertEquals(result, new Uint8Array([2, 3, 4]));
  });

  it("throws ReadBufferError for out of bounds read", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3])]);
    const buffer = new BlobReadableBuffer(blob);
    await assertRejects(() => buffer.read(2, 3), ReadBufferError);
  });
});
