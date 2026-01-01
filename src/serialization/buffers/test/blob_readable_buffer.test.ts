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

  it("throws ReadBufferError for negative offset", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3])]);
    const buffer = new BlobReadableBuffer(blob);
    await assertRejects(
      () => buffer.read(-1, 1),
      ReadBufferError,
      "Offset and size must be non-negative",
    );
  });

  it("throws ReadBufferError for negative size", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3])]);
    const buffer = new BlobReadableBuffer(blob);
    await assertRejects(
      () => buffer.read(0, -1),
      ReadBufferError,
      "Offset and size must be non-negative",
    );
  });

  it("canReadMore returns true when data is available", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3])]);
    const buffer = new BlobReadableBuffer(blob);

    assertEquals(await buffer.canReadMore(0), true);
    assertEquals(await buffer.canReadMore(2), true);
  });

  it("canReadMore returns false at end of buffer", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3])]);
    const buffer = new BlobReadableBuffer(blob);

    assertEquals(await buffer.canReadMore(3), false);
    assertEquals(await buffer.canReadMore(999), false);
  });

  it("canReadMore rethrows non-ReadBufferError", async () => {
    // Force Blob.prototype.slice to throw so we hit the `throw err` path.
    const originalSlice = Blob.prototype.slice;
    try {
      Blob.prototype.slice = () => {
        throw new Error("slice failed");
      };

      const blob = new Blob([new Uint8Array([1, 2, 3])]);
      const buffer = new BlobReadableBuffer(blob);

      await assertRejects(() => buffer.canReadMore(0), Error, "slice failed");
    } finally {
      Blob.prototype.slice = originalSlice;
    }
  });
});
