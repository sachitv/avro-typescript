import { describe, it } from "@std/testing/bdd";
import { assert, assertEquals, assertRejects } from "@std/assert";
import { BlobBuffer } from "./blob_buffer.ts";

describe("BlobBuffer", () => {
  it("create from Blob", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3, 4])]);
    const buffer = new BlobBuffer(blob);
    assertEquals(await buffer.length(), 4);
  });

  it("read within bounds", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3, 4, 5, 6])]);
    const buffer = new BlobBuffer(blob);
    const result = await buffer.read(1, 3);
    assert(result !== undefined);
    assertEquals(result, new Uint8Array([2, 3, 4]));
  });

  it("read out of bounds", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3])]);
    const buffer = new BlobBuffer(blob);
    const result = await buffer.read(2, 3);
    assertEquals(result, undefined);
  });

  it("write throws error", async () => {
    const blob = new Blob([new Uint8Array([1, 2, 3, 4])]);
    const buffer = new BlobBuffer(blob);
    const data = new Uint8Array([7, 8]);
    await assertRejects(
      async () => {
        await buffer.write(1, data);
      },
      Error,
      "BlobBuffer is read-only",
    );
  });
});
