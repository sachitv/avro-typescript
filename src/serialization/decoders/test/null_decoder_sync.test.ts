import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { NullDecoderSync } from "../decoder_null_sync.ts";

describe("NullDecoderSync", () => {
  it("returns the input data unchanged", () => {
    const decoder = new NullDecoderSync();

    const testData = [
      new Uint8Array([]),
      new Uint8Array([1, 2, 3, 4]),
      new Uint8Array([0, 255, 128, 64]),
      new Uint8Array(Array.from({ length: 10 }, (_, i) => i % 256)),
    ];

    for (const data of testData) {
      const result = decoder.decode(data);
      assertEquals(result, data);
      assertEquals(result.length, data.length);
      assertEquals(result.buffer, data.buffer);
    }
  });

  it("does not allocate a new backing buffer", () => {
    const decoder = new NullDecoderSync();
    const data = new Uint8Array([7, 8, 9]);
    const result = decoder.decode(data);

    assertEquals(result.buffer, data.buffer);
  });
});
