import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { NullEncoder } from "../null_encoder.ts";

describe("NullEncoder", () => {
  it("should return the original data without modification", async () => {
    const encoder = new NullEncoder();
    const data = new Uint8Array([10, 20, 30]);
    const result = await encoder.encode(data);

    assertEquals(result, data);
    assertEquals(Array.from(result), [10, 20, 30]);
  });
});
