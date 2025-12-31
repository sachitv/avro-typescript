import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { NullEncoderSync } from "../encoder_null_sync.ts";

describe("NullEncoderSync", () => {
  it("returns the original Uint8Array without allocation", () => {
    const encoder = new NullEncoderSync();
    const data = new Uint8Array([10, 20, 30]);
    const result = encoder.encode(data);

    assertEquals(result, data);
    assertEquals(Array.from(result), [10, 20, 30]);
  });

  it("keeps the buffer reference intact", () => {
    const encoder = new NullEncoderSync();
    const data = new Uint8Array([1, 2, 3]);
    const result = encoder.encode(data);

    assertEquals(result.buffer, data.buffer);
    assertEquals(result.byteLength, data.byteLength);
  });
});
