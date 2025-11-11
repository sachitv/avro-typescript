import { assertEquals } from "@std/assert";
import { NullDecoder } from "./null_decoder.ts";

Deno.test("NullDecoder", async () => {
  const decoder = new NullDecoder();

  // Test with various input data
  const testData = [
    new Uint8Array([]),
    new Uint8Array([1, 2, 3, 4]),
    new Uint8Array([0, 255, 128, 64]),
    new Uint8Array(Array.from({ length: 1000 }, (_, i) => i % 256)),
  ];

  for (const data of testData) {
    const result = await decoder.decode(data);
    assertEquals(
      result,
      data,
      "NullDecoder should return input data unchanged",
    );
    assertEquals(result.length, data.length, "Length should be preserved");
    assertEquals(result.buffer, data.buffer, "Buffer should be the same");
  }
});

Deno.test("NullDecoder returns Promise", async () => {
  const decoder = new NullDecoder();
  const data = new Uint8Array([1, 2, 3]);

  const result = decoder.decode(data);
  assertEquals(
    result instanceof Promise,
    true,
    "decode should return a Promise",
  );

  const resolved = await result;
  assertEquals(resolved, data);
});
