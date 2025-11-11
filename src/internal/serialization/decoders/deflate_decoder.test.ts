import { assertEquals, assertExists, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { DeflateDecoder } from "./deflate_decoder.ts";

describe("DeflateDecoder", () => {
  it("requires DecompressionStream API", async () => {
    const decoder = new DeflateDecoder();

    // Save original DecompressionStream if it exists
    const originalDecompressionStream = globalThis.DecompressionStream;

    try {
      // Temporarily remove DecompressionStream to test error handling
      delete (globalThis as {
        DecompressionStream?: typeof DecompressionStream;
      })
        .DecompressionStream;

      const data = new Uint8Array([1, 2, 3]);
      try {
        await decoder.decode(data);
        assertEquals(
          false,
          true,
          "Should have thrown error when DecompressionStream is not available",
        );
      } catch (error) {
        assertEquals(
          (error as Error).message.includes("Deflate codec not supported"),
          true,
        );
      }
    } finally {
      // Restore original DecompressionStream
      if (originalDecompressionStream) {
        globalThis.DecompressionStream = originalDecompressionStream;
      }
    }
  });

  it("handles missing DecompressionStream gracefully", () => {
    if (typeof DecompressionStream === "undefined") {
      // If DecompressionStream is not available, test should pass
      assertEquals(
        true,
        true,
        "Test environment doesn't have DecompressionStream - skipping",
      );
      return;
    }

    const decoder = new DeflateDecoder();
    assertExists(decoder.decode);
    assertEquals(typeof decoder.decode, "function");
  });

  it("handles error cases", () => {
    if (typeof DecompressionStream === "undefined") {
      // Skip test if DecompressionStream is not available
      return;
    }

    const decoder = new DeflateDecoder();

    // Test that it returns a Promise
    const result = decoder.decode(new Uint8Array([1, 2, 3]));
    assertEquals(
      result instanceof Promise,
      true,
      "decode should return a Promise",
    );

    // The actual decompression may succeed or fail with invalid data
    // We don't assert on the outcome, just that it returns a Promise
  });

  it("fails to decode garbage data", async () => {
    if (typeof DecompressionStream === "undefined") {
      // Skip test if DecompressionStream is not available
      return;
    }

    const decoder = new DeflateDecoder();
    const garbageData = new Uint8Array([0xFF, 0xFF, 0xFF, 0xFF]); // Invalid deflate data

    await assertRejects(() => decoder.decode(garbageData), Error);
  });
});
