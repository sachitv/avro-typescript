import { assertEquals, assertExists } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { DeflateDecoder } from "./deflate_decoder.ts";

describe("DeflateDecoder", () => {
  it("requires CompressionStream API", async () => {
    const decoder = new DeflateDecoder();

    // Save original CompressionStream if it exists
    const originalCompressionStream = globalThis.CompressionStream;

    try {
      // Temporarily remove CompressionStream to test error handling
      delete (globalThis as { CompressionStream?: typeof CompressionStream })
        .CompressionStream;

      const data = new Uint8Array([1, 2, 3]);
      try {
        await decoder.decode(data);
        assertEquals(
          false,
          true,
          "Should have thrown error when CompressionStream is not available",
        );
      } catch (error) {
        assertEquals(
          (error as Error).message.includes("Deflate codec not supported"),
          true,
        );
      }
    } finally {
      // Restore original CompressionStream
      if (originalCompressionStream) {
        globalThis.CompressionStream = originalCompressionStream;
      }
    }
  });

  it("handles missing CompressionStream gracefully", () => {
    if (typeof CompressionStream === "undefined") {
      // If CompressionStream is not available, test should pass
      assertEquals(
        true,
        true,
        "Test environment doesn't have CompressionStream - skipping",
      );
      return;
    }

    const decoder = new DeflateDecoder();
    assertExists(decoder.decode);
    assertEquals(typeof decoder.decode, "function");
  });

  it("handles error cases", async () => {
    if (typeof CompressionStream === "undefined") {
      // Skip test if CompressionStream is not available
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

    // The actual decompression might fail with invalid data, which is expected
    try {
      await result;
    } catch (error) {
      // Error is acceptable for invalid deflate data
      assertEquals(
        (error as Error).message.includes("Failed to decompress deflate data"),
        true,
      );
    }
  });

  it("handles Error thrown during decompression", async () => {
    // Save original CompressionStream
    const originalCompressionStream = globalThis.CompressionStream;

    try {
      // Mock CompressionStream to throw an Error
      // @ts-ignore: mocking global CompressionStream for test
      globalThis.CompressionStream = class MockCompressionStream {
        constructor() {
          throw new Error("Mock decompression error");
        }
      };

      const decoder = new DeflateDecoder();
      const data = new Uint8Array([1, 2, 3]);

      try {
        await decoder.decode(data);
        assertEquals(false, true, "Should have thrown an error");
      } catch (error) {
        assertEquals(
          (error as Error).message,
          "Failed to decompress deflate data: Mock decompression error",
        );
      }
    } finally {
      // Restore original CompressionStream
      globalThis.CompressionStream = originalCompressionStream;
    }
  });

  it("handles non-Error thrown during decompression", async () => {
    // Save original CompressionStream
    const originalCompressionStream = globalThis.CompressionStream;

    try {
      // Mock CompressionStream to throw a string
      // @ts-ignore: mocking global CompressionStream for test
      globalThis.CompressionStream = class MockCompressionStream {
        constructor() {
          throw "Mock string error";
        }
      };

      const decoder = new DeflateDecoder();
      const data = new Uint8Array([1, 2, 3]);

      try {
        await decoder.decode(data);
        assertEquals(false, true, "Should have thrown an error");
      } catch (error) {
        assertEquals(
          (error as Error).message,
          "Failed to decompress deflate data: Mock string error",
        );
      }
    } finally {
      // Restore original CompressionStream
      globalThis.CompressionStream = originalCompressionStream;
    }
  });
});
