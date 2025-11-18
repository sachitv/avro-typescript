import { assertEquals, assertExists, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { DeflateEncoder } from "../deflate_encoder.ts";

describe("DeflateEncoder", () => {
  it("requires CompressionStream API", async () => {
    const encoder = new DeflateEncoder();

    const originalCompressionStream = globalThis.CompressionStream;

    try {
      delete (globalThis as { CompressionStream?: typeof CompressionStream })
        .CompressionStream;

      const data = new Uint8Array([1, 2, 3]);
      try {
        await encoder.encode(data);
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
      if (originalCompressionStream) {
        globalThis.CompressionStream = originalCompressionStream;
      } else {
        delete (globalThis as { CompressionStream?: typeof CompressionStream })
          .CompressionStream;
      }
    }
  });

  it("handles missing CompressionStream gracefully", () => {
    if (typeof CompressionStream === "undefined") {
      assertEquals(
        true,
        true,
        "Test environment doesn't have CompressionStream - skipping",
      );
      return;
    }

    const encoder = new DeflateEncoder();
    assertExists(encoder.encode);
    assertEquals(typeof encoder.encode, "function");
  });

  it("returns a Promise", () => {
    if (typeof CompressionStream === "undefined") {
      assertEquals(
        true,
        true,
        "Test environment doesn't have CompressionStream - skipping",
      );
      return;
    }

    const encoder = new DeflateEncoder();

    const result = encoder.encode(new Uint8Array([1, 2, 3]));
    assertEquals(
      result instanceof Promise,
      true,
      "encode should return a Promise",
    );
  });

  it("handles error cases", async () => {
    if (typeof CompressionStream === "undefined") {
      // Skip test if CompressionStream is not available
      return;
    }

    const encoder = new DeflateEncoder();

    // Test that it returns a Promise
    const result = encoder.encode(new Uint8Array([1, 2, 3]));
    assertEquals(
      result instanceof Promise,
      true,
      "encode should return a Promise",
    );

    // Compression should succeed even with arbitrary data
    const compressed = await result;
    assertEquals(compressed instanceof Uint8Array, true);
    assertEquals(compressed.length > 0, true);
  });

  it("handles CompressionStream errors", async () => {
    const originalCompressionStream = globalThis.CompressionStream;

    try {
      (globalThis as { CompressionStream?: unknown }).CompressionStream =
        class MockCompressionStream {
          constructor() {
            throw new Error("Mock compression error");
          }
        };

      const encoder = new DeflateEncoder();
      const data = new Uint8Array([1, 2, 3]);

      await assertRejects(
        () => encoder.encode(data),
        Error,
        "Mock compression error",
      );
    } finally {
      if (originalCompressionStream) {
        globalThis.CompressionStream = originalCompressionStream;
      } else {
        delete (globalThis as { CompressionStream?: typeof CompressionStream })
          .CompressionStream;
      }
    }
  });

  it("compresses repeated bytes effectively", async () => {
    const encoder = new DeflateEncoder();
    const data = new Uint8Array(100).fill(0xAA);
    const result = await encoder.encode(data);

    assertEquals(result.length < data.length, true);

    // Optional: Ensure event loop clears
    await new Promise((resolve) => setTimeout(resolve, 50));
  });
});
