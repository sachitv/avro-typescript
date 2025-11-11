import { assertEquals, assertExists } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type { Decoder, DecoderRegistry } from "./decoder.ts";

describe("Decoder interface and types", () => {
  it("should create a decoder that implements the interface", () => {
    const testDecoder: Decoder = {
      decode: (data: Uint8Array): Promise<Uint8Array> => {
        return Promise.resolve(data);
      },
    };

    assertExists(testDecoder.decode);
    assertEquals(typeof testDecoder.decode, "function");
  });
});

describe("DecoderRegistry type", () => {
  it("should create a registry with multiple decoders", () => {
    const testDecoder: Decoder = {
      decode: (data: Uint8Array): Promise<Uint8Array> => {
        return Promise.resolve(data);
      },
    };

    const registry: DecoderRegistry = {
      "test": testDecoder,
      "custom": testDecoder,
    };

    assertEquals(registry.test, testDecoder);
    assertEquals(registry.custom, testDecoder);
    assertEquals(Object.keys(registry).length, 2);
  });
});

describe("DecoderRegistry allows custom codecs", () => {
  it("should allow custom decoder transformations", () => {
    const mockDecoder: Decoder = {
      decode: (data: Uint8Array): Promise<Uint8Array> => {
        // Simple transformation for testing
        return Promise.resolve(new Uint8Array(data.map((b) => b ^ 0xFF))); // Invert bytes
      },
    };

    const registry: DecoderRegistry = {
      "mock": mockDecoder,
      "test": mockDecoder,
    };

    // Test that we can look up decoders
    assertEquals(registry["mock"], mockDecoder);
    assertEquals(registry["test"], mockDecoder);
  });
});
