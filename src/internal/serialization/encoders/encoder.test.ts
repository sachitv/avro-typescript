import { assertEquals, assertExists } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type { Encoder, EncoderRegistry } from "./encoder.ts";

describe("Encoder interface and types", () => {
  it("should create an encoder that implements the interface", () => {
    const testEncoder: Encoder = {
      encode: (data: Uint8Array): Promise<Uint8Array> => {
        return Promise.resolve(data);
      },
    };

    assertExists(testEncoder.encode);
    assertEquals(typeof testEncoder.encode, "function");
  });
});

describe("EncoderRegistry type", () => {
  it("should create a registry with multiple encoders", () => {
    const testEncoder: Encoder = {
      encode: (data: Uint8Array): Promise<Uint8Array> => {
        return Promise.resolve(data);
      },
    };

    const registry: EncoderRegistry = {
      "test": testEncoder,
      "custom": testEncoder,
    };

    assertEquals(registry.test, testEncoder);
    assertEquals(registry.custom, testEncoder);
    assertEquals(Object.keys(registry).length, 2);
  });
});

describe("EncoderRegistry allows custom codecs", () => {
  it("should allow custom encoder transformations", async () => {
    const mockEncoder: Encoder = {
      encode: (data: Uint8Array): Promise<Uint8Array> => {
        // Simple transformation for testing
        return Promise.resolve(new Uint8Array(data.map((b) => b ^ 0xFF))); // Invert bytes
      },
    };

    const registry: EncoderRegistry = {
      "mock": mockEncoder,
      "test": mockEncoder,
    };

    // Test that we can look up encoders
    assertEquals(registry["mock"], mockEncoder);
    assertEquals(registry["test"], mockEncoder);

    // Ensure the transformation works as expected
    const input = new Uint8Array([0xAA, 0x55]);
    const output = await registry.mock.encode(input);
    assertEquals(Array.from(output), [0x55, 0xAA]);
  });
});
