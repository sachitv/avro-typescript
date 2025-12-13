import { assertEquals, assertExists } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type { SyncDecoder, SyncDecoderRegistry } from "../sync_decoder.ts";

describe("SyncDecoder interface and types", () => {
  it("should implement the interface", () => {
    const testDecoder: SyncDecoder = {
      decode: (data: Uint8Array): Uint8Array => data,
    };

    assertExists(testDecoder.decode);
    assertEquals(typeof testDecoder.decode, "function");
  });
});

describe("SyncDecoderRegistry type", () => {
  it("should create a registry with multiple sync decoders", () => {
    const testDecoder: SyncDecoder = {
      decode: (data: Uint8Array): Uint8Array => data,
    };

    const registry: SyncDecoderRegistry = {
      "test": testDecoder,
      "custom": testDecoder,
    };

    assertEquals(registry.test, testDecoder);
    assertEquals(registry.custom, testDecoder);
    assertEquals(Object.keys(registry).length, 2);
  });
});

describe("SyncDecoderRegistry allows custom codecs", () => {
  it("permits synchronous decoder transformation", () => {
    const mockDecoder: SyncDecoder = {
      decode: (data: Uint8Array): Uint8Array => {
        return new Uint8Array(data.map((b) => b ^ 0xff));
      },
    };

    const registry: SyncDecoderRegistry = {
      "mock": mockDecoder,
      "test": mockDecoder,
    };

    assertEquals(registry.mock, mockDecoder);
    assertEquals(registry.test, mockDecoder);

    const input = new Uint8Array([0xaa, 0x55]);
    const output = registry.mock.decode(input);
    assertEquals(Array.from(output), [0x55, 0xaa]);
  });
});
