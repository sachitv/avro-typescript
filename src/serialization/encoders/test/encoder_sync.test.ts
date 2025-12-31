import { assertEquals, assertExists } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type { SyncEncoder, SyncEncoderRegistry } from "../encoder_sync.ts";

describe("SyncEncoder interface and types", () => {
  it("should implement the interface", () => {
    const testEncoder: SyncEncoder = {
      encode: (data: Uint8Array): Uint8Array => {
        return data;
      },
    };

    assertExists(testEncoder.encode);
    assertEquals(typeof testEncoder.encode, "function");
  });
});

describe("SyncEncoderRegistry type", () => {
  it("should create a registry with multiple sync encoders", () => {
    const testEncoder: SyncEncoder = {
      encode: (data: Uint8Array): Uint8Array => data,
    };

    const registry: SyncEncoderRegistry = {
      "test": testEncoder,
      "custom": testEncoder,
    };

    assertEquals(registry.test, testEncoder);
    assertEquals(registry.custom, testEncoder);
    assertEquals(Object.keys(registry).length, 2);
  });
});

describe("SyncEncoderRegistry allows custom codecs", () => {
  it("should allow synchronous encoder transformations", () => {
    const mockEncoder: SyncEncoder = {
      encode: (data: Uint8Array): Uint8Array => {
        return new Uint8Array(data.map((b) => b ^ 0xff));
      },
    };

    const registry: SyncEncoderRegistry = {
      "mock": mockEncoder,
      "test": mockEncoder,
    };

    assertEquals(registry.mock, mockEncoder);
    assertEquals(registry.test, mockEncoder);

    const input = new Uint8Array([0xaa, 0x55]);
    const output = registry.mock.encode(input);
    assertEquals(Array.from(output), [0x55, 0xaa]);
  });
});
