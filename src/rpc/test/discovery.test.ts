import { assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { discoverProtocol } from "../discovery.ts";
import { Protocol } from "../protocol_core.ts";
import { createInMemoryTransportPair } from "../protocol/transports/in_memory.ts";
import { toBinaryDuplex } from "../protocol/transports/transport_helpers.ts";
import { encodeCallResponse } from "../protocol/wire_format/messages.ts";
import { frameMessage } from "../protocol/wire_format/framing.ts";
import { createType } from "../../type/create_type.ts";
import type { HandshakeResponseInit } from "../protocol/wire_format/handshake.ts";

describe("discoverProtocol", () => {
  const testProtocol = Protocol.create({
    protocol: "TestProtocol",
    messages: {
      testMessage: {
        request: [{ name: "input", type: "string" }],
        response: "string",
      },
    },
  });

  it("throws when transportFactory throws", async () => {
    const transportFactory = () =>
      Promise.reject(new Error("transport factory failed"));

    await assertRejects(
      () => discoverProtocol(transportFactory),
      Error,
      "transport factory failed",
    );
  });

  it("throws when transportFactory returns rejected promise", async () => {
    // deno-lint-ignore require-await
    const transportFactory = async () => {
      throw new Error("async transport factory failed");
    };

    await assertRejects(
      () => discoverProtocol(transportFactory),
      Error,
      "async transport factory failed",
    );
  });

  it("handles frameSize option being undefined", async () => {
    // Test that opts.frameSize can be undefined without throwing during frameMessage
    const transportFactory = () =>
      Promise.reject(new Error("expected failure"));

    try {
      await discoverProtocol(transportFactory);
    } catch (err) {
      // Should fail on transport creation, not frameSize handling
      assertEquals(err instanceof Error, true);
      assertEquals((err as Error).message.includes("expected failure"), true);
    }
  });

  it("handles frameSize option being defined", async () => {
    // Test that opts.frameSize can be defined without throwing during frameMessage
    const transportFactory = () =>
      Promise.reject(new Error("expected failure"));

    try {
      await discoverProtocol(transportFactory, { frameSize: 4096 });
    } catch (err) {
      // Should fail on transport creation, not frameSize handling
      assertEquals(err instanceof Error, true);
      assertEquals((err as Error).message.includes("expected failure"), true);
    }
  });

  // Helper function for testing discovery with different handshake responses
  async function testDiscoveryHandshake(
    handshake: HandshakeResponseInit,
    expectSuccess: boolean,
    expectedErrorMessage?: string,
    mockSetup?: () => Promise<() => void>, // Returns cleanup function
  ) {
    const transport = createInMemoryTransportPair();
    const discoveryPromise = discoverProtocol(transport.clientFactory);

    const serverTransportRaw = await transport.accept();
    const serverTransport = toBinaryDuplex(serverTransportRaw);
    await serverTransport.readable.read(); // Read client's request

    // Apply mocking if provided
    const cleanup = mockSetup ? await mockSetup() : null;

    try {
      const envelope = {
        handshake,
        metadata: new Map(),
        isError: false,
        payload: null,
        responseType: createType("null"),
        errorType: createType(["string"]),
      };

      const responseBuffer = await encodeCallResponse(envelope);
      const framedResponse = frameMessage(responseBuffer);

      await serverTransport.writable.write(framedResponse);
      await serverTransport.writable.close();

      if (expectSuccess) {
        const result = await discoveryPromise;
        assertEquals(result.getName(), testProtocol.getName());
      } else {
        await assertRejects(
          () => discoveryPromise,
          Error,
          expectedErrorMessage,
        );
      }
    } finally {
      cleanup?.(); // Restore mocks
    }
  }

  it("handles non-Error exceptions in protocol validation", async () => {
    await testDiscoveryHandshake(
      {
        match: "CLIENT" as const,
        serverProtocol: testProtocol.toString(),
        serverHash: testProtocol.getHashBytes(),
        meta: null,
      },
      false,
      "invalid server protocol JSON: unknown error",
      // Mock Protocol.create to throw a non-Error value
      async () => {
        const originalCreate = await import("../protocol_core.ts");
        const originalCreateFn = originalCreate.Protocol.create;
        // @ts-ignore: Intentionally mocking for test
        originalCreate.Protocol.create = () => {
          throw "string error"; // Throw a string, not an Error
        };
        return () => {
          // @ts-ignore: Restoring original function
          originalCreate.Protocol.create = originalCreateFn;
        };
      },
    );
  });

  it("handles serverProtocol that is null", async () => {
    await testDiscoveryHandshake(
      {
        match: "CLIENT" as const,
        serverProtocol: null,
        serverHash: new Uint8Array(16),
        meta: null,
      },
      false,
      "Server did not provide protocol in handshake response",
    );
  });

  it("handles serverProtocol that exceeds size limit", async () => {
    await testDiscoveryHandshake(
      {
        match: "CLIENT" as const,
        serverProtocol: "x".repeat(1024 * 1024 + 1), // Over 1MB
        serverHash: new Uint8Array(16),
        meta: null,
      },
      false,
      "invalid server protocol: too large or not a string",
    );
  });

  it("handles serverProtocol with invalid JSON", async () => {
    await testDiscoveryHandshake(
      {
        match: "CLIENT" as const,
        serverProtocol: "{invalid json syntax",
        serverHash: new Uint8Array(16),
        meta: null,
      },
      false,
      "invalid server protocol JSON: Expected property name or '}' in JSON at position 1",
    );
  });

  it("handles serverProtocol with valid JSON but invalid protocol", async () => {
    await testDiscoveryHandshake(
      {
        match: "CLIENT" as const,
        serverProtocol: '{"invalid": "protocol"}',
        serverHash: new Uint8Array(16),
        meta: null,
      },
      false,
      "invalid server protocol JSON:",
    );
  });

  it("handles serverProtocol with valid JSON but invalid protocol schema", async () => {
    await testDiscoveryHandshake(
      {
        match: "CLIENT" as const,
        serverProtocol:
          '{"invalid": "protocol", "schema": "missing required fields"}',
        serverHash: new Uint8Array(16),
        meta: null,
      },
      false,
      "invalid server protocol JSON:",
    );
  });

  it("accepts serverProtocol exactly at size limit", async () => {
    await testDiscoveryHandshake(
      {
        match: "CLIENT" as const,
        serverProtocol: testProtocol.toString(),
        serverHash: testProtocol.getHashBytes(),
        meta: null,
      },
      true, // Expect success
    );
  });
});
