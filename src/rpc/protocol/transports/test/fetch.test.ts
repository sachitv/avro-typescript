import { assert, assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type { FetchTransportOptions } from "../transport_helpers.ts";
import { createFetchTransport } from "../fetch.ts";

describe("createFetchTransport", () => {
  it("creates transport with default options", async () => {
    const transportFactory = createFetchTransport("http://example.com", {
      fetch: (_input, _init) =>
        Promise.resolve({
          body: new ReadableStream<Uint8Array>(),
        } as Response),
    });
    const transport = await transportFactory();
    assert(transport.readable instanceof ReadableStream);
    assert(transport.writable instanceof WritableStream);
  });

  it("applies custom options", async () => {
    let capturedInit: RequestInit | undefined;
    const mockFetch = (_input: RequestInfo | URL, init?: RequestInit) => {
      capturedInit = init;
      return Promise.resolve({
        body: new ReadableStream<Uint8Array>(),
      } as Response);
    };
    const options: FetchTransportOptions = {
      method: "PUT",
      headers: { custom: "header" },
      fetch: mockFetch,
    };
    const transportFactory = createFetchTransport(
      "http://example.com",
      options,
    );
    await transportFactory();
    assertEquals(capturedInit?.method, "PUT");
    const headers = capturedInit?.headers as Headers;
    assertEquals(headers.get("content-type"), "avro/binary");
    assertEquals(headers.get("custom"), "header");
  });

  it("throws when response is missing a body", async () => {
    const transportFactory = createFetchTransport("http://example.com", {
      fetch: () => Promise.resolve({} as Response),
    });
    await assertRejects(
      () => transportFactory(),
      Error,
      "Fetch response has no body.",
    );
  });

  it("uses global fetch when not provided", async () => {
    const originalFetch = globalThis.fetch;
    const mockFetch = (_input: RequestInfo | URL, _init?: RequestInit) => {
      return Promise.resolve({
        body: new ReadableStream<Uint8Array>(),
      } as Response);
    };
    globalThis.fetch = mockFetch as typeof fetch;
    try {
      const transportFactory = createFetchTransport("http://example.com");
      const transport = await transportFactory();
      assert(transport.readable instanceof ReadableStream);
      assert(transport.writable instanceof WritableStream);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it("supports URL endpoints", async () => {
    const transportFactory = createFetchTransport(
      new URL("http://example.com"),
      {
        fetch: (_input, _init) =>
          Promise.resolve({
            body: new ReadableStream<Uint8Array>(),
          } as Response),
      },
    );
    const transport = await transportFactory();
    assert(transport.readable instanceof ReadableStream);
    assert(transport.writable instanceof WritableStream);
  });
});
