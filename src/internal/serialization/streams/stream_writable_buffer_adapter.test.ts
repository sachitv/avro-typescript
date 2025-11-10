import { assert, assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { StreamWritableBufferAdapter } from "./stream_writable_buffer_adapter.ts";

describe("StreamWritableBufferAdapter", () => {
  describe("constructor", () => {
    it("creates from stream buffer", () => {
      const mockStream = {
        writeBytes: async () => {},
        close: async () => {},
      };
      const adapter = new StreamWritableBufferAdapter(mockStream);
      assert(adapter instanceof StreamWritableBufferAdapter);
    });
  });

  describe("appendBytes", () => {
    it("forwards to stream buffer", async () => {
      const written: Uint8Array[] = [];
      const mockStream = {
        // deno-lint-ignore require-await
        writeBytes: async (data: Uint8Array) => {
          written.push(data);
        },
        close: async () => {},
      };
      const adapter = new StreamWritableBufferAdapter(mockStream);

      await adapter.appendBytes(new Uint8Array([1, 2, 3]));
      await adapter.appendBytes(new Uint8Array([4, 5]));

      assertEquals(written.length, 2);
      assertEquals(written[0], new Uint8Array([1, 2, 3]));
      assertEquals(written[1], new Uint8Array([4, 5]));
    });

    it("ignores writes after close", async () => {
      const written: Uint8Array[] = [];
      const mockStream = {
        // deno-lint-ignore require-await
        writeBytes: async (data: Uint8Array) => {
          written.push(data);
        },
        close: async () => {},
      };
      const adapter = new StreamWritableBufferAdapter(mockStream);

      await adapter.close();
      await adapter.appendBytes(new Uint8Array([1, 2, 3]));

      assertEquals(written.length, 0);
    });
  });

  describe("isValid", () => {
    it("returns true when open", async () => {
      const mockStream = {
        writeBytes: async () => {},
        close: async () => {},
      };
      const adapter = new StreamWritableBufferAdapter(mockStream);

      assertEquals(await adapter.isValid(), true);
    });

    it("returns false after close", async () => {
      const mockStream = {
        writeBytes: async () => {},
        close: async () => {},
      };
      const adapter = new StreamWritableBufferAdapter(mockStream);

      await adapter.close();
      assertEquals(await adapter.isValid(), false);
    });
  });

  describe("close", () => {
    it("closes the stream buffer", async () => {
      let closed = false;
      const mockStream = {
        writeBytes: async () => {},
        // deno-lint-ignore require-await
        close: async () => {
          closed = true;
        },
      };
      const adapter = new StreamWritableBufferAdapter(mockStream);

      await adapter.close();
      assert(closed);
    });
  });
});
