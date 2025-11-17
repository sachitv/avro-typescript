import { assert, assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { ForwardOnlyStreamReadableBufferAdapter } from "../forward_only_stream_readable_buffer_adapter.ts";

describe("ForwardOnlyStreamReadableBufferAdapter", () => {
  describe("constructor", () => {
    it("creates from stream buffer", () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);
      assert(adapter instanceof ForwardOnlyStreamReadableBufferAdapter);
    });
  });

  describe("read", () => {
    it("reads sequentially from start", async () => {
      const chunks = [new Uint8Array([1, 2, 3, 4, 5])];
      let chunkIndex = 0;
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => {
          if (chunkIndex < chunks.length) {
            return chunks[chunkIndex++];
          }
          return undefined;
        },
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      const result = await adapter.read(0, 3);
      assert(result !== undefined);
      assertEquals(result, new Uint8Array([1, 2, 3]));
    });

    it("throws error for reading backwards", async () => {
      const chunks = [new Uint8Array([1, 2, 3, 4, 5])];
      let chunkIndex = 0;
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => {
          if (chunkIndex < chunks.length) {
            return chunks[chunkIndex++];
          }
          return undefined;
        },
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      // First read to advance position
      await adapter.read(0, 2);

      // Try to read backwards
      await assertRejects(
        async () => await adapter.read(0, 1),
        Error,
        "Cannot read backwards from current position",
      );
    });

    it("throws error for seeking forward", async () => {
      const chunks = [new Uint8Array([1, 2, 3, 4, 5])];
      let chunkIndex = 0;
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => {
          if (chunkIndex < chunks.length) {
            return chunks[chunkIndex++];
          }
          return undefined;
        },
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      // Try to seek forward
      await assertRejects(
        async () => await adapter.read(2, 1),
        Error,
        "Cannot seek forward; reads must be sequential",
      );
    });

    it("reads sequentially in multiple calls", async () => {
      const chunks = [new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])];
      let chunkIndex = 0;
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => {
          if (chunkIndex < chunks.length) {
            return chunks[chunkIndex++];
          }
          return undefined;
        },
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      const result1 = await adapter.read(0, 3);
      assertEquals(result1, new Uint8Array([1, 2, 3]));

      const result2 = await adapter.read(3, 3);
      assertEquals(result2, new Uint8Array([4, 5, 6]));

      const result3 = await adapter.read(6, 2);
      assertEquals(result3, new Uint8Array([7, 8]));
    });

    it("returns undefined for out of bounds", async () => {
      const chunks = [new Uint8Array([1, 2, 3])];
      let chunkIndex = 0;
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => {
          if (chunkIndex < chunks.length) {
            return chunks[chunkIndex++];
          }
          return undefined;
        },
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      // Read all
      await adapter.read(0, 3);

      // Try to read beyond
      const result = await adapter.read(3, 1);
      assertEquals(result, undefined);
    });

    it("returns undefined for negative offset", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      const result = await adapter.read(-1, 1);
      assertEquals(result, undefined);
    });

    it("returns undefined for negative size", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      const result = await adapter.read(0, -1);
      assertEquals(result, undefined);
    });

    it("handles zero-size reads", async () => {
      const chunks = [new Uint8Array([1, 2, 3])];
      let chunkIndex = 0;
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => {
          if (chunkIndex < chunks.length) {
            return chunks[chunkIndex++];
          }
          return undefined;
        },
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      const result = await adapter.read(0, 0);
      assertEquals(result, new Uint8Array(0));
    });

    it("handles multiple chunks", async () => {
      const chunks = [
        new Uint8Array([1, 2]),
        new Uint8Array([3, 4]),
        new Uint8Array([5, 6]),
      ];
      let chunkIndex = 0;
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => {
          if (chunkIndex < chunks.length) {
            return chunks[chunkIndex++];
          }
          return undefined;
        },
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      const result1 = await adapter.read(0, 3);
      assertEquals(result1, new Uint8Array([1, 2, 3]));

      const result2 = await adapter.read(3, 3);
      assertEquals(result2, new Uint8Array([4, 5, 6]));
    });

    it("handles empty stream", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      const result = await adapter.read(0, 1);
      assertEquals(result, undefined);
    });
  });
});
