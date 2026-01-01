import { assert, assertEquals, assertRejects } from "@std/assert";
import { ReadBufferError } from "../../buffers/buffer_error.ts";
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
        ReadBufferError,
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
        ReadBufferError,
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

    it("throws ReadBufferError for out of bounds", async () => {
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
      await assertRejects(
        () => adapter.read(3, 1),
        ReadBufferError,
        "Operation exceeds buffer bounds",
      );
    });

    it("throws ReadBufferError for negative offset", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      await assertRejects(
        () => adapter.read(-1, 1),
        ReadBufferError,
        "Offset and size must be non-negative",
      );
    });

    it("throws ReadBufferError for negative size", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      await assertRejects(
        () => adapter.read(0, -1),
        ReadBufferError,
        "Offset and size must be non-negative",
      );
    });

    it("throws ReadBufferError for negative offset with buffered data", async () => {
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

      // Read some data first to populate bufferedData
      await adapter.read(0, 2);

      // Now try negative offset
      await assertRejects(
        () => adapter.read(-1, 1),
        ReadBufferError,
        "Offset and size must be non-negative",
      );
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

    it("throws error when trying to read backwards", async () => {
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

      // Read some data first
      await adapter.read(0, 2);

      // Try to read backwards
      await assertRejects(
        () => adapter.read(0, 1),
        ReadBufferError,
        "Cannot read backwards from current position",
      );
    });

    it("throws error when trying to seek forward", async () => {
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

      // Try to seek forward without reading
      await assertRejects(
        () => adapter.read(5, 1),
        ReadBufferError,
        "Cannot seek forward; reads must be sequential",
      );
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

    it("throws ReadBufferError for empty stream", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      await assertRejects(
        () => adapter.read(0, 1),
        ReadBufferError,
        "Operation exceeds buffer bounds",
      );
    });
  });

  describe("canReadMore", () => {
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

      // Try to check backwards
      await assertRejects(
        async () => await adapter.canReadMore(0),
        ReadBufferError,
        "Cannot read backwards from current position",
      );
    });

    it("throws error for seeking forward", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      // Try to check forward
      await assertRejects(
        async () => await adapter.canReadMore(2),
        ReadBufferError,
        "Cannot seek forward; reads must be sequential",
      );
    });

    it("returns true when data is available at current position", async () => {
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

      let position = 0;
      let readCount = 0;
      while (await adapter.canReadMore(position)) {
        const byte = await adapter.read(position, 1);
        assert(byte !== undefined);
        assertEquals(byte.length, 1);
        position += 1;
        readCount += 1;
      }

      // Should have read 3 bytes
      assertEquals(readCount, 3);

      // After reading all, canReadMore should return false
      assertEquals(await adapter.canReadMore(position), false);
    });

    it("returns false when at EOF", async () => {
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

      const result = await adapter.canReadMore(3);
      assertEquals(result, false);
    });

    it("returns false for empty stream", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new ForwardOnlyStreamReadableBufferAdapter(mockStream);

      const result = await adapter.canReadMore(0);
      assertEquals(result, false);
    });
  });
});
