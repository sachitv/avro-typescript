import { assert, assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { ReadBufferError } from "../../buffers/buffer_error.ts";
import { StreamReadableBufferAdapter } from "../stream_readable_buffer_adapter.ts";

describe("StreamReadableBufferAdapter", () => {
  describe("constructor", () => {
    it("creates from stream buffer", () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new StreamReadableBufferAdapter(mockStream);
      assert(adapter instanceof StreamReadableBufferAdapter);
    });
  });

  describe("length", () => {
    it("returns buffered length", async () => {
      const chunks = [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5])];
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      const length1 = await adapter.length();
      assertEquals(length1, 5);

      // Call length again to test early return when already buffered
      const length2 = await adapter.length();
      assertEquals(length2, 5);
    });
  });

  describe("read", () => {
    it("reads from buffered data", async () => {
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      const result = await adapter.read(1, 3);
      assertEquals(result, new Uint8Array([2, 3, 4]));
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      await assertRejects(() => adapter.read(10, 1), ReadBufferError);
    });

    it("throws ReadBufferError for negative offset", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new StreamReadableBufferAdapter(mockStream);

      await assertRejects(() => adapter.read(-1, 1), ReadBufferError);
    });

    it("throws ReadBufferError for negative size", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new StreamReadableBufferAdapter(mockStream);

      await assertRejects(() => adapter.read(0, -1), ReadBufferError);
    });

    it("throws ReadBufferError for negative offset and size", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new StreamReadableBufferAdapter(mockStream);

      await assertRejects(() => adapter.read(-5, -1), ReadBufferError);
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      // Read some data first to populate bufferedData
      await adapter.read(0, 3);

      // Now try negative offset
      await assertRejects(() => adapter.read(-1, 1), ReadBufferError);
    });

    it("throws ReadBufferError when exceeding buffer after fill", async () => {
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      // Try to read beyond what's available
      await assertRejects(() => adapter.read(0, 10), ReadBufferError);
    });

    it("throws ReadBufferError for partial buffer after multiple reads", async () => {
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      // First read to buffer some data
      await adapter.read(0, 2);

      // Try to read beyond what's buffered (3 total bytes, already buffered)
      await assertRejects(() => adapter.read(0, 10), ReadBufferError);
    });

    it("uses cached data when already buffered", async () => {
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      // First read to buffer the data
      const result1 = await adapter.read(0, 3);
      assertEquals(result1, new Uint8Array([1, 2, 3]));

      // Second read should use cached data (early return path)
      const result2 = await adapter.read(1, 3);
      assertEquals(result2, new Uint8Array([2, 3, 4]));
    });

    it("handles partial reads from cached data", async () => {
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      // First read to buffer all data
      await adapter.read(0, 8);

      // Subsequent reads should use cached data
      const result1 = await adapter.read(2, 3);
      assertEquals(result1, new Uint8Array([3, 4, 5]));

      const result2 = await adapter.read(5, 2);
      assertEquals(result2, new Uint8Array([6, 7]));
    });

    it("handles empty stream", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new StreamReadableBufferAdapter(mockStream);

      const length = await adapter.length();
      assertEquals(length, 0);

      await assertRejects(() => adapter.read(0, 1), ReadBufferError);
    });

    it("handles multiple chunks for large reads", async () => {
      const chunks = [
        new Uint8Array([1, 2, 3]),
        new Uint8Array([4, 5, 6]),
        new Uint8Array([7, 8, 9]),
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      const result = await adapter.read(1, 7);
      assertEquals(result, new Uint8Array([2, 3, 4, 5, 6, 7, 8]));
    });

    it("handles read at EOF boundary", async () => {
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      // Read exactly to EOF
      const result1 = await adapter.read(0, 3);
      assertEquals(result1, new Uint8Array([1, 2, 3]));

      // Read beyond EOF should throw for random-access reads
      await assertRejects(() => adapter.read(3, 1), ReadBufferError);
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      const result = await adapter.read(1, 0);
      assertEquals(result, new Uint8Array(0));
    });
  });

  describe("canReadMore", () => {
    it("returns true for valid offset within buffered data", async () => {
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      assertEquals(await adapter.canReadMore(0), true);
      assertEquals(await adapter.canReadMore(2), true);
      assertEquals(await adapter.canReadMore(4), true);
    });

    it("returns false for offset at EOF", async () => {
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      assertEquals(await adapter.canReadMore(3), false);
    });

    it("returns false for offset beyond EOF", async () => {
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
      const adapter = new StreamReadableBufferAdapter(mockStream);

      assertEquals(await adapter.canReadMore(5), false);
    });

    it("returns false for negative offset", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new StreamReadableBufferAdapter(mockStream);

      assertEquals(await adapter.canReadMore(-1), false);
    });

    it("rethrows non-ReadBufferError", async () => {
      const mockStream = {
        readNext: () => {
          throw new Error("readNext failed");
        },
        close: async () => {},
      };
      const adapter = new StreamReadableBufferAdapter(mockStream);

      await assertRejects(
        () => adapter.canReadMore(0),
        Error,
        "readNext failed",
      );
    });

    it("handles empty stream", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new StreamReadableBufferAdapter(mockStream);

      assertEquals(await adapter.canReadMore(0), false);
    });
  });
});
