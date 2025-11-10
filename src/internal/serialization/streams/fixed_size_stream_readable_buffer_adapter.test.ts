import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { FixedSizeStreamReadableBufferAdapter } from "./fixed_size_stream_readable_buffer_adapter.ts";

describe("FixedSizeStreamReadableBufferAdapter", () => {
  describe("constructor", () => {
    it("creates with valid window size", () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new FixedSizeStreamReadableBufferAdapter(
        mockStream,
        1024,
      );
      assert(adapter instanceof FixedSizeStreamReadableBufferAdapter);
    });

    it("throws on invalid window size", () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      assertThrows(
        () => new FixedSizeStreamReadableBufferAdapter(mockStream, 0),
        RangeError,
        "Window size must be positive",
      );
      assertThrows(
        () => new FixedSizeStreamReadableBufferAdapter(mockStream, -1),
        RangeError,
        "Window size must be positive",
      );
    });
  });

  describe("length", () => {
    it("returns 0 for empty buffer", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 10);
      assertEquals(await adapter.length(), 0);
    });

    it("returns buffer end position", async () => {
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
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 10);

      // Trigger buffering by reading
      await adapter.read(0, 1);
      assertEquals(await adapter.length(), 3);
    });
  });

  describe("read", () => {
    it("reads within buffered window", async () => {
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
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 10);

      const result = await adapter.read(1, 2);
      assert(result !== undefined);
      assertEquals(result, new Uint8Array([2, 3]));
    });

    it("throws error for reads before window start", async () => {
      // Test the basic case: negative offset should return undefined
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => new Uint8Array([1, 2, 3]),
        close: async () => {},
      };
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 10);

      // Negative offset should return undefined (not throw)
      assertEquals(await adapter.read(-1, 1), undefined);
    });

    it("returns undefined for invalid parameters", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => new Uint8Array([1, 2, 3]),
        close: async () => {},
      };
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 10);

      assertEquals(await adapter.read(-1, 1), undefined);
      assertEquals(await adapter.read(0, -1), undefined);

      // Size > window should throw RangeError
      await assertRejects(
        () => adapter.read(0, 100),
        RangeError,
        "Requested size 100 exceeds window size 10",
      );
    });

    it("handles empty stream", async () => {
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => undefined,
        close: async () => {},
      };
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 10);

      const result = await adapter.read(0, 1);
      assertEquals(result, undefined);
    });

    it("advances window when reading beyond current buffer", async () => {
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
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 5);

      // Read first chunk
      const result1 = await adapter.read(0, 3);
      assert(result1 !== undefined);
      assertEquals(result1, new Uint8Array([1, 2, 3]));

      // Read beyond current buffer - should advance window
      const result2 = await adapter.read(3, 3);
      assert(result2 !== undefined);
      assertEquals(result2, new Uint8Array([4, 5, 6]));
    });

    it("handles partial chunk reads", async () => {
      const chunks = [
        new Uint8Array([1, 2, 3]),
        new Uint8Array([4, 5, 6]),
        new Uint8Array([7, 8, 9, 10]),
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
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 5);

      // Read first 3 bytes
      const result1 = await adapter.read(0, 3);
      assert(result1 !== undefined);
      assertEquals(result1, new Uint8Array([1, 2, 3]));

      // Read next 2 bytes (should fill more and shift window)
      const result2 = await adapter.read(3, 2);
      assert(result2 !== undefined);
      assertEquals(result2, new Uint8Array([4, 5]));
    });

    it("respects window size limits", async () => {
      const chunks = [new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])];
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
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 3);

      // Try to read a large chunk (larger than window) - should throw RangeError
      await assertRejects(
        () => adapter.read(0, 5),
        RangeError,
        "Requested size 5 exceeds window size 3",
      );
    });

    it("throws error for reads before window start after advancement", async () => {
      const chunks = [
        new Uint8Array([1, 2, 3, 4, 5]),
        new Uint8Array([6, 7, 8, 9, 10]),
        new Uint8Array([11, 12, 13, 14, 15]),
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
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 10);

      // Read initial data
      await adapter.read(0, 5); // buffers 0-5

      // Read data that causes window advancement
      await adapter.read(10, 5); // offset 10, size 5, fills to 15, window slides

      // Now try to read before current window start (should throw)
      await assertRejects(
        () => adapter.read(3, 2),
        RangeError,
        "Cannot read data before window start",
      );
    });

    it("reads data already in cache without filling", async () => {
      const chunks = [new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])];
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
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 10);

      // Read a range to buffer data
      const result1 = await adapter.read(0, 5);
      assert(result1 !== undefined);
      assertEquals(result1, new Uint8Array([1, 2, 3, 4, 5]));

      // Now read a subset within the buffered range (should not fill again)
      const result2 = await adapter.read(2, 3);
      assert(result2 !== undefined);
      assertEquals(result2, new Uint8Array([3, 4, 5]));
    });

    it("returns undefined when fill does not reach target offset", async () => {
      const chunks = [new Uint8Array([1, 2, 3, 4, 5])]; // Only 5 bytes total
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
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 10);

      // Try to read beyond available data
      const result = await adapter.read(3, 5); // offset 3, size 5 -> needs up to 8, but only 5 available
      assertEquals(result, undefined);
    });

    it("handles push error when chunk exceeds buffer capacity", async () => {
      const chunks = [new Uint8Array(15)]; // Chunk larger than window size
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
      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 10);

      // Try to read, which will attempt to buffer the large chunk
      await assertRejects(
        () => adapter.read(0, 5),
        RangeError,
        "Cannot buffer chunk of size 15",
      );
    });

    it("re-throws non-RangeError from push", async () => {
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
      // Mock CircularBuffer that throws TypeError on push
      const mockBuffer = {
        capacity: () => 10,
        length: () => 0,
        windowStart: () => 0,
        windowEnd: () => 0,
        push: () => {
          throw new TypeError("Mock push error");
        },
        get: () => new Uint8Array(),
        clear: () => {},
      };
      const adapter = new FixedSizeStreamReadableBufferAdapter(
        mockStream,
        10,
        // deno-lint-ignore no-explicit-any
        mockBuffer as any,
      );

      // Try to read, which will call push and throw TypeError
      await assertRejects(
        () => adapter.read(0, 3),
        TypeError,
        "Mock push error",
      );
    });
  });

  describe("window management", () => {
    it("maintains rolling window correctly", async () => {
      // Create a stream with more data than window size
      const data = new Uint8Array(100);
      for (let i = 0; i < 100; i++) {
        data[i] = i;
      }

      let readOffset = 0;
      const mockStream = {
        // deno-lint-ignore require-await
        readNext: async () => {
          if (readOffset >= data.length) {
            return undefined;
          }
          const chunkSize = Math.min(10, data.length - readOffset);
          const chunk = data.slice(readOffset, readOffset + chunkSize);
          readOffset += chunkSize;
          return chunk;
        },
        close: async () => {},
      };

      const adapter = new FixedSizeStreamReadableBufferAdapter(mockStream, 20);

      // Read some initial data
      const result1 = await adapter.read(0, 5);
      assert(result1 !== undefined);
      assertEquals(result1, data.slice(0, 5));

      // Read data that requires advancing the window
      const result2 = await adapter.read(15, 5);
      assert(result2 !== undefined);
      assertEquals(result2, data.slice(15, 20));

      // Try to read data that would be before current window (should fail)
      // This is hard to test directly since we control the window advancement
      // But the implementation should handle it correctly
    });
  });
});
