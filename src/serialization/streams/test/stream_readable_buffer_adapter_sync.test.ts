import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { ReadBufferError } from "../../buffers/buffer_sync.ts";
import { SyncFixedSizeStreamReader } from "../fixed_size_stream_reader_sync.ts";
import { SyncStreamReadableBufferAdapter } from "../stream_readable_buffer_adapter_sync.ts";

describe("SyncStreamReadableBufferAdapter", () => {
  describe("read", () => {
    it("reads buffered data and caches it", () => {
      const reader = new SyncFixedSizeStreamReader(
        new Uint8Array([1, 2, 3, 4, 5]),
        2,
      );
      const adapter = new SyncStreamReadableBufferAdapter(reader);

      const first = adapter.read(0, 3);
      assertEquals(first, new Uint8Array([1, 2, 3]));

      // Cached read should not pull additional data
      const second = adapter.read(2, 2);
      assertEquals(second, new Uint8Array([3, 4]));
    });

    it("supports zero-length reads at EOF", () => {
      const reader = new SyncFixedSizeStreamReader(
        new Uint8Array([1, 2, 3]),
        3,
      );
      const adapter = new SyncStreamReadableBufferAdapter(reader);

      adapter.read(0, 3);
      const result = adapter.read(3, 0);
      assertEquals(result, new Uint8Array());
    });

    it("throws for negative offset or size", () => {
      const reader = new SyncFixedSizeStreamReader(
        new Uint8Array([1, 2, 3]),
        3,
      );
      const adapter = new SyncStreamReadableBufferAdapter(reader);

      assertThrows(
        () => adapter.read(-1, 1),
        ReadBufferError,
        "Offset and size must be non-negative",
      );
      assertThrows(
        () => adapter.read(0, -2),
        ReadBufferError,
        "Offset and size must be non-negative",
      );
    });

    it("throws when requesting beyond buffered data", () => {
      const reader = new SyncFixedSizeStreamReader(
        new Uint8Array([1, 2, 3]),
        3,
      );
      const adapter = new SyncStreamReadableBufferAdapter(reader);

      assertThrows(
        () => adapter.read(0, 5),
        ReadBufferError,
        "Requested range exceeds buffered data",
      );
    });

    it("skips zero-length chunks while buffering", () => {
      let call = 0;
      const reader = {
        readNext: () => {
          call++;
          if (call === 1) {
            return new Uint8Array(0);
          }
          if (call === 2) {
            return new Uint8Array([1, 2]);
          }
          return undefined;
        },
        close: () => {},
      };
      const adapter = new SyncStreamReadableBufferAdapter(reader);
      const result = adapter.read(0, 2);
      assertEquals(result, new Uint8Array([1, 2]));
    });
  });

  describe("canReadMore", () => {
    it("returns true when bytes remain", () => {
      const reader = new SyncFixedSizeStreamReader(
        new Uint8Array([1, 2, 3]),
        2,
      );
      const adapter = new SyncStreamReadableBufferAdapter(reader);
      assert(adapter.canReadMore(0));
      assert(adapter.canReadMore(2));
    });

    it("returns false when offset is at EOF", () => {
      const reader = new SyncFixedSizeStreamReader(
        new Uint8Array([1, 2, 3]),
        3,
      );
      const adapter = new SyncStreamReadableBufferAdapter(reader);
      adapter.read(0, 3);
      assertEquals(adapter.canReadMore(3), false);
    });

    it("rethrows non-ReadBufferError failures", () => {
      const reader = {
        readNext: () => {
          throw new TypeError("boom");
        },
        close: () => {},
      };
      const adapter = new SyncStreamReadableBufferAdapter(reader);
      assertThrows(() => adapter.canReadMore(0), TypeError, "boom");
    });
  });

  describe("close", () => {
    it("delegates to the underlying reader", () => {
      let closed = false;
      const reader = {
        readNext: () => undefined,
        close: () => {
          closed = true;
        },
      };
      const adapter = new SyncStreamReadableBufferAdapter(reader);
      adapter.close();
      assertEquals(closed, true);
    });
  });
});
