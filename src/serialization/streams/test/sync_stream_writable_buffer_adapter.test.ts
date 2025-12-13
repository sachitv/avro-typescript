import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { SyncFixedSizeStreamWriter } from "../sync_fixed_size_stream_writer.ts";
import { SyncStreamWritableBufferAdapter } from "../sync_stream_writable_buffer_adapter.ts";

describe("SyncStreamWritableBufferAdapter", () => {
  describe("appendBytes", () => {
    it("writes to the underlying stream", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(10));
      const adapter = new SyncStreamWritableBufferAdapter(writer);

      adapter.appendBytes(new Uint8Array([1, 2, 3]));
      assertEquals(writer.toUint8Array(), new Uint8Array([1, 2, 3]));
    });

    it("ignores writes after close", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(10));
      const adapter = new SyncStreamWritableBufferAdapter(writer);

      adapter.close();
      adapter.appendBytes(new Uint8Array([4, 5]));
      assertEquals(writer.toUint8Array(), new Uint8Array());
    });
  });

  describe("isValid and canAppendMore", () => {
    it("reflects closed state", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(10));
      const adapter = new SyncStreamWritableBufferAdapter(writer);

      assertEquals(adapter.isValid(), true);
      assertEquals(adapter.canAppendMore(5), true);

      adapter.close();
      assertEquals(adapter.isValid(), false);
      assertEquals(adapter.canAppendMore(1), false);
    });
  });

  describe("close", () => {
    it("delegates to the underlying writer", () => {
      let closeCount = 0;
      const writer = {
        writeBytes: (_data: Uint8Array) => {},
        close: () => {
          closeCount++;
        },
      };
      const adapter = new SyncStreamWritableBufferAdapter(writer);
      adapter.close();
      adapter.close(); // idempotent
      assertEquals(closeCount, 1);
    });

    it("does not prevent writer from signalling overflow", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(2));
      const adapter = new SyncStreamWritableBufferAdapter(writer);

      adapter.appendBytes(new Uint8Array([1, 2]));
      assertThrows(
        () => adapter.appendBytes(new Uint8Array([3])),
        RangeError,
        "Write operation exceeds buffer capacity",
      );
    });
  });
});
