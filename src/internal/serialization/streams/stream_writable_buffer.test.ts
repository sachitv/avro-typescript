import { assert, assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { StreamWritableBuffer } from "./stream_writable_buffer.ts";

describe("StreamWritableBuffer", () => {
  describe("constructor", () => {
    it("creates from WritableStream", () => {
      const chunks: Uint8Array[] = [];
      const stream = new WritableStream<Uint8Array>({
        write(chunk) {
          chunks.push(chunk);
        },
      });
      const buffer = new StreamWritableBuffer(stream);
      assert(buffer instanceof StreamWritableBuffer);
    });
  });

  describe("writeBytes", () => {
    it("writes chunks to stream", async () => {
      const chunks: Uint8Array[] = [];
      const stream = new WritableStream<Uint8Array>({
        write(chunk) {
          chunks.push(chunk);
        },
      });
      const buffer = new StreamWritableBuffer(stream);

      await buffer.writeBytes(new Uint8Array([1, 2, 3]));
      await buffer.writeBytes(new Uint8Array([4, 5]));
      await buffer.close();

      assertEquals(chunks.length, 2);
      assertEquals(chunks[0], new Uint8Array([1, 2, 3]));
      assertEquals(chunks[1], new Uint8Array([4, 5]));
    });

    it("handles empty writes", async () => {
      const chunks: Uint8Array[] = [];
      const stream = new WritableStream<Uint8Array>({
        write(chunk) {
          chunks.push(chunk);
        },
      });
      const buffer = new StreamWritableBuffer(stream);

      await buffer.writeBytes(new Uint8Array([]));
      await buffer.close();

      assertEquals(chunks.length, 0);
    });
  });

  describe("close", () => {
    it("closes the stream and releases the writer lock", async () => {
      let closed = false;
      let released = false;
      const stream = new WritableStream<Uint8Array>({
        close() {
          closed = true;
        },
      });

      // Mock the releaseLock method
      const originalGetWriter = stream.getWriter;
      stream.getWriter = () => {
        const writer = originalGetWriter.call(stream);
        const originalReleaseLock = writer.releaseLock;
        writer.releaseLock = () => {
          released = true;
          originalReleaseLock.call(writer);
        };
        return writer;
      };

      const buffer = new StreamWritableBuffer(stream);

      await buffer.close();
      assert(closed);
      assert(released);
    });
  });
});
