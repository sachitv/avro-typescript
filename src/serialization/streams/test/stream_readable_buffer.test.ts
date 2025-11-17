import { assert, assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { StreamReadableBuffer } from "../stream_readable_buffer.ts";

describe("StreamReadableBuffer", () => {
  describe("constructor", () => {
    it("creates from ReadableStream", () => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3]));
          controller.close();
        },
      });
      const buffer = new StreamReadableBuffer(stream);
      assert(buffer instanceof StreamReadableBuffer);
    });
  });

  describe("readNext", () => {
    it("reads chunks sequentially", async () => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3]));
          controller.enqueue(new Uint8Array([4, 5]));
          controller.close();
        },
      });
      const buffer = new StreamReadableBuffer(stream);

      const chunk1 = await buffer.readNext();
      assert(chunk1 !== undefined);
      assertEquals(chunk1, new Uint8Array([1, 2, 3]));

      const chunk2 = await buffer.readNext();
      assert(chunk2 !== undefined);
      assertEquals(chunk2, new Uint8Array([4, 5]));

      const chunk3 = await buffer.readNext();
      assertEquals(chunk3, undefined);

      await buffer.close();
    });

    it("handles empty stream", async () => {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.close();
        },
      });
      const buffer = new StreamReadableBuffer(stream);

      const chunk = await buffer.readNext();
      assertEquals(chunk, undefined);

      await buffer.close();
    });
  });
});
