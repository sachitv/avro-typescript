import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { decodeFramedMessage, frameMessage } from "./framing.ts";

function toArrayBuffer(data: Uint8Array): ArrayBuffer {
  return data.slice().buffer;
}

describe("framing", () => {
  describe("frameMessage", () => {
    it("frames empty payload", () => {
      const payload = new Uint8Array(0);
      const framed = frameMessage(payload);
      // Should be just the terminator frame (4 bytes of zeros)
      assertEquals(framed.length, 4);
      assertEquals(framed, new Uint8Array(4));
    });

    it("frames small payload in single frame", () => {
      const payload = new Uint8Array([1, 2, 3]);
      const framed = frameMessage(payload);
      // Header (4 bytes) + payload (3 bytes) + terminator (4 bytes)
      assertEquals(framed.length, 11);
      // Check header: length 3
      const view = new DataView(framed.buffer);
      assertEquals(view.getUint32(0, false), 3);
      // Check payload
      assertEquals(framed.subarray(4, 7), payload);
      // Check terminator
      assertEquals(view.getUint32(7, false), 0);
    });

    it("frames large payload in multiple frames", () => {
      const payload = new Uint8Array(100);
      payload.fill(42);
      const framed = frameMessage(payload, { frameSize: 10 });
      // 10 frames of 10 bytes + terminator = 10*14 + 4 = 144 bytes
      // Each frame: 4 header + 10 data = 14 bytes
      assertEquals(framed.length, 144);
      const view = new DataView(framed.buffer);
      for (let i = 0; i < 10; i++) {
        const offset = i * 14;
        assertEquals(view.getUint32(offset, false), 10);
        assertEquals(
          framed.subarray(offset + 4, offset + 14),
          payload.subarray(i * 10, (i + 1) * 10),
        );
      }
      // Terminator
      assertEquals(view.getUint32(140, false), 0);
    });

    it("frames payload as ArrayBuffer", () => {
      const payload = new ArrayBuffer(3);
      new Uint8Array(payload).set([1, 2, 3]);
      const framed = frameMessage(payload);
      assertEquals(framed.length, 11);
      const view = new DataView(framed.buffer);
      assertEquals(view.getUint32(0, false), 3);
      assertEquals(framed.subarray(4, 7), new Uint8Array([1, 2, 3]));
    });

    it("uses default frame size when not specified", () => {
      const payload = new Uint8Array(8193); // Larger than default 8192
      const framed = frameMessage(payload);
      // Should have two frames + terminator
      const view = new DataView(framed.buffer);
      assertEquals(view.getUint32(0, false), 8192);
      assertEquals(view.getUint32(8192 + 4, false), 1);
      assertEquals(view.getUint32(8192 + 4 + 1 + 4, false), 0);
    });

    it("uses custom frame size", () => {
      const payload = new Uint8Array(5);
      const framed = frameMessage(payload, { frameSize: 2 });
      // Frames: 2 bytes, 2 bytes, 1 byte + terminator
      assertEquals(framed.length, 4 + 2 + 4 + 2 + 4 + 1 + 4);
      const view = new DataView(framed.buffer);
      assertEquals(view.getUint32(0, false), 2);
      assertEquals(view.getUint32(6, false), 2);
      assertEquals(view.getUint32(12, false), 1);
      assertEquals(view.getUint32(17, false), 0);
    });

    it("throws on invalid frame size", () => {
      const payload = new Uint8Array(1);
      assertThrows(
        () => frameMessage(payload, { frameSize: 0 }),
        RangeError,
        "positive integer",
      );
      assertThrows(
        () => frameMessage(payload, { frameSize: -1 }),
        RangeError,
        "positive integer",
      );
      assertThrows(
        () => frameMessage(payload, { frameSize: 1.5 }),
        RangeError,
        "positive integer",
      );
      assertThrows(
        () => frameMessage(payload, { frameSize: NaN }),
        RangeError,
        "positive integer",
      );
    });
  });

  describe("decodeFramedMessage", () => {
    it("decodes empty message", () => {
      const buffer = new ArrayBuffer(4);
      const result = decodeFramedMessage(buffer);
      assertEquals(result.payload.length, 0);
      assertEquals(result.nextOffset, 4);
    });

    it("decodes single frame", () => {
      const payload = new Uint8Array([1, 2, 3]);
      const framed = frameMessage(payload);
      const result = decodeFramedMessage(toArrayBuffer(framed));
      assertEquals(result.payload, payload);
      assertEquals(result.nextOffset, framed.length);
    });

    it("decodes multiple frames", () => {
      const payload = new Uint8Array([1, 2, 3, 4, 5, 6]);
      const framed = frameMessage(payload, { frameSize: 2 });
      const result = decodeFramedMessage(toArrayBuffer(framed));
      assertEquals(result.payload, payload);
      assertEquals(result.nextOffset, framed.length);
    });

    it("decodes with custom offset", () => {
      const payload1 = new Uint8Array([1, 2]);
      const payload2 = new Uint8Array([3, 4]);
      const framed1 = frameMessage(payload1);
      const framed2 = frameMessage(payload2);
      const combined = new Uint8Array(framed1.length + framed2.length);
      combined.set(framed1, 0);
      combined.set(framed2, framed1.length);

      const result1 = decodeFramedMessage(toArrayBuffer(combined));
      const result2 = decodeFramedMessage(toArrayBuffer(combined), {
        offset: result1.nextOffset,
      });

      assertEquals(result1.payload, payload1);
      assertEquals(result2.payload, payload2);
    });

    it("throws on invalid offset", () => {
      const buffer = new ArrayBuffer(4);
      assertThrows(
        () => decodeFramedMessage(buffer, { offset: -1 }),
        RangeError,
        "non-negative integer",
      );
      assertThrows(
        () => decodeFramedMessage(buffer, { offset: 1.5 }),
        RangeError,
        "non-negative integer",
      );
      assertThrows(
        () => decodeFramedMessage(buffer, { offset: 5 }),
        RangeError,
        "exceeds buffer length",
      );
    });

    it("throws on incomplete frame", () => {
      // Frame header says 10 bytes, but only 5 available
      const buffer = new ArrayBuffer(9); // 4 header + 5 data, but header says 10
      const view = new DataView(buffer);
      view.setUint32(0, 10, false);
      assertThrows(
        () => decodeFramedMessage(buffer),
        Error,
        "Incomplete frame data",
      );
    });

    it("throws on missing terminator", () => {
      // Frame with data but no terminator
      const buffer = new ArrayBuffer(8);
      const view = new DataView(buffer);
      view.setUint32(0, 4, false);
      new Uint8Array(buffer, 4, 4).set([1, 2, 3, 4]);
      // No terminator after
      assertThrows(
        () => decodeFramedMessage(buffer),
        Error,
        "Message terminator not found",
      );
    });

    it("throws when offset is in middle of frame", () => {
      const payload = new Uint8Array([1, 2, 3]);
      const framed = frameMessage(payload);
      // Offset in middle of first frame header, causing incomplete frame read
      assertThrows(
        () => decodeFramedMessage(toArrayBuffer(framed), { offset: 2 }),
        Error,
        "Incomplete frame data",
      );
    });
  });

  describe("end-to-end framing round-trip", () => {
    it("round-trips empty payload", () => {
      const payload = new Uint8Array(0);
      const framed = frameMessage(payload);
      const decoded = decodeFramedMessage(toArrayBuffer(framed));
      assertEquals(decoded.payload, payload);
    });

    it("round-trips small payload", () => {
      const payload = new Uint8Array([42]);
      const framed = frameMessage(payload);
      const decoded = decodeFramedMessage(toArrayBuffer(framed));
      assertEquals(decoded.payload, payload);
    });

    it("round-trips large payload with multiple frames", () => {
      const payload = new Uint8Array(10000);
      for (let i = 0; i < payload.length; i++) {
        payload[i] = i % 256;
      }
      const framed = frameMessage(payload, { frameSize: 1024 });
      const decoded = decodeFramedMessage(toArrayBuffer(framed));
      assertEquals(decoded.payload, payload);
    });

    it("round-trips payload as ArrayBuffer", () => {
      const buffer = new ArrayBuffer(5);
      const view = new Uint8Array(buffer);
      view.set([1, 2, 3, 4, 5]);
      const framed = frameMessage(buffer);
      const decoded = decodeFramedMessage(toArrayBuffer(framed));
      assertEquals(decoded.payload, new Uint8Array([1, 2, 3, 4, 5]));
    });

    it("round-trips with custom frame size", () => {
      const payload = new Uint8Array(100);
      payload.fill(99);
      const framed = frameMessage(payload, { frameSize: 7 });
      const decoded = decodeFramedMessage(toArrayBuffer(framed));
      assertEquals(decoded.payload, payload);
    });

    it("handles multiple messages in buffer with offset", () => {
      const payload1 = new Uint8Array([1, 2]);
      const payload2 = new Uint8Array([3, 4, 5]);
      const framed1 = frameMessage(payload1);
      const framed2 = frameMessage(payload2);
      const combined = new Uint8Array(framed1.length + framed2.length);
      combined.set(framed1, 0);
      combined.set(framed2, framed1.length);

      const decoded1 = decodeFramedMessage(toArrayBuffer(combined));
      const decoded2 = decodeFramedMessage(toArrayBuffer(combined), {
        offset: decoded1.nextOffset,
      });

      assertEquals(decoded1.payload, payload1);
      assertEquals(decoded2.payload, payload2);
      assertEquals(decoded2.nextOffset, combined.length);
    });
  });
});
