import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { SyncCountingWritableTap } from "../sync_counting_writable_tap.ts";
import { SyncWritableTap } from "../sync_tap.ts";
import { SyncInMemoryWritableBuffer } from "../buffers/sync_in_memory_buffer.ts";

describe("SyncCountingWritableTap", () => {
  describe("construction", () => {
    it("starts at position 0", () => {
      const tap = new SyncCountingWritableTap();
      expect(tap.getPos()).toBe(0);
    });

    it("isValid always returns true", () => {
      const tap = new SyncCountingWritableTap();
      expect(tap.isValid()).toBe(true);
    });
  });

  describe("primitive counting", () => {
    it("writeBoolean counts 1 byte", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeBoolean(true);
      expect(tap.getPos()).toBe(1);
      tap.writeBoolean(false);
      expect(tap.getPos()).toBe(2);
    });

    it("writeFloat counts 4 bytes", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeFloat(3.14);
      expect(tap.getPos()).toBe(4);
    });

    it("writeDouble counts 8 bytes", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeDouble(3.14159265359);
      expect(tap.getPos()).toBe(8);
    });
  });

  describe("varint counting", () => {
    it("writeInt counts small values as 1 byte", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeInt(0);
      expect(tap.getPos()).toBe(1);
    });

    it("writeInt counts negative values correctly", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeInt(-1);
      expect(tap.getPos()).toBe(1);
    });

    it("writeInt counts larger values correctly", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeInt(1234567);
      // zigzag encode: 1234567 << 1 = 2469134, which is > 0x7f
      expect(tap.getPos()).toBeGreaterThan(1);
    });

    it("writeLong counts small values as 1 byte", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeLong(0n);
      expect(tap.getPos()).toBe(1);
    });

    it("writeLong counts negative values correctly", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeLong(-1n);
      expect(tap.getPos()).toBe(1);
    });

    it("writeLong counts large values correctly", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeLong(1440756011948n);
      expect(tap.getPos()).toBe(6);
    });
  });

  describe("binary data counting", () => {
    it("writeFixed counts exact buffer length", () => {
      const tap = new SyncCountingWritableTap();
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      tap.writeFixed(data);
      expect(tap.getPos()).toBe(5);
    });

    it("writeBytes counts length prefix + data", () => {
      const tap = new SyncCountingWritableTap();
      const data = new Uint8Array([1, 2, 3]);
      tap.writeBytes(data);
      // 1 byte for length (3 encoded as varint) + 3 bytes data
      expect(tap.getPos()).toBe(4);
    });

    it("writeString counts UTF-8 length prefix + encoded bytes", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeString("abc");
      // 1 byte for length (3 encoded as varint) + 3 bytes data
      expect(tap.getPos()).toBe(4);
    });

    it("writeString handles multibyte UTF-8 characters", () => {
      const tap = new SyncCountingWritableTap();
      // "七" is 3 UTF-8 bytes
      tap.writeString("七");
      // 1 byte for length (3 encoded as varint) + 3 bytes data
      expect(tap.getPos()).toBe(4);
    });

    it("writeBinary counts specified length", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeBinary("\x01\x02\x03", 3);
      expect(tap.getPos()).toBe(3);
    });

    it("writeBinary with len=0 does nothing", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeBinary("abc", 0);
      expect(tap.getPos()).toBe(0);
    });
  });

  describe("parity with SyncWritableTap", () => {
    it("counts same bytes as SyncWritableTap for boolean", () => {
      const counting = new SyncCountingWritableTap();
      const writing = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(new ArrayBuffer(64)),
      );

      counting.writeBoolean(true);
      writing.writeBoolean(true);
      expect(counting.getPos()).toBe(writing.getPos());
    });

    it("counts same bytes as SyncWritableTap for int", () => {
      const values = [0, 42, -1, 1234567, -1234567, 2147483647, -2147483648];
      for (const v of values) {
        const counting = new SyncCountingWritableTap();
        const writing = new SyncWritableTap(
          new SyncInMemoryWritableBuffer(new ArrayBuffer(64)),
        );

        counting.writeInt(v);
        writing.writeInt(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as SyncWritableTap for long", () => {
      const values = [0n, -1n, 109213n, -1211n, -1312411211n, 900719925474090n];
      for (const v of values) {
        const counting = new SyncCountingWritableTap();
        const writing = new SyncWritableTap(
          new SyncInMemoryWritableBuffer(new ArrayBuffer(64)),
        );

        counting.writeLong(v);
        writing.writeLong(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as SyncWritableTap for float", () => {
      const values = [0, 3.14, -5.5, 1e9];
      for (const v of values) {
        const counting = new SyncCountingWritableTap();
        const writing = new SyncWritableTap(
          new SyncInMemoryWritableBuffer(new ArrayBuffer(64)),
        );

        counting.writeFloat(v);
        writing.writeFloat(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as SyncWritableTap for double", () => {
      const values = [0, 3.14159265359, -5.5, 1e12];
      for (const v of values) {
        const counting = new SyncCountingWritableTap();
        const writing = new SyncWritableTap(
          new SyncInMemoryWritableBuffer(new ArrayBuffer(64)),
        );

        counting.writeDouble(v);
        writing.writeDouble(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as SyncWritableTap for string", () => {
      const values = ["", "hello", "七転び八起き", "abc\x00def"];
      for (const v of values) {
        const counting = new SyncCountingWritableTap();
        const writing = new SyncWritableTap(
          new SyncInMemoryWritableBuffer(new ArrayBuffer(128)),
        );

        counting.writeString(v);
        writing.writeString(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as SyncWritableTap for bytes", () => {
      const values = [
        new Uint8Array([]),
        new Uint8Array([1, 2, 3]),
        new Uint8Array([255, 0, 128]),
      ];
      for (const v of values) {
        const counting = new SyncCountingWritableTap();
        const writing = new SyncWritableTap(
          new SyncInMemoryWritableBuffer(new ArrayBuffer(64)),
        );

        counting.writeBytes(v);
        writing.writeBytes(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as SyncWritableTap for fixed", () => {
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      const counting = new SyncCountingWritableTap();
      const writing = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(new ArrayBuffer(64)),
      );

      counting.writeFixed(data);
      writing.writeFixed(data);
      expect(counting.getPos()).toBe(writing.getPos());
    });
  });

  describe("edge cases", () => {
    it("handles INT32_MAX correctly", () => {
      const counting = new SyncCountingWritableTap();
      const writing = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(new ArrayBuffer(64)),
      );

      counting.writeInt(2147483647);
      writing.writeInt(2147483647);
      expect(counting.getPos()).toBe(writing.getPos());
    });

    it("handles INT32_MIN correctly", () => {
      const counting = new SyncCountingWritableTap();
      const writing = new SyncWritableTap(
        new SyncInMemoryWritableBuffer(new ArrayBuffer(64)),
      );

      counting.writeInt(-2147483648);
      writing.writeInt(-2147483648);
      expect(counting.getPos()).toBe(writing.getPos());
    });

    it("handles empty string", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeString("");
      // 1 byte for length (0 encoded as varint) + 0 bytes data
      expect(tap.getPos()).toBe(1);
    });

    it("handles empty bytes", () => {
      const tap = new SyncCountingWritableTap();
      tap.writeBytes(new Uint8Array(0));
      // 1 byte for length (0 encoded as varint) + 0 bytes data
      expect(tap.getPos()).toBe(1);
    });
  });
});
