import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { CountingWritableTap } from "../counting_writable_tap.ts";
import { WritableTap } from "../tap.ts";

describe("CountingWritableTap", () => {
  describe("construction", () => {
    it("starts at position 0", () => {
      const tap = new CountingWritableTap();
      expect(tap.getPos()).toBe(0);
    });

    it("isValid always returns true", async () => {
      const tap = new CountingWritableTap();
      expect(await tap.isValid()).toBe(true);
    });
  });

  describe("primitive counting", () => {
    it("writeBoolean counts 1 byte", async () => {
      const tap = new CountingWritableTap();
      await tap.writeBoolean(true);
      expect(tap.getPos()).toBe(1);
      await tap.writeBoolean(false);
      expect(tap.getPos()).toBe(2);
    });

    it("writeFloat counts 4 bytes", async () => {
      const tap = new CountingWritableTap();
      await tap.writeFloat(3.14);
      expect(tap.getPos()).toBe(4);
    });

    it("writeDouble counts 8 bytes", async () => {
      const tap = new CountingWritableTap();
      await tap.writeDouble(3.14159265359);
      expect(tap.getPos()).toBe(8);
    });
  });

  describe("varint counting", () => {
    it("writeInt counts varint bytes correctly", async () => {
      const tap = new CountingWritableTap();
      await tap.writeInt(0);
      expect(tap.getPos()).toBe(1);
    });

    it("writeLong counts small values as 1 byte", async () => {
      const tap = new CountingWritableTap();
      await tap.writeLong(0n);
      expect(tap.getPos()).toBe(1);
    });

    it("writeLong counts negative values correctly", async () => {
      const tap = new CountingWritableTap();
      await tap.writeLong(-1n);
      expect(tap.getPos()).toBe(1);
    });

    it("writeLong counts large values correctly", async () => {
      const tap = new CountingWritableTap();
      await tap.writeLong(1440756011948n);
      expect(tap.getPos()).toBe(6);
    });
  });

  describe("binary data counting", () => {
    it("writeFixed counts exact buffer length", async () => {
      const tap = new CountingWritableTap();
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      await tap.writeFixed(data);
      expect(tap.getPos()).toBe(5);
    });


    it("writeBytes counts length prefix + data", async () => {
      const tap = new CountingWritableTap();
      const data = new Uint8Array([1, 2, 3]);
      await tap.writeBytes(data);
      // 1 byte for length (3 encoded as varint) + 3 bytes data
      expect(tap.getPos()).toBe(4);
    });

    it("writeString counts UTF-8 length prefix + encoded bytes", async () => {
      const tap = new CountingWritableTap();
      await tap.writeString("abc");
      // 1 byte for length (3 encoded as varint) + 3 bytes data
      expect(tap.getPos()).toBe(4);
    });

    it("writeString handles multibyte UTF-8 characters", async () => {
      const tap = new CountingWritableTap();
      // "七" is 3 UTF-8 bytes
      await tap.writeString("七");
      // 1 byte for length (3 encoded as varint) + 3 bytes data
      expect(tap.getPos()).toBe(4);
    });

    it("writeBinary counts specified length", async () => {
      const tap = new CountingWritableTap();
      await tap.writeBinary("\x01\x02\x03", 3);
      expect(tap.getPos()).toBe(3);
    });

    it("writeBinary with len=0 does nothing", async () => {
      const tap = new CountingWritableTap();
      await tap.writeBinary("abc", 0);
      expect(tap.getPos()).toBe(0);
    });

  });

  describe("parity with WritableTap", () => {
    it("counts same bytes as WritableTap for boolean", async () => {
      const counting = new CountingWritableTap();
      const writing = new WritableTap(new ArrayBuffer(64));

      await counting.writeBoolean(true);
      await writing.writeBoolean(true);
      expect(counting.getPos()).toBe(writing.getPos());
    });

    it("counts same bytes as WritableTap for int", async () => {
      const values = [0, 42, -1, 1234567, -1234567];
      for (const v of values) {
        const counting = new CountingWritableTap();
        const writing = new WritableTap(new ArrayBuffer(64));

        await counting.writeInt(v);
        await writing.writeInt(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as WritableTap for long", async () => {
      const values = [0n, -1n, 109213n, -1211n, -1312411211n, 900719925474090n];
      for (const v of values) {
        const counting = new CountingWritableTap();
        const writing = new WritableTap(new ArrayBuffer(64));

        await counting.writeLong(v);
        await writing.writeLong(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as WritableTap for float", async () => {
      const values = [0, 3.14, -5.5, 1e9];
      for (const v of values) {
        const counting = new CountingWritableTap();
        const writing = new WritableTap(new ArrayBuffer(64));

        await counting.writeFloat(v);
        await writing.writeFloat(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as WritableTap for double", async () => {
      const values = [0, 3.14159265359, -5.5, 1e12];
      for (const v of values) {
        const counting = new CountingWritableTap();
        const writing = new WritableTap(new ArrayBuffer(64));

        await counting.writeDouble(v);
        await writing.writeDouble(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as WritableTap for string", async () => {
      const values = ["", "hello", "七転び八起き", "abc\x00def"];
      for (const v of values) {
        const counting = new CountingWritableTap();
        const writing = new WritableTap(new ArrayBuffer(128));

        await counting.writeString(v);
        await writing.writeString(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as WritableTap for bytes", async () => {
      const values = [
        new Uint8Array([]),
        new Uint8Array([1, 2, 3]),
        new Uint8Array([255, 0, 128]),
      ];
      for (const v of values) {
        const counting = new CountingWritableTap();
        const writing = new WritableTap(new ArrayBuffer(64));

        await counting.writeBytes(v);
        await writing.writeBytes(v);
        expect(counting.getPos()).toBe(writing.getPos());
      }
    });

    it("counts same bytes as WritableTap for fixed", async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      const counting = new CountingWritableTap();
      const writing = new WritableTap(new ArrayBuffer(64));

      await counting.writeFixed(data);
      await writing.writeFixed(data);
      expect(counting.getPos()).toBe(writing.getPos());
    });
  });
});
