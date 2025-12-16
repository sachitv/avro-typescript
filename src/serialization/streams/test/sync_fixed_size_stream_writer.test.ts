import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { SyncFixedSizeStreamWriter } from "../sync_fixed_size_stream_writer.ts";

describe("SyncFixedSizeStreamWriter", () => {
  it("writes sequential data", () => {
    const writer = new SyncFixedSizeStreamWriter(new Uint8Array(4));
    writer.writeBytes(new Uint8Array([1, 2]));
    writer.writeBytes(new Uint8Array([3, 4]));

    assertEquals(writer.toUint8Array(), new Uint8Array([1, 2, 3, 4]));
    assertEquals(writer.remaining(), 0);
  });

  it("throws when writing past capacity", () => {
    const writer = new SyncFixedSizeStreamWriter(new Uint8Array(2));
    writer.writeBytes(new Uint8Array([1, 2]));
    assertThrows(
      () => writer.writeBytes(new Uint8Array([3])),
      RangeError,
      "Write operation exceeds buffer capacity",
    );
  });

  it("throws when writing after close", () => {
    const writer = new SyncFixedSizeStreamWriter(new Uint8Array(2));
    writer.close();
    assertThrows(
      () => writer.writeBytes(new Uint8Array([1])),
      Error,
      "Cannot write to a closed writer",
    );
  });

  it("ignores zero-length writes", () => {
    const writer = new SyncFixedSizeStreamWriter(new Uint8Array(2));
    writer.writeBytes(new Uint8Array());
    assertEquals(writer.remaining(), 2);
  });

  describe("writeBytesFrom", () => {
    it("writes a slice of the source array", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(4));
      const source = new Uint8Array([10, 20, 30, 40, 50]);
      writer.writeBytesFrom(source, 1, 3); // writes [20, 30, 40]
      assertEquals(writer.toUint8Array(), new Uint8Array([20, 30, 40]));
      assertEquals(writer.remaining(), 1);
    });

    it("ignores zero-length writes", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(2));
      writer.writeBytesFrom(new Uint8Array([1, 2, 3]), 0, 0);
      assertEquals(writer.remaining(), 2);
    });

    it("throws when writing after close", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(4));
      writer.close();
      assertThrows(
        () => writer.writeBytesFrom(new Uint8Array([1, 2]), 0, 2),
        Error,
        "Cannot write to a closed writer",
      );
    });

    it("throws when writing past capacity", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(2));
      assertThrows(
        () => writer.writeBytesFrom(new Uint8Array([1, 2, 3, 4]), 0, 4),
        RangeError,
        "Write operation exceeds buffer capacity",
      );
    });

    it("throws for invalid offset (negative)", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(10));
      assertThrows(
        () => writer.writeBytesFrom(new Uint8Array([1, 2, 3]), -1, 2),
        RangeError,
        "Invalid source range",
      );
    });

    it("throws for invalid length (negative)", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(10));
      assertThrows(
        () => writer.writeBytesFrom(new Uint8Array([1, 2, 3]), 0, -1),
        RangeError,
        "Invalid source range",
      );
    });

    it("throws for offset + length exceeding source data", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(10));
      assertThrows(
        () => writer.writeBytesFrom(new Uint8Array([1, 2, 3]), 2, 5),
        RangeError,
        "Invalid source range",
      );
    });

    it("throws for non-integer offset", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(10));
      assertThrows(
        () => writer.writeBytesFrom(new Uint8Array([1, 2, 3]), 0.5, 2),
        RangeError,
        "Invalid source range",
      );
    });

    it("throws for non-integer length", () => {
      const writer = new SyncFixedSizeStreamWriter(new Uint8Array(10));
      assertThrows(
        () => writer.writeBytesFrom(new Uint8Array([1, 2, 3]), 0, 1.5),
        RangeError,
        "Invalid source range",
      );
    });
  });
});
