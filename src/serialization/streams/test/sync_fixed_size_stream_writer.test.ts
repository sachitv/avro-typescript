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
});
