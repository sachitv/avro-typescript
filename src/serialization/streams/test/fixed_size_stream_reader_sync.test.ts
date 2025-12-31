import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { SyncFixedSizeStreamReader } from "../fixed_size_stream_reader_sync.ts";

describe("SyncFixedSizeStreamReader", () => {
  it("reads sequential chunks", () => {
    const reader = new SyncFixedSizeStreamReader(
      new Uint8Array([1, 2, 3, 4, 5]),
      2,
    );

    assertEquals(reader.readNext(), new Uint8Array([1, 2]));
    assertEquals(reader.readNext(), new Uint8Array([3, 4]));
    assertEquals(reader.readNext(), new Uint8Array([5]));
    assertEquals(reader.readNext(), undefined);
  });

  it("throws on invalid chunk size", () => {
    assertThrows(
      () => new SyncFixedSizeStreamReader(new Uint8Array([1]), 0),
      RangeError,
      "chunkSize must be positive",
    );
  });

  it("stops reading when closed", () => {
    const reader = new SyncFixedSizeStreamReader(new Uint8Array([1, 2]), 2);
    reader.close();
    assertEquals(reader.readNext(), undefined);
  });

  it("exposes read offset for testing", () => {
    const reader = new SyncFixedSizeStreamReader(
      new Uint8Array([1, 2, 3]),
      2,
    );
    reader.readNext();
    assertEquals(reader._testOnlyOffset(), 2);
  });
});
