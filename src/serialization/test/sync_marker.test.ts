import { assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { assertSyncMarker } from "../sync_marker.ts";

describe("assertSyncMarker", () => {
  const expected = Uint8Array.from({ length: 16 }, (_, i) => i);

  it("accepts a matching marker", () => {
    assertSyncMarker(expected.slice(), expected, 0);
  });

  it("reports the block offset on mismatch", () => {
    const actual = expected.slice();
    actual[15] ^= 0xff;
    assertThrows(
      () => assertSyncMarker(actual, expected, 1234),
      Error,
      "sync marker mismatch for block at offset 1234",
    );
  });
});
