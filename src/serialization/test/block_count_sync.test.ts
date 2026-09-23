import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import { readBlockCountSync } from "../block_count_sync.ts";
import { DirectSyncReadableTap } from "../direct_tap_sync.ts";
import { SyncReadableTap, type SyncReadableTapLike } from "../tap_sync.ts";

const taps: Array<{
  name: string;
  open: (bytes: number[]) => SyncReadableTapLike & { getPos(): number };
}> = [
  {
    name: "SyncReadableTap",
    open: (bytes) => new SyncReadableTap(new Uint8Array(bytes).buffer),
  },
  {
    name: "DirectSyncReadableTap",
    open: (bytes) => {
      const tap = new DirectSyncReadableTap(new Uint8Array(bytes));
      return Object.assign(tap, { getPos: () => tap.pos });
    },
  },
];

describe("readBlockCountSync", () => {
  for (const { name, open } of taps) {
    describe(name, () => {
      it("returns zero for the terminating block", () => {
        const tap = open([0x00]);
        expect(readBlockCountSync(tap, "Test block length")).toBe(0);
        expect(tap.getPos()).toBe(1);
      });

      it("returns a positive count and stops at the first item", () => {
        // zig-zag 3 = 6, followed by an item byte that must not be consumed.
        const tap = open([0x06, 0x7f]);
        expect(readBlockCountSync(tap, "Test block length")).toBe(3);
        expect(tap.getPos()).toBe(1);
      });

      it("negates a size-prefixed count and skips the block size", () => {
        // zig-zag -2 = 3, then block size 300 (a two-byte varint), then an
        // item byte.
        const tap = open([0x03, 0xd8, 0x04, 0x7f]);
        expect(readBlockCountSync(tap, "Test block length")).toBe(2);
        expect(tap.getPos()).toBe(3);
      });

      it("decodes counts wider than the int32 fast path", () => {
        // Count 1 padded to six varint bytes.
        const tap = open([0x82, 0x80, 0x80, 0x80, 0x80, 0x00]);
        expect(readBlockCountSync(tap, "Test block length")).toBe(1);
        expect(tap.getPos()).toBe(6);
      });

      it("names the block in the out-of-range error", () => {
        // zig-zag(2^53) = 2^54: seven continuation bytes, then bit 54 as
        // bit 5 of the eighth byte.
        const tap = open([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20]);
        expect(() => readBlockCountSync(tap, "Test block length")).toThrow(
          "Test block length value 9007199254740992 is outside the safe integer range.",
        );
      });
    });
  }
});
