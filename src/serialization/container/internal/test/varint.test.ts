import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { SyncInMemoryWritableBuffer } from "../../../buffers/in_memory_buffer_sync.ts";
import { SyncWritableTap } from "../../../tap_sync.ts";
import { readSafeLong } from "../varint.ts";

function fail(reason: string): never {
  throw new RangeError(reason);
}

/** Encodes `value` with the library's own long writer. */
function encodeLong(value: bigint): Uint8Array {
  const buffer = new SyncInMemoryWritableBuffer(new ArrayBuffer(16));
  new SyncWritableTap(buffer).writeLong(value);
  return new Uint8Array(buffer.getBufferCopy());
}

const SAFE_VALUES = [
  0n,
  -1n,
  1n,
  63n,
  -64n,
  64n,
  300n,
  -300n,
  2n ** 31n,
  -(2n ** 31n) - 1n,
  2n ** 48n - 1n,
  -(2n ** 48n),
  2n ** 48n,
  2n ** 52n + 1n,
  BigInt(Number.MAX_SAFE_INTEGER),
  BigInt(Number.MIN_SAFE_INTEGER),
];

describe("readSafeLong", () => {
  it("decodes what the long writer encodes", () => {
    for (const value of SAFE_VALUES) {
      const bytes = encodeLong(value);
      assertEquals(readSafeLong(bytes, 0, fail), {
        value: Number(value),
        next: bytes.length,
      });
    }
  });

  it("starts at the given offset", () => {
    const bytes = Uint8Array.from([0xff, ...encodeLong(-300n), 0xff]);
    assertEquals(readSafeLong(bytes, 1, fail), { value: -300, next: 3 });
  });

  it("returns undefined when the bytes end inside the varint", () => {
    for (const value of SAFE_VALUES) {
      const bytes = encodeLong(value);
      for (let end = 0; end < bytes.length; end++) {
        assertEquals(readSafeLong(bytes.subarray(0, end), 0, fail), undefined);
      }
    }
  });

  it("fails for values outside the safe integer range", () => {
    for (const value of [2n ** 53n, -(2n ** 53n) - 1n, 2n ** 63n - 1n]) {
      assertThrows(
        () => readSafeLong(encodeLong(value), 0, fail),
        RangeError,
        `value ${value} is outside the safe integer range`,
      );
    }
  });

  it("fails for varints longer than ten bytes", () => {
    assertThrows(
      () => readSafeLong(new Uint8Array(10).fill(0x80), 0, fail),
      RangeError,
      "varint is longer than 10 bytes",
    );
  });
});
