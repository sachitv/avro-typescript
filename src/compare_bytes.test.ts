import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import { compareByteRanges, compareUint8Arrays } from "./compare_bytes.ts";

describe("compare bytes utilities", () => {
  it("compareByteRanges differentiates views", () => {
    const a = new Uint8Array([1, 2, 3, 4]);
    const b = new Uint8Array([1, 2, 4, 4]);
    const viewA = new DataView(a.buffer);
    const viewB = new DataView(b.buffer);
    expect(compareByteRanges(viewA, 0, 4, viewB, 0, 4)).toBeLessThan(0);
    expect(compareByteRanges(viewB, 0, 4, viewA, 0, 4)).toBeGreaterThan(0);
    expect(compareByteRanges(viewA, -2, 4, viewB, -3, 4)).toBeLessThan(0);
    expect(compareByteRanges(viewA, 10, 2, viewB, 0, 4)).toBeLessThan(0);
    const short = new Uint8Array([1, 2, 3]).buffer;
    const long = new Uint8Array([1, 2, 3, 0]).buffer;
    expect(
      compareByteRanges(new DataView(short), 0, 4, new DataView(long), 0, 4),
    ).toBeLessThan(0);
    expect(
      compareByteRanges(new DataView(long), 0, 4, new DataView(short), 0, 4),
    ).toBeGreaterThan(0);
  });

  it("compareUint8Arrays handles equal arrays", () => {
    const a = new Uint8Array([5, 6, 7]);
    const b = new Uint8Array([5, 6, 7]);
    expect(compareUint8Arrays(a, b)).toBe(0);
    const c = new Uint8Array([5, 7, 7]);
    expect(compareUint8Arrays(a, c)).toBeLessThan(0);
    expect(compareUint8Arrays(c, a)).toBeGreaterThan(0);
  });
});
