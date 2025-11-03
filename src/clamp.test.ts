import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import { clampLengthForView, getClampedLength } from "./clamp.ts";

describe("clamp utilities", () => {
  it("getClampedLength limits to available bytes when requested is less than available", () => {
    expect(getClampedLength(10, 3, 5)).toBe(5);
  });

  it("getClampedLength limits to available bytes when requested is more than available", () => {
    expect(getClampedLength(10, 8, 5)).toBe(2);
  });

  it("getClampedLength returns 0 when offset is equal to totalLength", () => {
    expect(getClampedLength(10, 10, 1)).toBe(0);
  });

  it("getClampedLength returns 0 when requested length is 0", () => {
    expect(getClampedLength(10, 2, 0)).toBe(0);
  });

  it("getClampedLength returns 0 when offset is negative", () => {
    expect(getClampedLength(10, -1, 5)).toBe(0);
  });

  it("clampLengthForView returns requested length when available is sufficient", () => {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    expect(clampLengthForView(view, 2, 4)).toBe(4);
  });

  it("clampLengthForView returns available length when requested is too large", () => {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    expect(clampLengthForView(view, 7, 4)).toBe(1);
  });

  it("clampLengthForView returns 0 when offset is beyond view byteLength", () => {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    expect(clampLengthForView(view, 8, 1)).toBe(0);
  });

  it("clampLengthForView returns 0 when requested length is 0", () => {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    expect(clampLengthForView(view, 4, 0)).toBe(0);
  });
});
