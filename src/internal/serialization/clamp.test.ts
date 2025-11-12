import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import { clampLengthForView } from "./clamp.ts";

describe("clampLengthForView", () => {
  it("returns requested length when available is sufficient", () => {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    expect(clampLengthForView(view, 2, 4)).toBe(4);
  });

  it("returns available length when requested is too large", () => {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    expect(clampLengthForView(view, 7, 4)).toBe(1);
  });

  it("returns 0 when offset is beyond view byteLength", () => {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    expect(clampLengthForView(view, 8, 1)).toBe(0);
  });

  it("returns 0 when requested length is 0", () => {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    expect(clampLengthForView(view, 4, 0)).toBe(0);
  });
});
