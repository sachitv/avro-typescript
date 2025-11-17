import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import { readUIntLE } from "../read_uint_le.ts";

describe("readUIntLE", () => {
  it("reads little endian integers within bounds", () => {
    const buffer = new Uint8Array([0x34, 0x12, 0x56, 0x78]).buffer;
    const view = new DataView(buffer);
    expect(readUIntLE(view, 0, 4)).toBe(0x78561234 >>> 0);
  });

  it("stops reading when exceeding view length", () => {
    const buffer = new Uint8Array([0xff, 0x00]).buffer;
    const view = new DataView(buffer);
    expect(readUIntLE(view, 1, 4)).toBe(0);
  });
});
