import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import { invert } from "./manipulate_bytes.ts";

describe("invert", () => {
  it("bitwise inverts requested bytes", () => {
    const buffer = new Uint8Array([0x00, 0xff, 0x55, 0xaa]);
    invert(buffer, buffer.length);
    expect(Array.from(buffer)).toEqual([0xff, 0x00, 0xaa, 0x55]);
  });
});
