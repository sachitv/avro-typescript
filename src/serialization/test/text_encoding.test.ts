import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import { decode, encode } from "../text_encoding.ts";

describe("text encoding helpers", () => {
  it("encode converts string to Uint8Array", () => {
    const bytes = encode("abc");
    expect(Array.from(bytes)).toEqual([97, 98, 99]);
  });

  it("decode converts Uint8Array to string", () => {
    const str = decode(new Uint8Array([0x68, 0x69]));
    expect(str).toBe("hi");
  });
});
