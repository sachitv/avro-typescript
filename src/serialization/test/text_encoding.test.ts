import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import { decode, encode, utf8ByteLength } from "../text_encoding.ts";

describe("text encoding helpers", () => {
  it("encode converts string to Uint8Array", () => {
    const bytes = encode("abc");
    expect(Array.from(bytes)).toEqual([97, 98, 99]);
  });

  it("decode converts Uint8Array to string", () => {
    const str = decode(new Uint8Array([0x68, 0x69]));
    expect(str).toBe("hi");
  });

  describe("utf8ByteLength", () => {
    it("counts ASCII as one byte per code unit", () => {
      expect(utf8ByteLength("abc")).toBe(3);
    });

    it("counts 2-byte UTF-8 characters", () => {
      expect(utf8ByteLength("\u00e9")).toBe(2);
      expect(utf8ByteLength("\u00e9".repeat(40))).toBe(80);
    });

    it("counts 3-byte UTF-8 characters", () => {
      expect(utf8ByteLength("\u20ac")).toBe(3); // €
      expect(utf8ByteLength("\u2603")).toBe(3); // ☃
    });

    it("counts surrogate pairs as 4 bytes", () => {
      expect(utf8ByteLength("\ud83d\ude80")).toBe(4); // 🚀
      expect(utf8ByteLength("a\ud83d\ude80b")).toBe(6);
    });

    it("treats unpaired high surrogate as U+FFFD (3 bytes)", () => {
      const value = "\ud800";
      expect(utf8ByteLength(value)).toBe(3);
      expect(utf8ByteLength(value.repeat(40))).toBe(120);
    });

    it("treats unpaired low surrogate as U+FFFD (3 bytes)", () => {
      const value = "\udc00";
      expect(utf8ByteLength(value)).toBe(3);
    });

    it("treats unmatched high surrogate followed by normal char as U+FFFD", () => {
      // Not a valid pair since 'a' isn't a low surrogate.
      expect(utf8ByteLength("\ud800a")).toBe(4);
    });

    it("matches TextEncoder output length", () => {
      const input = "a\u00e9\u20ac\ud83d\ude80";
      expect(utf8ByteLength(input)).toBe(encode(input).length);
    });
  });
});
