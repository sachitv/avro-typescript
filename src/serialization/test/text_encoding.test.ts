import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import {
  decode,
  decodeUtf8Range,
  decodeWithFastPath,
  encode,
  utf8ByteLength,
} from "../text_encoding.ts";

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

  describe("decodeWithFastPath", () => {
    it("decodes empty array", () => {
      expect(decodeWithFastPath(new Uint8Array([]))).toBe("");
    });

    it("decodes short ASCII string via fast path", () => {
      const bytes = new Uint8Array([104, 101, 108, 108, 111]);
      expect(decodeWithFastPath(bytes)).toBe("hello");
    });

    it("decodes ASCII string exactly 8 bytes (single chunk)", () => {
      const bytes = encode("abcdefgh");
      expect(decodeWithFastPath(bytes)).toBe("abcdefgh");
    });

    it("decodes ASCII string with remainder after 8-byte chunks", () => {
      const bytes = encode("abcdefghijk");
      expect(decodeWithFastPath(bytes)).toBe("abcdefghijk");
    });

    it("decodes ASCII string exactly 48 bytes (threshold boundary)", () => {
      const input = "a".repeat(48);
      const bytes = encode(input);
      expect(decodeWithFastPath(bytes)).toBe(input);
    });

    it("falls back to TextDecoder for strings over 48 bytes", () => {
      const input = "a".repeat(49);
      const bytes = encode(input);
      expect(decodeWithFastPath(bytes)).toBe(input);
    });

    it("falls back to TextDecoder for non-ASCII in small string", () => {
      const bytes = encode("café");
      expect(decodeWithFastPath(bytes)).toBe("café");
    });

    it("falls back to TextDecoder for multi-byte UTF-8 characters", () => {
      const bytes = encode("\u20ac");
      expect(decodeWithFastPath(bytes)).toBe("€");
    });

    it("falls back to TextDecoder for emoji (4-byte UTF-8)", () => {
      const bytes = encode("\ud83d\ude80");
      expect(decodeWithFastPath(bytes)).toBe("🚀");
    });

    it("matches decode output for various inputs", () => {
      const inputs = [
        "",
        "a",
        "hello",
        "12345678",
        "123456789012345678901234567890ab",
        "123456789012345678901234567890abc",
        "héllo",
        "日本語",
        "\ud83d\ude00\ud83d\ude01",
      ];
      for (const input of inputs) {
        const bytes = encode(input);
        expect(decodeWithFastPath(bytes)).toBe(decode(bytes));
      }
    });
  });

  describe("decodeUtf8Range", () => {
    // Surround the encoded payload with sentinel bytes so an off-by-one range
    // would corrupt the decoded value.
    function withPadding(payload: Uint8Array): {
      bytes: Uint8Array;
      start: number;
      end: number;
    } {
      const bytes = new Uint8Array(payload.length + 4);
      bytes.fill(0xff);
      bytes.set(payload, 2);
      return { bytes, start: 2, end: 2 + payload.length };
    }

    it("decodes an empty range", () => {
      const { bytes, start } = withPadding(new Uint8Array([]));
      expect(decodeUtf8Range(bytes, start, start)).toBe("");
    });

    it("decodes a short ASCII range mid-buffer via fast path", () => {
      const { bytes, start, end } = withPadding(encode("hello"));
      expect(decodeUtf8Range(bytes, start, end)).toBe("hello");
    });

    it("decodes an ASCII range spanning full 8-byte chunks", () => {
      const { bytes, start, end } = withPadding(encode("abcdefghijklmnop"));
      expect(decodeUtf8Range(bytes, start, end)).toBe("abcdefghijklmnop");
    });

    it("decodes an ASCII range with remainder after 8-byte chunks", () => {
      const { bytes, start, end } = withPadding(encode("abcdefghijk"));
      expect(decodeUtf8Range(bytes, start, end)).toBe("abcdefghijk");
    });

    it("falls back to TextDecoder for a non-ASCII range mid-buffer", () => {
      const { bytes, start, end } = withPadding(encode("café"));
      expect(decodeUtf8Range(bytes, start, end)).toBe("café");
    });

    it("falls back to TextDecoder for a large ASCII range mid-buffer", () => {
      const input = "a".repeat(64);
      const { bytes, start, end } = withPadding(encode(input));
      expect(decodeUtf8Range(bytes, start, end)).toBe(input);
    });

    it("decodes a full-buffer non-ASCII range without slicing", () => {
      const bytes = encode("日本語");
      expect(decodeUtf8Range(bytes, 0, bytes.length)).toBe("日本語");
    });

    it("matches decodeWithFastPath on equivalent full ranges", () => {
      const inputs = ["", "a", "hello", "héllo", "a".repeat(33), "🚀"];
      for (const input of inputs) {
        const bytes = encode(input);
        expect(decodeUtf8Range(bytes, 0, bytes.length)).toBe(
          decodeWithFastPath(bytes),
        );
      }
    });

    it("decodes every ASCII length 0-40 (all chunk/remainder combinations)", () => {
      const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN";
      for (let len = 0; len <= 40; len++) {
        const input = alphabet.slice(0, len);
        const { bytes, start, end } = withPadding(encode(input));
        expect(decodeUtf8Range(bytes, start, end)).toBe(input);
      }
    });

    it("clamps an end past the buffer instead of fabricating NUL bytes", () => {
      const bytes = encode("hello");
      const result = decodeUtf8Range(bytes, 0, bytes.length + 10);
      expect(result).toBe("hello");
      expect(result).not.toContain("\u0000");
    });

    it("clamps a negative start to zero", () => {
      const bytes = encode("hi");
      expect(decodeUtf8Range(bytes, -3, bytes.length)).toBe("hi");
    });

    it("returns empty string for an inverted range", () => {
      const bytes = encode("hello");
      expect(decodeUtf8Range(bytes, 4, 2)).toBe("");
    });

    it("matches subarray-based decoding for out-of-bounds ranges", () => {
      // Note: negative starts are clamped to 0 rather than given subarray's
      // from-the-end meaning; tap callers only ever pass non-negative starts.
      const bytes = encode("truncated input");
      for (const [start, end] of [[0, 100], [5, 999], [10, 3]]) {
        expect(decodeUtf8Range(bytes, start!, end!)).toBe(
          decode(bytes.subarray(start!, end!)),
        );
      }
    });
  });
});
