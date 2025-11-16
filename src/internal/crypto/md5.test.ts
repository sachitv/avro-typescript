import { assert, assertEquals, assertExists } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { md5, md5FromString } from "./md5.ts";

describe("md5", () => {
  it("returns a 16-byte hash for a string", () => {
    const result = md5FromString("hello");
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("returns a 16-byte hash for an empty string", () => {
    const result = md5FromString("");
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("produces correct hashes for known test vectors", () => {
    const testCases = [
      { input: "", expected: "d41d8cd98f00b204e9800998ecf8427e" },
      { input: "a", expected: "0cc175b9c0f1b6a831c399e269772661" },
      { input: "abc", expected: "900150983cd24fb0d6963f7d28e17f72" },
      { input: "message digest", expected: "f96b697d7cb7938d525a2f31aaf161d0" },
      {
        input: "abcdefghijklmnopqrstuvwxyz",
        expected: "c3fcd3d76192e4007dfb496cca67e13b",
      },
      {
        input: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
        expected: "d174ab98d277d9f5a5611c2c9f419d9f",
      },
      {
        input:
          "12345678901234567890123456789012345678901234567890123456789012345678901234567890",
        expected: "57edf4a22be3c955ac49da2e2107b67a",
      },
    ];

    for (const testCase of testCases) {
      const result = md5FromString(testCase.input);
      const hex = Array.from(result)
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");
      assertEquals(
        hex,
        testCase.expected,
        `Failed for input: "${testCase.input}"`,
      );
    }
  });

  it("returns a 16-byte hash for a Uint8Array", () => {
    const input = new Uint8Array([104, 101, 108, 108, 111]); // "hello"
    const result = md5(input);
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("returns a 16-byte hash for an empty Uint8Array", () => {
    const input = new Uint8Array(0);
    const result = md5(input);
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("returns a 16-byte hash for a single byte", () => {
    const input = new Uint8Array([97]); // "a"
    const result = md5(input);
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("returns a 16-byte hash for multiple bytes", () => {
    const input = new Uint8Array([97, 98, 99]); // "abc"
    const result = md5(input);
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("produces consistent results between md5 and md5FromString", () => {
    const testString = "test message";
    const resultFromString = md5FromString(testString);
    const resultFromBytes = md5(new TextEncoder().encode(testString));

    assertEquals(resultFromString, resultFromBytes);
  });

  it("handles long inputs spanning multiple blocks", () => {
    const input = "a".repeat(1000);
    const result = md5FromString(input);
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("handles exactly 56 bytes input", () => {
    const input = "a".repeat(56);
    const result = md5FromString(input);
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("handles exactly 64 bytes input", () => {
    const input = "a".repeat(64);
    const result = md5FromString(input);
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("handles 63 bytes input", () => {
    const input = "a".repeat(63);
    const result = md5FromString(input);
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("handles 65 bytes input", () => {
    const input = "a".repeat(65);
    const result = md5FromString(input);
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("handles very long inputs", () => {
    const input = "a".repeat(10000);
    const result = md5FromString(input);
    assertExists(result);
    assertEquals(result.length, 16);
  });

  it("handles unicode characters", () => {
    const testCases = [
      "hello world",
      "¡Hola!",
      "你好",
      "🌍",
      "test with émojis 🚀",
    ];

    for (const testCase of testCases) {
      const result = md5FromString(testCase);
      assertExists(result);
      assertEquals(result.length, 16);
    }
  });

  it("handles binary data", () => {
    const testCases = [
      new Uint8Array([0x00]),
      new Uint8Array([0xff]),
      new Uint8Array([0x00, 0xff, 0x80, 0x7f]),
      new Uint8Array(Array.from({ length: 256 }, (_, i) => i % 256)),
    ];

    for (const testCase of testCases) {
      const result = md5(testCase);
      assertExists(result);
      assertEquals(result.length, 16);
    }
  });

  it("produces deterministic results", () => {
    const input = "deterministic test";
    const result1 = md5FromString(input);
    const result2 = md5FromString(input);

    assertEquals(result1, result2);
  });

  it("produces different outputs for different inputs", () => {
    const inputs = ["test1", "test2", "test3"];
    const results = inputs.map((input) => md5FromString(input));

    // All results should be different
    for (let i = 0; i < results.length; i++) {
      for (let j = i + 1; j < results.length; j++) {
        const resultsEqual = results[i].every((byte, index) =>
          byte === results[j][index]
        );
        assertEquals(
          resultsEqual,
          false,
          `Results for "${inputs[i]}" and "${inputs[j]}" should be different`,
        );
      }
    }
  });

  it("exhibits avalanche effect for small input changes", () => {
    const input1 = "test message";
    const input2 = "test messagf"; // Changed last character
    const result1 = md5FromString(input1);
    const result2 = md5FromString(input2);

    // Count differing bits
    let differingBits = 0;
    for (let i = 0; i < result1.length; i++) {
      const xor = result1[i] ^ result2[i];
      for (let bit = 0; bit < 8; bit++) {
        if (xor & (1 << bit)) {
          differingBits++;
        }
      }
    }

    // Should have significant difference (avalanche effect)
    assert(
      differingBits > 50,
      `Avalanche effect test failed: only ${differingBits} bits differ`,
    );
  });

  it("returns results in correct format", () => {
    const result = md5FromString("test");

    // Check length is exactly 16 bytes
    assertEquals(result.length, 16);

    // Check it's a Uint8Array
    assert(result instanceof Uint8Array);

    // Check all values are valid bytes (0-255)
    for (let i = 0; i < result.length; i++) {
      assert(
        result[i] >= 0 && result[i] <= 255,
        `Invalid byte value at index ${i}: ${result[i]}`,
      );
    }
  });

  it("matches known NIST test vectors", () => {
    const testCases = [
      {
        input: "",
        expected: [
          0xd4,
          0x1d,
          0x8c,
          0xd9,
          0x8f,
          0x00,
          0xb2,
          0x04,
          0xe9,
          0x80,
          0x09,
          0x98,
          0xec,
          0xf8,
          0x42,
          0x7e,
        ],
      },
      {
        input: "a",
        expected: [
          0x0c,
          0xc1,
          0x75,
          0xb9,
          0xc0,
          0xf1,
          0xb6,
          0xa8,
          0x31,
          0xc3,
          0x99,
          0xe2,
          0x69,
          0x77,
          0x26,
          0x61,
        ],
      },
      {
        input: "abc",
        expected: [
          0x90,
          0x01,
          0x50,
          0x98,
          0x3c,
          0xd2,
          0x4f,
          0xb0,
          0xd6,
          0x96,
          0x3f,
          0x7d,
          0x28,
          0xe1,
          0x7f,
          0x72,
        ],
      },
    ];

    for (const testCase of testCases) {
      const result = md5FromString(testCase.input);
      for (let i = 0; i < 16; i++) {
        assertEquals(
          result[i],
          testCase.expected[i],
          `Byte ${i} mismatch for input "${testCase.input}"`,
        );
      }
    }
  });
});
