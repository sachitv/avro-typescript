import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  assertNoBuiltInOverride,
  resolveCodecName,
  selectDecoder,
} from "../codec.ts";
import { UnsupportedCodecError } from "../errors.ts";

const encode = (text: string) => new TextEncoder().encode(text);

describe("resolveCodecName", () => {
  it("defaults to null when avro.codec is missing or empty", () => {
    assertEquals(resolveCodecName(new Map()), "null");
    assertEquals(
      resolveCodecName(new Map([["avro.codec", new Uint8Array()]])),
      "null",
    );
  });

  it("decodes the codec name", () => {
    assertEquals(
      resolveCodecName(new Map([["avro.codec", encode("deflate")]])),
      "deflate",
    );
  });
});

describe("assertNoBuiltInOverride", () => {
  it("accepts custom codecs that do not shadow built-ins", () => {
    assertNoBuiltInOverride({ null: 1 }, { snappy: 2, toString: 3 });
  });

  it("rejects a custom codec named like a built-in", () => {
    assertThrows(
      () => assertNoBuiltInOverride({ null: 1, deflate: 2 }, { deflate: 3 }),
      Error,
      "Cannot override built-in decoder for codec: deflate",
    );
  });
});

describe("selectDecoder", () => {
  const builtIns = { null: "built-in null" };
  const custom = { snappy: "custom snappy" };

  it("prefers built-ins, then custom decoders", () => {
    assertEquals(selectDecoder("null", builtIns, custom), "built-in null");
    assertEquals(selectDecoder("snappy", builtIns, custom), "custom snappy");
  });

  it("throws UnsupportedCodecError for unknown codecs", () => {
    const error = assertThrows(
      () => selectDecoder("zstandard", builtIns, custom),
      UnsupportedCodecError,
      "Unsupported codec: zstandard. Provide a custom decoder.",
    );
    assertEquals(error.codec, "zstandard");
  });

  it("does not resolve codec names through the prototype", () => {
    assertThrows(
      () => selectDecoder("toString", builtIns, custom),
      UnsupportedCodecError,
    );
  });
});
