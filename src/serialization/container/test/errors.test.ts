import { assertEquals, assertInstanceOf } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  AvroContainerError,
  InvalidBlockError,
  InvalidHeaderError,
  InvalidMagicError,
  SyncMismatchError,
  UnsupportedCodecError,
} from "../errors.ts";

describe("container errors", () => {
  it("share a base class and name themselves after the subclass", () => {
    const errors = [
      new InvalidMagicError(),
      new InvalidHeaderError("bad header"),
      new InvalidBlockError(7, "bad block"),
      new SyncMismatchError(9),
      new UnsupportedCodecError("snappy"),
    ];
    for (const error of errors) {
      assertInstanceOf(error, AvroContainerError);
      assertInstanceOf(error, Error);
      assertEquals(error.name, error.constructor.name);
    }
  });

  it("keeps the cause", () => {
    const cause = new Error("inner");
    assertEquals(new InvalidHeaderError("outer", { cause }).cause, cause);
  });

  it("reports sync mismatches with the parser's existing message", () => {
    const error = new SyncMismatchError(42);
    assertEquals(error.blockOffset, 42);
    assertEquals(
      error.message,
      "Invalid AVRO file: sync marker mismatch for block at offset 42",
    );
  });
});
