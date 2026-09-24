import {
  assert,
  assertEquals,
  assertInstanceOf,
  assertThrows,
} from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { InvalidHeaderError, InvalidMagicError } from "../../errors.ts";
import { isNeedMore } from "../../need_more.ts";
import { avroString, SYNC, varint } from "../../test/container_fixtures.ts";
import {
  checkMagic,
  parseSchema,
  readLengthPrefixed,
  readMapBlockCount,
  readMetadata,
  readMetadataEntry,
  readSyncMarker,
} from "../header_sections.ts";

const MAGIC = [0x4f, 0x62, 0x6a, 0x01];
const encoder = new TextEncoder();

describe("checkMagic", () => {
  it("accepts the four magic bytes", () => {
    assertEquals(checkMagic(Uint8Array.from([...MAGIC, 0xff])), undefined);
  });

  it("asks for four bytes while the available ones match", () => {
    for (let end = 0; end < MAGIC.length; end++) {
      const result = checkMagic(Uint8Array.from(MAGIC.slice(0, end)));
      assert(result !== undefined && isNeedMore(result));
      assertEquals(result.minBytes, 4);
    }
  });

  it("throws at the first byte that differs", () => {
    assertThrows(() => checkMagic(Uint8Array.of(0x50)), InvalidMagicError);
    assertThrows(
      () => checkMagic(Uint8Array.of(0x4f, 0x62, 0x6a, 0x02)),
      InvalidMagicError,
    );
  });
});

describe("readMapBlockCount", () => {
  it("reads a positive count", () => {
    const bytes = Uint8Array.from([0xff, ...varint(3), 0xff]);
    assertEquals(readMapBlockCount(bytes, 1), { items: 3, next: 2 });
  });

  it("reads the terminating zero", () => {
    assertEquals(readMapBlockCount(Uint8Array.of(0), 0), {
      items: 0,
      next: 1,
    });
  });

  it("reads a negative count as its magnitude and skips the byte size", () => {
    const bytes = Uint8Array.from([...varint(-2), ...varint(300)]);
    assertEquals(readMapBlockCount(bytes, 0), {
      items: 2,
      next: bytes.length,
    });
  });

  it("asks for more when the count or the size is cut off", () => {
    const bytes = Uint8Array.from([...varint(-2), ...varint(300)]);
    for (let end = 0; end < bytes.length; end++) {
      assertEquals(readMapBlockCount(bytes.subarray(0, end), 0), {
        needMore: true,
      });
    }
  });

  it("rejects a negative byte size after a negative count", () => {
    const bytes = Uint8Array.from([...varint(-1), ...varint(-1)]);
    assertThrows(
      () => readMapBlockCount(bytes, 0),
      InvalidHeaderError,
      "Invalid AVRO file header: negative metadata block size -1",
    );
  });

  it("rejects a malformed count", () => {
    assertThrows(
      () => readMapBlockCount(new Uint8Array(10).fill(0x80), 0),
      InvalidHeaderError,
      "varint is longer than 10 bytes",
    );
  });
});

describe("readLengthPrefixed", () => {
  // Field names are only built for error messages.
  const field = () => {
    throw new Error("field name built without an error");
  };

  it("locates the field without copying it", () => {
    const bytes = Uint8Array.from([0xff, ...avroString("abc"), 0xff]);
    assertEquals(readLengthPrefixed(bytes, 1, field), { start: 2, end: 5 });
  });

  it("accepts an empty field", () => {
    assertEquals(readLengthPrefixed(Uint8Array.of(0), 0, field), {
      start: 1,
      end: 1,
    });
  });

  it("asks for more without minBytes when the length is cut off", () => {
    const bytes = Uint8Array.from(varint(300));
    assertEquals(readLengthPrefixed(bytes.subarray(0, 1), 0, field), {
      needMore: true,
    });
  });

  it("asks for the field's end when its contents are cut off", () => {
    const bytes = Uint8Array.from(avroString("abcdef"));
    const result = readLengthPrefixed(bytes.subarray(0, 3), 0, field);
    assert(isNeedMore(result));
    assertEquals(result.minBytes, bytes.length);
  });

  it("names the field when the length is negative", () => {
    assertThrows(
      () => readLengthPrefixed(Uint8Array.from(varint(-1)), 0, () => "thing"),
      InvalidHeaderError,
      "Invalid AVRO file header: negative length for thing",
    );
  });

  it("rejects a length whose end would not be a safe integer", () => {
    const bytes = Uint8Array.from([0xff, ...varint(Number.MAX_SAFE_INTEGER)]);
    assertThrows(
      () => readLengthPrefixed(bytes, 1, () => "thing"),
      InvalidHeaderError,
      `length ${Number.MAX_SAFE_INTEGER} for thing is too large`,
    );
    // The largest length whose end is still safe is a request, not an error.
    const largest = Number.MAX_SAFE_INTEGER - (1 + 8);
    const edge = Uint8Array.from([0xff, ...varint(largest)]);
    assertEquals(edge.length, 1 + 8);
    const result = readLengthPrefixed(edge, 1, field);
    assert(isNeedMore(result));
    assertEquals(result.minBytes, Number.MAX_SAFE_INTEGER);
  });
});

describe("readMetadataEntry", () => {
  const entry = Uint8Array.from([...avroString("k"), ...avroString("value")]);

  it("locates the key and value without decoding or copying", () => {
    assertEquals(readMetadataEntry(entry, 0), {
      key: { start: 1, end: 2 },
      value: { start: 3, end: entry.length },
    });
  });

  it("asks for more at every truncation point", () => {
    for (let end = 0; end < entry.length; end++) {
      const result = readMetadataEntry(entry.subarray(0, end), 0);
      assert(isNeedMore(result), `prefix of ${end} bytes`);
      assert(result.minBytes === undefined || result.minBytes > end);
    }
  });

  it("names the key when its value length is negative", () => {
    const bytes = Uint8Array.from([...avroString("k"), ...varint(-3)]);
    assertThrows(
      () => readMetadataEntry(bytes, 0),
      InvalidHeaderError,
      'negative length for metadata value "k"',
    );
  });
});

describe("readMetadata", () => {
  it("reads an empty map", () => {
    assertEquals(readMetadata(Uint8Array.of(0xff, 0), 1), {
      meta: new Map(),
      next: 2,
    });
  });

  it("reads entries across positive and negative blocks", () => {
    const second = [...avroString("b"), ...avroString("2")];
    const bytes = Uint8Array.from([
      ...varint(1),
      ...avroString("a"),
      ...avroString("1"),
      ...varint(-1),
      ...varint(second.length),
      ...second,
      0,
    ]);
    const result = readMetadata(bytes, 0);
    assert(!isNeedMore(result));
    assertEquals([...result.meta.keys()], ["a", "b"]);
    assertEquals(result.next, bytes.length);
  });

  it("returns values that are copies of the input", () => {
    const bytes = Uint8Array.from([
      ...varint(1),
      ...avroString("k"),
      ...avroString("value"),
      0,
    ]);
    const result = readMetadata(bytes, 0);
    assert(!isNeedMore(result));
    bytes.fill(0);
    assertEquals(result.meta.get("k"), encoder.encode("value"));
  });

  it("lets a later entry replace an earlier one with the same key", () => {
    const bytes = Uint8Array.from([
      ...varint(2),
      ...avroString("k"),
      ...avroString("old"),
      ...avroString("k"),
      ...avroString("new"),
      0,
    ]);
    const result = readMetadata(bytes, 0);
    assert(!isNeedMore(result));
    assertEquals(result.meta.get("k"), encoder.encode("new"));
  });

  it("asks for more at every truncation point", () => {
    const bytes = Uint8Array.from([
      ...varint(1),
      ...avroString("k"),
      ...avroString("v"),
      0,
    ]);
    for (let end = 0; end < bytes.length; end++) {
      assert(isNeedMore(readMetadata(bytes.subarray(0, end), 0)));
    }
  });
});

describe("readSyncMarker", () => {
  it("reads a copy of the marker and the header length", () => {
    const bytes = Uint8Array.from([0xff, ...SYNC]);
    const result = readSyncMarker(bytes, 1);
    assert(!isNeedMore(result));
    assertEquals(result, { sync: SYNC, next: 17 });
    bytes.fill(0);
    assertEquals(result.sync, SYNC);
  });

  it("asks for the marker's end when it is cut off", () => {
    const result = readSyncMarker(
      Uint8Array.from([0xff, ...SYNC.subarray(0, 5)]),
      1,
    );
    assert(isNeedMore(result));
    assertEquals(result.minBytes, 17);
  });
});

describe("parseSchema", () => {
  it("parses the schema JSON", () => {
    const meta = new Map([["avro.schema", encoder.encode('{"type":"int"}')]]);
    assertEquals(parseSchema(meta), { type: "int" });
  });

  it("rejects a missing schema", () => {
    assertThrows(
      () => parseSchema(new Map()),
      InvalidHeaderError,
      "AVRO schema not found in metadata",
    );
  });

  it("rejects invalid JSON and keeps the SyntaxError as the cause", () => {
    const meta = new Map([["avro.schema", encoder.encode("{")]]);
    const error = assertThrows(() => parseSchema(meta), InvalidHeaderError);
    assertInstanceOf(error.cause, SyntaxError);
  });
});
