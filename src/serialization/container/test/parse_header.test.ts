import {
  assert,
  assertEquals,
  assertFalse,
  assertInstanceOf,
  assertThrows,
} from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { HEADER_TYPE } from "../../avro_constants.ts";
import { SyncReadableTap } from "../../tap_sync.ts";
import { InvalidHeaderError, InvalidMagicError } from "../errors.ts";
import { isNeedMore } from "../need_more.ts";
import {
  type ContainerHeader,
  DEFAULT_MAX_HEADER_BYTES,
  parseHeader,
} from "../parse_header.ts";
import {
  avroString,
  FIXTURE_FILES,
  headerBytes,
  SYNC,
  varint,
  writeContainer,
} from "./container_fixtures.ts";

const SCHEMA_JSON = '"long"';
const MAGIC_PREFIX = [0x4f, 0x62, 0x6a, 0x01];

function parsed(bytes: Uint8Array): ContainerHeader {
  const result = parseHeader(bytes);
  if (isNeedMore(result)) {
    throw new Error(`unexpected NeedMore(${result.minBytes})`);
  }
  return result;
}

describe("parseHeader", () => {
  it("agrees with HEADER_TYPE on files written by the library", () => {
    const bytes = writeContainer(3, { metadata: { "user.key": "value" } });
    const header = parsed(bytes);

    const tap = new SyncReadableTap(bytes.slice().buffer);
    const expected = HEADER_TYPE.readSync(tap) as {
      meta: Map<string, Uint8Array>;
      sync: Uint8Array;
    };
    assertEquals(header.meta, expected.meta);
    assertEquals(header.sync, expected.sync);
    assertEquals(header.length, tap.getPos());
    assertEquals(header.codec, "null");
    assertEquals(
      header.schema,
      JSON.parse(
        new TextDecoder().decode(expected.meta.get("avro.schema")),
      ),
    );
  });

  for (const path of FIXTURE_FILES) {
    it(`agrees with HEADER_TYPE on ${path}`, async () => {
      const bytes = await Deno.readFile(path);
      const header = parsed(bytes);

      const tap = new SyncReadableTap(bytes.slice().buffer);
      const expected = HEADER_TYPE.readSync(tap) as {
        meta: Map<string, Uint8Array>;
        sync: Uint8Array;
      };
      assertEquals(header.meta, expected.meta);
      assertEquals(header.sync, expected.sync);
      assertEquals(header.length, tap.getPos());
    });
  }

  it("reads the codec name, defaulting to null", async () => {
    assertEquals(
      parsed(await Deno.readFile("test-data/weather-deflate.avro")).codec,
      "deflate",
    );
    assertEquals(
      parsed(await Deno.readFile("test-data/weather-zstd.avro")).codec,
      "zstandard",
    );
    assertEquals(
      parsed(headerBytes([["avro.schema", SCHEMA_JSON]])).codec,
      "null",
    );
    assertEquals(
      parsed(headerBytes([["avro.schema", SCHEMA_JSON], ["avro.codec", ""]]))
        .codec,
      "null",
    );
  });

  it("returns NeedMore for every truncated prefix", async () => {
    const files = [
      writeContainer(1),
      await Deno.readFile("test-data/syncInMeta.avro"),
    ];
    for (const bytes of files) {
      const { length } = parsed(bytes);
      for (let end = 0; end < length; end++) {
        const result = parseHeader(bytes.subarray(0, end));
        assert(isNeedMore(result), `prefix of ${end} bytes`);
        if (result.minBytes === undefined) {
          assertFalse("minBytes" in result);
          continue;
        }
        assert(result.minBytes > end, `minBytes grows past ${end}`);
        assert(result.minBytes <= length, `minBytes is a lower bound`);
      }
      assertEquals(parsed(bytes.subarray(0, length)).length, length);
    }
  });

  it("reaches the header with a reader that doubles its buffer", () => {
    // How callers should retry: grow geometrically, and jump straight to
    // minBytes when it asks for more than the next doubling.
    const bytes = writeContainer(1, {
      metadata: { "user.big": "x".repeat(5000) },
    });
    const { length } = parsed(bytes);
    let end = 16;
    let calls = 1;
    let result = parseHeader(bytes.subarray(0, end));
    while (isNeedMore(result)) {
      end = Math.min(bytes.length, Math.max(result.minBytes ?? 0, end * 2));
      calls++;
      result = parseHeader(bytes.subarray(0, end));
    }
    assertEquals(result.length, length);
    assert(calls <= 5, `took ${calls} calls for a ${length}-byte header`);
  });

  it("jumps minBytes past a known-length value", () => {
    const bytes = writeContainer(1, {
      metadata: { "user.big": "x".repeat(5000) },
    });
    const valueStart = bytes.indexOf(0x78); // first "x"
    const result = parseHeader(bytes.subarray(0, valueStart + 1));
    assert(isNeedMore(result));
    assertEquals(result.minBytes, valueStart + 5000);
  });

  it("reads a map block with a negative count and byte size", () => {
    const entries = [
      ...avroString("avro.schema"),
      ...avroString(SCHEMA_JSON),
      ...avroString("k"),
      ...avroString("v"),
    ];
    const bytes = Uint8Array.from([
      0x4f,
      0x62,
      0x6a,
      0x01,
      ...varint(-2),
      ...varint(entries.length),
      ...entries,
      0,
      ...SYNC,
    ]);
    const header = parsed(bytes);
    assertEquals(header.length, bytes.length);
    assertEquals(new TextDecoder().decode(header.meta.get("k")), "v");
    assertEquals(header.schema, "long");

    for (let end = 0; end < bytes.length; end++) {
      assert(isNeedMore(parseHeader(bytes.subarray(0, end))));
    }
  });

  it("reads a header split over several map blocks", () => {
    const bytes = Uint8Array.from([
      0x4f,
      0x62,
      0x6a,
      0x01,
      ...varint(1),
      ...avroString("avro.schema"),
      ...avroString(SCHEMA_JSON),
      ...varint(1),
      ...avroString("k"),
      ...avroString("v"),
      0,
      ...SYNC,
    ]);
    const header = parsed(bytes);
    assertEquals([...header.meta.keys()], ["avro.schema", "k"]);
  });

  it("returns copies that do not alias the input", () => {
    const bytes = headerBytes([["avro.schema", SCHEMA_JSON], ["k", "v"]]);
    const header = parsed(bytes);
    bytes.fill(0);
    assertEquals(header.sync, SYNC);
    assertEquals(new TextDecoder().decode(header.meta.get("k")), "v");
  });

  it("rejects bad magic as soon as a byte differs", () => {
    assertThrows(
      () => parseHeader(Uint8Array.of(0x4f, 0x78)),
      InvalidMagicError,
      "Invalid AVRO file: incorrect magic bytes",
    );
    const bytes = headerBytes([["avro.schema", SCHEMA_JSON]]);
    bytes[3] = 0x02;
    assertThrows(() => parseHeader(bytes), InvalidMagicError);
  });

  it("asks for the magic when given fewer than four bytes", () => {
    for (const bytes of [new Uint8Array(), Uint8Array.of(0x4f, 0x62)]) {
      const result = parseHeader(bytes);
      assert(isNeedMore(result));
      assertEquals(result.minBytes, 4);
    }
  });

  it("rejects a header without a schema", () => {
    assertThrows(
      () => parseHeader(headerBytes([["k", "v"]])),
      InvalidHeaderError,
      "AVRO schema not found in metadata",
    );
    assertThrows(() => parseHeader(headerBytes([])), InvalidHeaderError);
  });

  it("rejects a schema that is not JSON", () => {
    const error = assertThrows(
      () => parseHeader(headerBytes([["avro.schema", "{not json"]])),
      InvalidHeaderError,
      "avro.schema is not valid JSON",
    );
    assertInstanceOf(error.cause, SyntaxError);
  });

  it("rejects negative metadata lengths", () => {
    const magic = [0x4f, 0x62, 0x6a, 0x01];
    assertThrows(
      () =>
        parseHeader(Uint8Array.from([...magic, ...varint(1), ...varint(-1)])),
      InvalidHeaderError,
      "negative length for metadata key",
    );
    assertThrows(
      () =>
        parseHeader(
          Uint8Array.from([
            ...magic,
            ...varint(1),
            ...avroString("k"),
            ...varint(-3),
          ]),
        ),
      InvalidHeaderError,
      'negative length for metadata value "k"',
    );
  });

  it("rejects an overlong varint", () => {
    assertThrows(
      () =>
        parseHeader(
          Uint8Array.from([
            0x4f,
            0x62,
            0x6a,
            0x01,
            ...new Array(10).fill(0x80),
          ]),
        ),
      InvalidHeaderError,
      "Invalid AVRO file header: varint is longer than 10 bytes",
    );
  });

  describe("maxHeaderBytes", () => {
    const magic = [0x4f, 0x62, 0x6a, 0x01];

    it("rejects a huge metadata length without asking for the bytes", () => {
      const bytes = Uint8Array.from([
        ...magic,
        ...varint(1),
        ...avroString("k"),
        ...varint(2 ** 50),
      ]);
      assertThrows(
        () => parseHeader(bytes),
        InvalidHeaderError,
        `more than maxHeaderBytes (${DEFAULT_MAX_HEADER_BYTES})`,
      );
    });

    it("rejects a length whose end is not a safe integer", () => {
      // The end would be MAX_SAFE_INTEGER plus the prefix length: past the
      // safe range, where it must still fail as a header error.
      for (const field of [[], [...avroString("k")]]) {
        const bytes = Uint8Array.from([
          ...magic,
          ...varint(1),
          ...field,
          ...varint(Number.MAX_SAFE_INTEGER),
        ]);
        assertThrows(
          () => parseHeader(bytes),
          InvalidHeaderError,
          `length ${Number.MAX_SAFE_INTEGER} for metadata`,
        );
      }
    });

    it("rejects a huge key length", () => {
      const bytes = Uint8Array.from([...magic, ...varint(1), ...varint(100)]);
      assertThrows(
        () => parseHeader(bytes, { maxHeaderBytes: 50 }),
        InvalidHeaderError,
        "header needs at least 107 bytes, more than maxHeaderBytes (50)",
      );
    });

    it("accepts a header of exactly the limit and rejects one byte less", () => {
      const bytes = writeContainer(1);
      const { length } = parsed(bytes);
      assertEquals(
        parsed(bytes.subarray(0, length)).length,
        (parseHeader(bytes, { maxHeaderBytes: length }) as ContainerHeader)
          .length,
      );
      assertThrows(
        () => parseHeader(bytes, { maxHeaderBytes: length - 1 }),
        InvalidHeaderError,
        `header needs at least ${length} bytes`,
      );
    });

    it("throws for exactly the prefixes whose request exceeds the limit", () => {
      const bytes = writeContainer(1);
      const { length } = parsed(bytes);
      const limit = Math.floor(length / 2);
      let sawUnknown = false;
      let sawThrow = false;
      for (let end = 0; end < length; end++) {
        const prefix = bytes.subarray(0, end);
        // Without a limit, every prefix is a request for more bytes.
        const unlimited = parseHeader(prefix);
        assert(isNeedMore(unlimited));
        const needed = unlimited.minBytes ?? end + 1;
        sawUnknown ||= unlimited.minBytes === undefined && needed <= limit;
        if (needed > limit) {
          sawThrow = true;
          assertThrows(
            () => parseHeader(prefix, { maxHeaderBytes: limit }),
            InvalidHeaderError,
            `more than maxHeaderBytes (${limit})`,
          );
        } else {
          assertEquals(
            parseHeader(prefix, { maxHeaderBytes: limit }),
            unlimited,
            `prefix of ${end} bytes`,
          );
        }
      }
      assert(sawUnknown && sawThrow, "covers both outcomes");
      assertThrows(
        () =>
          parseHeader(
            Uint8Array.from([...magic, ...varint(1), 0x80, 0x80]),
            { maxHeaderBytes: 7 },
          ),
        InvalidHeaderError,
        "header needs at least 8 bytes, more than maxHeaderBytes (7)",
      );
    });

    it("parses a header that fits the limit from a much longer buffer", () => {
      const bytes = writeContainer(500, { blockSize: 256 });
      const { length } = parsed(bytes);
      assert(bytes.length > 4 * length, "fixture has data after the header");
      const header = parseHeader(bytes, { maxHeaderBytes: length });
      assert(!isNeedMore(header));
      assertEquals(header.length, length);
    });

    it("rejects an oversized header without scanning past the limit", () => {
      // A whole file whose metadata is far longer than the limit. Only the
      // first maxHeaderBytes bytes are ever looked at.
      const bytes = writeContainer(1, {
        metadata: { "user.big": "x".repeat(50_000) },
      });
      assertThrows(
        () => parseHeader(bytes, { maxHeaderBytes: 1024 }),
        InvalidHeaderError,
        "more than maxHeaderBytes (1024)",
      );
    });

    it("rejects a map of many empty entries past the limit", () => {
      // Each empty entry is 2 bytes, so without the cap a whole-file input
      // would be scanned entry by entry far beyond the limit.
      const entries = 200_000;
      const bytes = new Uint8Array(4 + 4 + entries * 2 + 1 + 16);
      bytes.set(MAGIC_PREFIX);
      bytes.set(varint(entries), 4); // 3 bytes for 200_000
      // Entries stay zero-filled: key length 0, value length 0.
      assertThrows(
        () => parseHeader(bytes, { maxHeaderBytes: 64 }),
        InvalidHeaderError,
        "more than maxHeaderBytes (64)",
      );
    });

    it("must be a positive safe integer", () => {
      for (const maxHeaderBytes of [0, -1, 1.5, NaN, 2 ** 53]) {
        assertThrows(
          () => parseHeader(writeContainer(1), { maxHeaderBytes }),
          RangeError,
          `maxHeaderBytes must be a positive safe integer, got ${maxHeaderBytes}`,
        );
      }
    });
  });
});
