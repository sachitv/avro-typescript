import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { SyncAvroFileParser } from "../../avro_file_parser_sync.ts";
import { SyncInMemoryReadableBuffer } from "../../buffers/in_memory_buffer_sync.ts";
import { InvalidBlockError } from "../errors.ts";
import { isNeedMore } from "../need_more.ts";
import { type BlockHead, parseBlockHead } from "../parse_block_head.ts";
import { parseHeader } from "../parse_header.ts";
import { matchesSync, SYNC_SIZE } from "../internal/sync_marker.ts";
import { FIXTURE_FILES, varint, writeContainer } from "./container_fixtures.ts";

function head(bytes: Uint8Array, fileOffset = 0): BlockHead {
  const result = parseBlockHead(bytes, fileOffset);
  if (isNeedMore(result)) throw new Error("unexpected NeedMore");
  return result;
}

/** Walks every block of a container, checking each one's sync marker. */
function walkBlocks(bytes: Uint8Array): { blocks: number; records: number } {
  const header = parseHeader(bytes);
  if (isNeedMore(header)) throw new Error("truncated header");
  let pos = header.length;
  let blocks = 0;
  let records = 0;
  while (pos < bytes.length) {
    const block = head(bytes.subarray(pos), pos);
    const syncAt = pos + block.headLength + block.byteLength;
    assert(matchesSync(bytes, syncAt, header.sync), `sync after ${pos}`);
    pos = syncAt + SYNC_SIZE;
    blocks++;
    records += block.count;
  }
  assertEquals(pos, bytes.length);
  return { blocks, records };
}

describe("parseBlockHead", () => {
  it("reads the count, data length, and prefix length", () => {
    const bytes = Uint8Array.from([
      ...varint(300),
      ...varint(70_000),
      0xff,
    ]);
    assertEquals(head(bytes), {
      count: 300,
      byteLength: 70_000,
      headLength: bytes.length - 1,
    });
  });

  it("accepts an empty block", () => {
    assertEquals(head(Uint8Array.of(0, 0)), {
      count: 0,
      byteLength: 0,
      headLength: 2,
    });
  });

  it("walks every block of a multi-block file", () => {
    const bytes = writeContainer(500, { blockSize: 256 });
    const { blocks, records } = walkBlocks(bytes);
    assert(blocks > 1, "fixture spans several blocks");
    assertEquals(records, 500);
  });

  for (const path of FIXTURE_FILES) {
    it(`walks every block of ${path}`, async () => {
      const bytes = await Deno.readFile(path);
      const { records } = walkBlocks(bytes);
      const header = parseHeader(bytes);
      // The sync parser only decodes uncompressed files.
      if (isNeedMore(header) || header.codec !== "null") return;
      const parser = new SyncAvroFileParser(
        new SyncInMemoryReadableBuffer(bytes.slice().buffer),
      );
      assertEquals(records, [...parser.iterRecords()].length);
    });
  }

  it("returns NeedMore without minBytes for every truncated prefix", () => {
    // Multi-byte varints, one of them long enough to take the BigInt path.
    const bytes = Uint8Array.from([...varint(2 ** 40), ...varint(2 ** 50)]);
    for (let end = 0; end < bytes.length; end++) {
      const result = parseBlockHead(bytes.subarray(0, end), 0);
      assert(isNeedMore(result), `prefix of ${end} bytes`);
      assertEquals(result, { needMore: true });
    }
    assertEquals(head(bytes).byteLength, 2 ** 50);
  });

  it("rejects negative counts and lengths with the file offset", () => {
    const negativeCount = assertThrows(
      () => parseBlockHead(Uint8Array.from([...varint(-1), 0]), 4096),
      InvalidBlockError,
      "Invalid AVRO file: block at offset 4096: negative record count -1",
    );
    assertEquals(negativeCount.blockOffset, 4096);
    assertThrows(
      () => parseBlockHead(Uint8Array.from([...varint(1), ...varint(-5)]), 0),
      InvalidBlockError,
      "negative byte length -5",
    );
  });

  it("reports the file offset of a block parsed from a slice", () => {
    const file = Uint8Array.from([
      ...new Array(100).fill(0),
      ...varint(1),
      ...varint(-5),
    ]);
    const error = assertThrows(
      () => parseBlockHead(file.subarray(100), 100),
      InvalidBlockError,
    );
    assertEquals(error.blockOffset, 100);
  });

  it("rejects values outside the safe integer range", () => {
    assertThrows(
      () => parseBlockHead(Uint8Array.from(varint(2n ** 53n)), 0),
      InvalidBlockError,
      `value ${2n ** 53n} is outside the safe integer range`,
    );
    assertThrows(
      () => parseBlockHead(Uint8Array.from(new Array(11).fill(0xff)), 0),
      InvalidBlockError,
      "varint is longer than 10 bytes",
    );
  });
});
