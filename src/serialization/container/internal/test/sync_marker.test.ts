import { assert, assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { isNeedMore } from "../../need_more.ts";
import { parseBlockHead } from "../../parse_block_head.ts";
import { parseHeader } from "../../parse_header.ts";
import {
  findNextSync,
  findPrevSync,
  matchesSync,
  SYNC_SIZE,
} from "../sync_marker.ts";
import { SYNC, writeContainer } from "../../test/container_fixtures.ts";

/** `before` bytes of filler, the marker, then `after` bytes of filler. */
function withSyncAt(before: number, after = 0): Uint8Array {
  // The filler repeats the marker's first byte to exercise false starts.
  return Uint8Array.from([
    ...new Array(before).fill(SYNC[0]),
    ...SYNC,
    ...new Array(after).fill(SYNC[0]),
  ]);
}

describe("matchesSync", () => {
  it("matches the marker at the given offset only", () => {
    const bytes = withSyncAt(3, 2);
    assert(matchesSync(bytes, 3, SYNC));
    assert(!matchesSync(bytes, 2, SYNC));
    assert(!matchesSync(bytes, 4, SYNC));
  });

  it("is false when the marker would fall outside the bytes", () => {
    const bytes = withSyncAt(0);
    assert(!matchesSync(bytes, -1, SYNC));
    assert(!matchesSync(bytes.subarray(0, SYNC_SIZE - 1), 0, SYNC));
  });
});

describe("findNextSync", () => {
  it("finds the marker at the start, middle, and end", () => {
    assertEquals(findNextSync(withSyncAt(0, 5), SYNC), 0);
    assertEquals(findNextSync(withSyncAt(7, 5), SYNC), 7);
    assertEquals(findNextSync(withSyncAt(7), SYNC), 7);
  });

  it("starts at from and treats a negative from as zero", () => {
    const bytes = Uint8Array.from([...SYNC, ...SYNC]);
    assertEquals(findNextSync(bytes, SYNC, 1), SYNC_SIZE);
    assertEquals(findNextSync(bytes, SYNC, -5), 0);
  });

  it("returns -1 when there is no complete marker", () => {
    assertEquals(findNextSync(new Uint8Array(40), SYNC), -1);
    assertEquals(findNextSync(withSyncAt(4).subarray(0, 19), SYNC), -1);
    assertEquals(findNextSync(withSyncAt(4, 4), SYNC, 5), -1);
  });

  it("lands on block boundaries in a multi-block file", () => {
    const bytes = writeContainer(500, { blockSize: 256 });
    const header = parseHeader(bytes);
    assert(!isNeedMore(header));
    const middle = Math.floor(bytes.length / 2);
    const found = findNextSync(bytes, header.sync, middle);
    assert(found >= middle);
    const blockStart = found + SYNC_SIZE;
    const block = parseBlockHead(bytes.subarray(blockStart), blockStart);
    assert(!isNeedMore(block));
    const nextSync = blockStart + block.headLength + block.byteLength;
    assert(matchesSync(bytes, nextSync, header.sync));
  });

  it("can match marker bytes stored in metadata", async () => {
    // syncInMeta.avro stores its sync marker as a metadata value, so the
    // first match is inside the header rather than at its end.
    const bytes = await Deno.readFile("test-data/syncInMeta.avro");
    const header = parseHeader(bytes);
    assert(!isNeedMore(header));
    const first = findNextSync(bytes, header.sync);
    assert(first < header.length - SYNC_SIZE);
    assertEquals(
      findNextSync(bytes, header.sync, first + 1),
      header.length - SYNC_SIZE,
    );
  });
});

describe("findPrevSync", () => {
  it("finds the marker at the end, middle, and start", () => {
    assertEquals(findPrevSync(withSyncAt(5), SYNC), 5);
    assertEquals(findPrevSync(withSyncAt(5, 7), SYNC), 5);
    assertEquals(findPrevSync(withSyncAt(0, 7), SYNC), 0);
  });

  it("only returns markers that end at or before before", () => {
    const bytes = Uint8Array.from([...SYNC, ...SYNC]);
    assertEquals(findPrevSync(bytes, SYNC, 2 * SYNC_SIZE), SYNC_SIZE);
    assertEquals(findPrevSync(bytes, SYNC, 2 * SYNC_SIZE - 1), 0);
    assertEquals(findPrevSync(bytes, SYNC, 100), SYNC_SIZE);
  });

  it("returns -1 when there is no complete marker", () => {
    assertEquals(findPrevSync(new Uint8Array(40), SYNC), -1);
    assertEquals(findPrevSync(withSyncAt(0), SYNC, SYNC_SIZE - 1), -1);
    assertEquals(findPrevSync(withSyncAt(4, 4), SYNC, 19), -1);
    assertEquals(findPrevSync(withSyncAt(0).subarray(1), SYNC), -1);
  });

  it("steps back to earlier candidates with before = match + SYNC_SIZE - 1", () => {
    // Three markers; walking back visits each once, then runs out.
    const bytes = Uint8Array.from([...SYNC, 0, ...SYNC, 0, ...SYNC]);
    const seen: number[] = [];
    let match = findPrevSync(bytes, SYNC);
    while (match !== -1) {
      seen.push(match);
      match = findPrevSync(bytes, SYNC, match + SYNC_SIZE - 1);
    }
    assertEquals(seen, [34, 17, 0]);
  });

  it("finds the last block's start from only the file's tail", () => {
    const bytes = writeContainer(500, { blockSize: 256 });
    const header = parseHeader(bytes);
    assert(!isNeedMore(header));
    const tailOffset = bytes.length - 300;
    const tail = bytes.subarray(tailOffset);
    const match = findPrevSync(tail, header.sync, tail.length - SYNC_SIZE);
    assert(match !== -1, "the tail holds the previous block's marker");
    const lastBlock = tailOffset + match + SYNC_SIZE;
    const block = parseBlockHead(bytes.subarray(lastBlock), lastBlock);
    assert(!isNeedMore(block));
    assertEquals(
      lastBlock + block.headLength + block.byteLength,
      bytes.length - SYNC_SIZE,
    );
  });

  it("finds the last block's start in a multi-block file", () => {
    const bytes = writeContainer(500, { blockSize: 256 });
    const header = parseHeader(bytes);
    assert(!isNeedMore(header));
    const finalSync = bytes.length - SYNC_SIZE;
    const lastBlock = findPrevSync(bytes, header.sync, finalSync) + SYNC_SIZE;
    assert(lastBlock > header.length);
    const block = parseBlockHead(bytes.subarray(lastBlock), lastBlock);
    assert(!isNeedMore(block));
    assertEquals(lastBlock + block.headLength + block.byteLength, finalSync);
  });

  it("finds the header's marker in a single-block file", () => {
    const bytes = writeContainer(3);
    const header = parseHeader(bytes);
    assert(!isNeedMore(header));
    const finalSync = bytes.length - SYNC_SIZE;
    assertEquals(
      findPrevSync(bytes, header.sync, finalSync),
      header.length - SYNC_SIZE,
    );
  });
});
