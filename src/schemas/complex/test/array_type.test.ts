import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import {
  SyncReadableTap,
  type SyncReadableTapLike,
  SyncWritableTap,
} from "../../../serialization/sync_tap.ts";
import { ArrayType, readArrayInto, readArrayIntoSync } from "../array_type.ts";
import { IntType } from "../../primitive/int_type.ts";
import { LongType } from "../../primitive/long_type.ts";
import { StringType } from "../../primitive/string_type.ts";
import { BytesType } from "../../primitive/bytes_type.ts";
import type { Type } from "../../type.ts";

function createArray<T>(items: Type<T>): ArrayType<T> {
  return new ArrayType({ items });
}

describe("ArrayType", () => {
  const intItems = new IntType();
  const intArray = createArray(intItems);

  describe("Validation", () => {
    it("validates valid arrays", () => {
      assert(intArray.check([1, 2, 3]));
    });

    it("invalidates non-array values", () => {
      assert(!intArray.check("hello"));
    });

    it("reports element paths in error hook", () => {
      const paths: string[][] = [];
      intArray.check([1, "bad", 3] as unknown[], (path, value, type) => {
        assertEquals(type, intItems);
        assertEquals(value, "bad");
        paths.push(path);
      });
      assertEquals(paths, [["1"]]);
    });

    it("calls error hook when checking non-array value", () => {
      const paths: string[][] = [];
      intArray.check("not an array", (path, value, type) => {
        assertEquals(type, intArray);
        assertEquals(value, "not an array");
        paths.push(path);
      });
      assertEquals(paths, [[]]);
    });

    it("returns false early when element check fails and no error hook", () => {
      assert(!intArray.check([1, "invalid" as unknown]));
    });
  });

  describe("Serialization", () => {
    it("serializes and deserializes using blocks", async () => {
      const buffer = new Uint8Array([1, 2, 2, 0]).buffer;
      const values = await intArray.fromBuffer(buffer);
      assertEquals(values, [1]);
    });

    it("round-trips via toBuffer/fromBuffer", async () => {
      const values = [4, 5, 6];
      const buffer = await intArray.toBuffer(values);
      assertEquals(await intArray.fromBuffer(buffer), values);
    });

    it("round-trips empty array via toBuffer/fromBuffer", async () => {
      const values: number[] = [];
      const buffer = await intArray.toBuffer(values);
      assertEquals(await intArray.fromBuffer(buffer), values);
    });

    it("writes empty array correctly", async () => {
      const buffer = await intArray.toBuffer([]);
      const tap = new Tap(buffer);
      assertEquals(await tap.readLong(), 0n);
    });

    it("round-trips via sync buffer", () => {
      const values = [4, 5, 6];
      const buffer = intArray.toSyncBuffer(values);
      assertEquals(intArray.fromSyncBuffer(buffer), values);
    });

    it("handles empty arrays via sync buffer", () => {
      const buffer = intArray.toSyncBuffer([]);
      assertEquals(intArray.fromSyncBuffer(buffer), []);
    });

    it("writes arrays correctly via write method", async () => {
      const buffer = new ArrayBuffer(20);
      const writeTap = new Tap(buffer);
      await intArray.write(writeTap, [10, 20]);

      const readTap = new Tap(buffer);
      const result = await intArray.read(readTap);
      assertEquals(result, [10, 20]);
    });

    it("reads and writes via sync taps", () => {
      const buffer = new ArrayBuffer(64);
      const writeTap = new SyncWritableTap(buffer);
      intArray.writeSync(writeTap, [10, 20]);
      const readTap = new SyncReadableTap(buffer);
      assertEquals(readTap.getPos(), 0);
      assertEquals(intArray.readSync(readTap), [10, 20]);
      assertEquals(readTap.getPos(), writeTap.getPos());
    });
  });

  describe("Skipping", () => {
    it("skips encoded array blocks", async () => {
      const buffer = await intArray.toBuffer([7, 8]);
      const tap = new Tap(buffer);
      await intArray.skip(tap);
      assertEquals(tap.getPos(), buffer.byteLength);
    });

    it("skips encoded array blocks via sync taps", () => {
      const buffer = intArray.toSyncBuffer([7, 8]);
      const tap = new SyncReadableTap(buffer);
      intArray.skipSync(tap);
      assertEquals(tap.getPos(), buffer.byteLength);
    });

    it("skips size-prefixed blocks via sync taps", async () => {
      const tempBuffer = new ArrayBuffer(16);
      const tempTap = new Tap(tempBuffer);
      await tempTap.writeLong(100n);
      await tempTap.writeLong(200n);
      const blockSize = tempTap.getPos();

      const buffer = new ArrayBuffer(32);
      const writeTap = new Tap(buffer);
      await writeTap.writeLong(-2n);
      await writeTap.writeLong(BigInt(blockSize));
      await writeTap.writeLong(100n);
      await writeTap.writeLong(200n);
      await writeTap.writeLong(0n);

      const encoded = buffer.slice(0, writeTap.getPos());
      const tap = new SyncReadableTap(encoded);
      intArray.skipSync(tap);
      assertEquals(tap.getPos(), encoded.byteLength);
    });

    it("skips size-prefixed array blocks", async () => {
      // Calculate block size
      const tempBuffer = new ArrayBuffer(10);
      const tempTap = new Tap(tempBuffer);
      await tempTap.writeLong(100n);
      await tempTap.writeLong(200n);
      const blockSize = tempTap.getPos();

      // Create a buffer with negative count (size-prefixed)
      const buffer = new ArrayBuffer(7); // -2n(1) + blockSize(1) + block(4) + 0n(1)
      const writeTap = new Tap(buffer);
      await writeTap.writeLong(-2n); // negative count
      await writeTap.writeLong(BigInt(blockSize)); // block size
      await writeTap.writeLong(100n);
      await writeTap.writeLong(200n);
      await writeTap.writeLong(0n); // terminator

      const readTap = new Tap(buffer);
      await intArray.skip(readTap);
      assertEquals(readTap.getPos(), buffer.byteLength);
    });
  });

  describe("Matching", () => {
    it("matches encoded arrays via sync taps", () => {
      const buf1 = intArray.toSyncBuffer([1, 2]);
      const buf2 = intArray.toSyncBuffer([1, 3]);
      assertEquals(
        intArray.matchSync(
          new SyncReadableTap(buf1),
          new SyncReadableTap(buf2),
        ),
        -1,
      );
      assertEquals(
        intArray.matchSync(
          new SyncReadableTap(buf2),
          new SyncReadableTap(buf1),
        ),
        1,
      );
      assertEquals(
        intArray.matchSync(
          new SyncReadableTap(buf1),
          new SyncReadableTap(intArray.toSyncBuffer([1, 2])),
        ),
        0,
      );
    });

    it("handles size-prefixed blocks inside matchSync loop", async () => {
      const items = [123, 456];

      const elementBuffer = await intItems.toBuffer(items[1]);
      const elementSize = elementBuffer.byteLength;

      const multiBlockBuf = new ArrayBuffer(30);
      const tap = new Tap(multiBlockBuf);

      await tap.writeLong(1n);
      await intItems.write(tap, items[0]);
      await tap.writeLong(-1n);
      await tap.writeLong(BigInt(elementSize));
      await intItems.write(tap, items[1]);
      await tap.writeLong(0n);

      const buf1 = multiBlockBuf.slice(0, tap.getPos());
      const buf2 = intArray.toSyncBuffer(items);

      assertEquals(
        intArray.matchSync(
          new SyncReadableTap(buf1),
          new SyncReadableTap(buf2),
        ),
        0,
      );
      assertEquals(
        intArray.matchSync(
          new SyncReadableTap(buf2),
          new SyncReadableTap(buf1),
        ),
        0,
      );
    });

    it("returns 0 when matching empty arrays via sync taps", () => {
      const emptyBuffer = intArray.toSyncBuffer([]);
      assertEquals(
        intArray.matchSync(
          new SyncReadableTap(emptyBuffer),
          new SyncReadableTap(emptyBuffer),
        ),
        0,
      );
    });

    it("tests matchSync conditions where n1 < n2 and n1 > n2", () => {
      // Test case where first array is shorter (n1 < n2)
      const shortArray = [1, 2];
      const longArray = [1, 2, 3, 4];

      const bufShort = intArray.toSyncBuffer(shortArray);
      const bufLong = intArray.toSyncBuffer(longArray);

      // When comparing short vs long, should return -1 (n1 < n2)
      assertEquals(
        intArray.matchSync(
          new SyncReadableTap(bufShort),
          new SyncReadableTap(bufLong),
        ),
        -1,
      );

      // When comparing long vs short, should return 1 (n1 > n2)
      assertEquals(
        intArray.matchSync(
          new SyncReadableTap(bufLong),
          new SyncReadableTap(bufShort),
        ),
        1,
      );

      // Test with empty array vs non-empty array
      const emptyBuffer = intArray.toSyncBuffer([]);
      const nonEmptyBuffer = intArray.toSyncBuffer([1, 2, 3]);

      // Empty vs non-empty should return -1 (n1 < n2)
      assertEquals(
        intArray.matchSync(
          new SyncReadableTap(emptyBuffer),
          new SyncReadableTap(nonEmptyBuffer),
        ),
        -1,
      );

      // Non-empty vs empty should return 1 (n1 > n2)
      assertEquals(
        intArray.matchSync(
          new SyncReadableTap(nonEmptyBuffer),
          new SyncReadableTap(emptyBuffer),
        ),
        1,
      );
    });

    it("should match encoded array buffers", async () => {
      const arr1 = [1, 2];
      const arr2 = [1, 3];
      const arr3 = [1, 2, 4];

      const buf1 = await intArray.toBuffer(arr1);
      const buf2 = await intArray.toBuffer(arr2);
      const buf3 = await intArray.toBuffer(arr3);

      assertEquals(await intArray.match(new Tap(buf1), new Tap(buf2)), -1); // [1,2] < [1,3]
      assertEquals(await intArray.match(new Tap(buf2), new Tap(buf1)), 1); // [1,3] > [1,2]
      assertEquals(await intArray.match(new Tap(buf1), new Tap(buf3)), -1); // [1,2] < [1,2,4] (shorter first)
      assertEquals(await intArray.match(new Tap(buf3), new Tap(buf1)), 1); // [1,2,4] > [1,2]
      const buf1_copy = await intArray.toBuffer(arr1);
      assertEquals(await intArray.match(new Tap(buf1), new Tap(buf1_copy)), 0); // equal

      // Test size-prefixed block (negative count)
      const sizePrefixedBuf = new ArrayBuffer(20);
      const tap = new Tap(sizePrefixedBuf);
      await tap.writeLong(-2n); // negative count for size-prefixed
      await tap.writeLong(3n); // byte size of the block (1 byte for int 1 + 2 bytes for int 32)
      await tap.writeLong(1n);
      await tap.writeLong(32n); // int 32 requires 2 bytes in zigzag varint encoding
      await tap.writeLong(0n); // terminator

      const normalBuf = await intArray.toBuffer([1, 32]);
      assertEquals(
        await intArray.match(new Tap(sizePrefixedBuf), new Tap(normalBuf)),
        0,
      ); // should be equal
    });

    it("handles size-prefixed blocks inside match loop", async () => {
      const items = [123, 456];

      // Calculate size of the second element to build a size-prefixed block
      const elementBuffer = await intItems.toBuffer(items[1]);
      const elementSize = elementBuffer.byteLength;

      const multiBlockBuf = new ArrayBuffer(30);
      const tap = new Tap(multiBlockBuf);

      // First block: standard, one element
      await tap.writeLong(1n);
      await intItems.write(tap, items[0]);

      // Second block: size-prefixed, one element
      await tap.writeLong(-1n);
      await tap.writeLong(BigInt(elementSize));
      await intItems.write(tap, items[1]);

      // Terminator
      await tap.writeLong(0n);

      const buf1 = multiBlockBuf.slice(0, tap.getPos());
      const buf2 = await intArray.toBuffer(items);

      // This forces match() to call #readArraySize on a size-prefixed block
      // from inside its loop.
      assertEquals(await intArray.match(new Tap(buf1), new Tap(buf2)), 0);
      assertEquals(await intArray.match(new Tap(buf2), new Tap(buf1)), 0);
    });
  });

  describe("Error Handling", () => {
    it("throws when writeSync receives a non-array", () => {
      const buffer = new ArrayBuffer(16);
      const tap = new SyncWritableTap(buffer);
      assertThrows(
        () => intArray.writeSync(tap, "invalid" as unknown as number[]),
        Error,
        "Invalid value",
      );
    });

    it("throws error in write when value is not array", async () => {
      const buffer = new ArrayBuffer(10);
      const tap = new Tap(buffer);
      await assertRejects(
        () => intArray.write(tap, "not an array" as unknown as number[]),
        Error,
        "Invalid value",
      );
    });

    it("throws when toSyncBuffer receives invalid value", () => {
      assertThrows(
        () => intArray.toSyncBuffer("invalid" as unknown as number[]),
        Error,
      );
    });

    it("throws error in toBuffer when value is not array", async () => {
      await assertRejects(
        () => intArray.toBuffer("not an array" as unknown as number[]),
        Error,
        "Invalid value",
      );
    });

    it("throws error in clone when value is not array", () => {
      assertThrows(
        () => intArray.cloneFromValue("not an array" as unknown as number[]),
        Error,
        "Cannot clone non-array value.",
      );
    });

    it("throws error when constructor receives falsy items", () => {
      assertThrows(
        () =>
          new ArrayType({
            items: undefined as unknown as Type<unknown>,
          }),
        Error,
        "ArrayType requires an items type.",
      );
    });

    it("throws resolver error for incompatible writer arrays", () => {
      const stringArray = createArray(new StringType());
      const intArrayReader = createArray(new IntType());
      assertThrows(() => intArrayReader.createResolver(stringArray));
    });

    it("falls back to super createResolver for non-array writer types", () => {
      const stringType = new StringType();
      assertThrows(
        () => intArray.createResolver(stringType),
        Error,
        "Schema evolution not supported",
      );
    });
  });

  describe("Resolver", () => {
    it("creates resolver for compatible writer arrays", async () => {
      // Writer schema: array of strings
      const stringArray = createArray(new StringType());
      // Reader schema: array of byte arrays (compatible evolution)
      const bytesArray = createArray(new BytesType());
      // Create resolver to adapt from string array to byte array
      const resolver = bytesArray.createResolver(stringArray);
      // Write an array of 2 strings containing binary data
      const buffer = await stringArray.toBuffer(["\x01\x02", "\x03\x04"]);
      const tap = new Tap(buffer);
      // Read using resolver: strings evolve to Uint8Arrays
      const result = await resolver.read(tap) as Uint8Array[];
      // Result should be an array of 2 Uint8Arrays
      assertEquals(result.length, 2);
      assertEquals([...result[0]], [1, 2]); // First string "\x01\x02" -> bytes [1, 2]
      assertEquals([...result[1]], [3, 4]); // Second string "\x03\x04" -> bytes [3, 4]
    });

    it("reads resolver values synchronously", () => {
      const stringArray = createArray(new StringType());
      const bytesArray = createArray(new BytesType());
      const resolver = bytesArray.createResolver(stringArray);
      const buffer = stringArray.toSyncBuffer(["\x01\x02"]);
      const result = resolver.readSync(
        new SyncReadableTap(buffer),
      ) as Uint8Array[];
      assertEquals(result.length, 1);
      assertEquals([...result[0]], [1, 2]);
    });

    it("resolves array of array of ints to array of array of longs", async () => {
      // Writer schema: array of array of ints
      const intArray = createArray(new IntType());
      const writerSchema = createArray(intArray);

      // Reader schema: array of array of longs
      const longArray = createArray(new LongType());
      const readerSchema = createArray(longArray);

      // Create resolver
      const resolver = readerSchema.createResolver(writerSchema);

      // Test data: nested arrays of ints
      const testData = [
        [1, 2],
        [3, 4, 5],
      ];

      // Write with writer schema
      const buffer = await writerSchema.toBuffer(testData);
      const tap = new Tap(buffer);

      // Read with resolver (evolves ints to longs)
      const result = await resolver.read(tap) as bigint[][];

      // Expected: arrays of bigints
      const expected = [
        [1n, 2n],
        [3n, 4n, 5n],
      ];
      assertEquals(result, expected);
    });
  });

  describe("Cloning", () => {
    it("clones arrays deeply", () => {
      const original = [9, 10];
      const cloned = intArray.cloneFromValue(original);
      assertEquals(cloned, original);
      cloned[0] = 99;
      assertEquals(original[0], 9);
    });
  });

  describe("Comparison", () => {
    it("compares arrays lexicographically", () => {
      assertEquals(intArray.compare([], []), 0);
      assertEquals(intArray.compare([], [1]), -1);
      assertEquals(intArray.compare([1, 2], [1, 3]), -1);
      assertEquals(intArray.compare([2], [1, 3]), 1);
    });

    it("compares arrays by length when elements are equal", () => {
      assertEquals(intArray.compare([1, 2], [1, 2, 3]), -1);
      assertEquals(intArray.compare([1, 2, 3], [1, 2]), 1);
    });
  });

  describe("Random Generation", () => {
    it("generates random arrays", () => {
      const randomValue = intArray.random();
      assert(Array.isArray(randomValue));
    });
  });

  describe("JSON Serialization", () => {
    it("serializes to JSON correctly", () => {
      assertEquals(intArray.toJSON(), {
        type: "array",
        items: intItems.toJSON(),
      });
    });

    it("exposes items type", () => {
      assertEquals(intArray.getItemsType(), intItems);
    });
  });

  describe("Nested Arrays", () => {
    it("handles nested arrays of ints", async () => {
      // Create array of array of ints
      const intArrayOfInts = createArray(intArray);

      // Test data: array of arrays
      const nestedData = [
        [1, 2],
        [3, 4, 5],
        [6],
      ];

      // Round-trip via toBuffer/fromBuffer
      const buffer = await intArrayOfInts.toBuffer(nestedData);
      const result = await intArrayOfInts.fromBuffer(buffer);
      assertEquals(result, nestedData);

      // Test validation
      assert(intArrayOfInts.check(nestedData));
      assert(!intArrayOfInts.check("not an array"));
      assert(!intArrayOfInts.check([1, 2])); // inner elements must be arrays

      // Test cloning
      const cloned = intArrayOfInts.cloneFromValue(nestedData);
      assertEquals(cloned, nestedData);
      cloned[0][0] = 99;
      assertEquals(nestedData[0][0], 1); // deep clone
    });
  });
});

describe("readArrayInto", () => {
  it("reads positive block count", async () => {
    const buffer = new ArrayBuffer(20);
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(2n); // block count
    await writeTap.writeLong(10n); // element 1
    await writeTap.writeLong(20n); // element 2
    await writeTap.writeLong(0n); // terminator

    const readTap = new Tap(buffer);
    const results: bigint[] = [];
    await readArrayInto(
      readTap,
      (t) => t.readLong(),
      (value) => results.push(value),
    );
    assertEquals(results, [10n, 20n]);
  });

  it("reads negative block count (size-prefixed)", async () => {
    const buffer = new ArrayBuffer(30);
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(-2n); // negative block count
    await writeTap.writeLong(100n); // block size, ignored
    await writeTap.writeLong(30n);
    await writeTap.writeLong(40n);
    await writeTap.writeLong(0n);

    const readTap = new Tap(buffer);
    const results: bigint[] = [];
    await readArrayInto(
      readTap,
      (t) => t.readLong(),
      (value) => results.push(value),
    );
    assertEquals(results, [30n, 40n]);
  });

  it("stops at terminator", async () => {
    const buffer = new ArrayBuffer(25);
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(1n);
    await writeTap.writeLong(50n);
    await writeTap.writeLong(0n); // terminator
    await writeTap.writeLong(1n); // more, but should stop
    await writeTap.writeLong(60n);

    const readTap = new Tap(buffer);
    const results: bigint[] = [];
    await readArrayInto(
      readTap,
      (t) => t.readLong(),
      (value) => results.push(value),
    );
    assertEquals(results, [50n]);
  });

  it("throws on bigint outside safe integer range", async () => {
    const buffer = new ArrayBuffer(15);
    const writeTap = new Tap(buffer);
    // Write a large bigint > MAX_SAFE_INTEGER
    const largeBigInt = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    await writeTap.writeLong(largeBigInt);

    const readTap = new Tap(buffer);
    await assertRejects(
      async () => {
        await readArrayInto(
          readTap,
          (t) => t.readLong(),
          () => {},
        );
      },
      RangeError,
      "Array block length is outside the safe integer range.",
    );
  });
});

describe("readArrayIntoSync", () => {
  it("reads positive block count", async () => {
    const buffer = new ArrayBuffer(20);
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(2n);
    await writeTap.writeLong(10n);
    await writeTap.writeLong(20n);
    await writeTap.writeLong(0n);

    const encoded = buffer.slice(0, writeTap.getPos());
    const tap = new SyncReadableTap(encoded);
    const results: bigint[] = [];
    readArrayIntoSync(
      tap,
      (t: SyncReadableTapLike) => t.readLong(),
      (value: bigint) => results.push(value),
    );

    assertEquals(results, [10n, 20n]);
  });

  it("reads negative block count (size-prefixed)", async () => {
    const buffer = new ArrayBuffer(30);
    const writeTap = new Tap(buffer);
    await writeTap.writeLong(-2n);
    await writeTap.writeLong(100n);
    await writeTap.writeLong(30n);
    await writeTap.writeLong(40n);
    await writeTap.writeLong(0n);

    const encoded = buffer.slice(0, writeTap.getPos());
    const tap = new SyncReadableTap(encoded);
    const results: bigint[] = [];
    readArrayIntoSync(
      tap,
      (t: SyncReadableTapLike) => t.readLong(),
      (value: bigint) => results.push(value),
    );

    assertEquals(results, [30n, 40n]);
  });
});
