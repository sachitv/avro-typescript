import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { Tap } from "../serialization/tap.ts";
import { ArrayType, readArrayInto } from "./array_type.ts";
import { IntType } from "./int_type.ts";
import { LongType } from "./long_type.ts";
import { StringType } from "./string_type.ts";
import { BytesType } from "./bytes_type.ts";
import { Type } from "./type.ts";

function createArray<T>(items: Type<T>): ArrayType<T> {
  return new ArrayType({ items });
}

describe("ArrayType", () => {
  const intItems = new IntType();
  const intArray = createArray(intItems);

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

  it("serializes and deserializes using blocks", () => {
    const buffer = new Uint8Array([1, 2, 2, 0]).buffer;
    const values = intArray.fromBuffer(buffer);
    assertEquals(values, [1]);
  });

  it("round-trips via toBuffer/fromBuffer", () => {
    const values = [4, 5, 6];
    const buffer = intArray.toBuffer(values);
    assertEquals(intArray.fromBuffer(buffer), values);
  });

  it("skips encoded array blocks", () => {
    const buffer = intArray.toBuffer([7, 8]);
    const tap = new Tap(buffer);
    intArray.skip(tap);
    assertEquals(tap._testOnlyPos, buffer.byteLength);
  });

  it("clones arrays deeply", () => {
    const original = [9, 10];
    const cloned = intArray.clone(original);
    assertEquals(cloned, original);
    cloned[0] = 99;
    assertEquals(original[0], 9);
  });

  it("compares arrays lexicographically", () => {
    assertEquals(intArray.compare([], []), 0);
    assertEquals(intArray.compare([], [1]), -1);
    assertEquals(intArray.compare([1, 2], [1, 3]), -1);
    assertEquals(intArray.compare([2], [1, 3]), 1);
  });

  it("generates random arrays", () => {
    const randomValue = intArray.random();
    assert(Array.isArray(randomValue));
  });

  it("exposes items type", () => {
    assertEquals(intArray.getItemsType(), intItems);
  });

  it("creates resolver for compatible writer arrays", () => {
    // Writer schema: array of strings
    const stringArray = createArray(new StringType());
    // Reader schema: array of byte arrays (compatible evolution)
    const bytesArray = createArray(new BytesType());
    // Create resolver to adapt from string array to byte array
    const resolver = bytesArray.createResolver(stringArray);
    // Write an array of 2 strings containing binary data
    const buffer = stringArray.toBuffer(["\x01\x02", "\x03\x04"]);
    const tap = new Tap(buffer);
    // Read using resolver: strings evolve to Uint8Arrays
    const result = resolver.read(tap) as Uint8Array[];
    // Result should be an array of 2 Uint8Arrays
    assertEquals(result.length, 2);
    assertEquals([...result[0]], [1, 2]); // First string "\x01\x02" -> bytes [1, 2]
    assertEquals([...result[1]], [3, 4]); // Second string "\x03\x04" -> bytes [3, 4]
  });

  it("throws resolver error for incompatible writer arrays", () => {
    const stringArray = createArray(new StringType());
    const intArrayReader = createArray(new IntType());
    assertThrows(() => intArrayReader.createResolver(stringArray));
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

  it("writes empty array correctly", () => {
    const buffer = intArray.toBuffer([]);
    const tap = new Tap(buffer);
    assertEquals(tap.readLong(), 0n);
  });

  it("round-trips empty array via toBuffer/fromBuffer", () => {
    const values: number[] = [];
    const buffer = intArray.toBuffer(values);
    assertEquals(intArray.fromBuffer(buffer), values);
  });

  it("throws error in write when value is not array", () => {
    const buffer = new ArrayBuffer(10);
    const tap = new Tap(buffer);
    assertThrows(
      () => intArray.write(tap, "not an array" as unknown as number[]),
      Error,
      "Invalid value",
    );
  });

  it("writes arrays correctly via write method", () => {
    const buffer = new ArrayBuffer(20);
    const writeTap = new Tap(buffer);
    intArray.write(writeTap, [10, 20]);

    const readTap = new Tap(buffer);
    const result = intArray.read(readTap);
    assertEquals(result, [10, 20]);
  });

  it("skips size-prefixed array blocks", () => {
    // Calculate block size
    const tempBuffer = new ArrayBuffer(10);
    const tempTap = new Tap(tempBuffer);
    tempTap.writeLong(100n);
    tempTap.writeLong(200n);
    const blockSize = tempTap._testOnlyPos;

    // Create a buffer with negative count (size-prefixed)
    const buffer = new ArrayBuffer(7); // -2n(1) + blockSize(1) + block(4) + 0n(1)
    const writeTap = new Tap(buffer);
    writeTap.writeLong(-2n); // negative count
    writeTap.writeLong(BigInt(blockSize)); // block size
    writeTap.writeLong(100n);
    writeTap.writeLong(200n);
    writeTap.writeLong(0n); // terminator

    const readTap = new Tap(buffer);
    intArray.skip(readTap);
    assertEquals(readTap._testOnlyPos, buffer.byteLength);
  });

  it("throws error in toBuffer when value is not array", () => {
    assertThrows(
      () => intArray.toBuffer("not an array" as unknown as number[]),
      Error,
      "Invalid value",
    );
  });

  it("throws error in clone when value is not array", () => {
    assertThrows(
      () => intArray.clone("not an array" as unknown as number[]),
      Error,
      "Cannot clone non-array value.",
    );
  });

  it("compares arrays by length when elements are equal", () => {
    assertEquals(intArray.compare([1, 2], [1, 2, 3]), -1);
    assertEquals(intArray.compare([1, 2, 3], [1, 2]), 1);
  });

  it("serializes to JSON correctly", () => {
    assertEquals(intArray.toJSON(), {
      type: "array",
      items: intItems.toJSON(),
    });
  });

  it("falls back to super createResolver for non-array writer types", () => {
    const stringType = new StringType();
    assertThrows(
      () => intArray.createResolver(stringType),
      Error,
      "Schema evolution not supported",
    );
  });

  it("handles nested arrays of ints", () => {
    // Create array of array of ints
    const intArrayOfInts = createArray(intArray);

    // Test data: array of arrays
    const nestedData = [
      [1, 2],
      [3, 4, 5],
      [6],
    ];

    // Round-trip via toBuffer/fromBuffer
    const buffer = intArrayOfInts.toBuffer(nestedData);
    const result = intArrayOfInts.fromBuffer(buffer);
    assertEquals(result, nestedData);

    // Test validation
    assert(intArrayOfInts.check(nestedData));
    assert(!intArrayOfInts.check("not an array"));
    assert(!intArrayOfInts.check([1, 2])); // inner elements must be arrays

    // Test cloning
    const cloned = intArrayOfInts.clone(nestedData);
    assertEquals(cloned, nestedData);
    cloned[0][0] = 99;
    assertEquals(nestedData[0][0], 1); // deep clone
  });

  it("resolves array of array of ints to array of array of longs", () => {
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
    const buffer = writerSchema.toBuffer(testData);
    const tap = new Tap(buffer);

    // Read with resolver (evolves ints to longs)
    const result = resolver.read(tap) as bigint[][];

    // Expected: arrays of bigints
    const expected = [
      [1n, 2n],
      [3n, 4n, 5n],
    ];
    assertEquals(result, expected);
  });

  it("should match encoded array buffers", () => {
    const arr1 = [1, 2];
    const arr2 = [1, 3];
    const arr3 = [1, 2, 4];

    const buf1 = intArray.toBuffer(arr1);
    const buf2 = intArray.toBuffer(arr2);
    const buf3 = intArray.toBuffer(arr3);

    assertEquals(intArray.match(new Tap(buf1), new Tap(buf2)), -1); // [1,2] < [1,3]
    assertEquals(intArray.match(new Tap(buf2), new Tap(buf1)), 1); // [1,3] > [1,2]
    assertEquals(intArray.match(new Tap(buf1), new Tap(buf3)), -1); // [1,2] < [1,2,4] (shorter first)
    assertEquals(intArray.match(new Tap(buf3), new Tap(buf1)), 1); // [1,2,4] > [1,2]
    const buf1_copy = intArray.toBuffer(arr1);
    assertEquals(intArray.match(new Tap(buf1), new Tap(buf1_copy)), 0); // equal

    // Test size-prefixed block (negative count)
    const sizePrefixedBuf = new ArrayBuffer(20);
    const tap = new Tap(sizePrefixedBuf);
    tap.writeLong(-2n); // negative count for size-prefixed
    tap.writeLong(3n); // byte size of the block (1 byte for int 1 + 2 bytes for int 32)
    tap.writeLong(1n);
    tap.writeLong(32n); // int 32 requires 2 bytes in zigzag varint encoding
    tap.writeLong(0n); // terminator

    const normalBuf = intArray.toBuffer([1, 32]);
    assertEquals(
      intArray.match(new Tap(sizePrefixedBuf), new Tap(normalBuf)),
      0,
    ); // should be equal
  });

  it("handles size-prefixed blocks inside match loop", () => {
    const items = [123, 456];

    // Calculate size of the second element to build a size-prefixed block
    const elementSize = intItems.toBuffer(items[1]).byteLength;

    const multiBlockBuf = new ArrayBuffer(30);
    const tap = new Tap(multiBlockBuf);

    // First block: standard, one element
    tap.writeLong(1n);
    intItems.write(tap, items[0]);

    // Second block: size-prefixed, one element
    tap.writeLong(-1n);
    tap.writeLong(BigInt(elementSize));
    intItems.write(tap, items[1]);

    // Terminator
    tap.writeLong(0n);

    const buf1 = multiBlockBuf.slice(0, tap._testOnlyPos);
    const buf2 = intArray.toBuffer(items);

    // This forces match() to call #readArraySize on a size-prefixed block
    // from inside its loop.
    assertEquals(intArray.match(new Tap(buf1), new Tap(buf2)), 0);
    assertEquals(intArray.match(new Tap(buf2), new Tap(buf1)), 0);
  });
});

describe("readArrayInto", () => {
  it("reads positive block count", () => {
    const buffer = new ArrayBuffer(20);
    const writeTap = new Tap(buffer);
    writeTap.writeLong(2n); // block count
    writeTap.writeLong(10n); // element 1
    writeTap.writeLong(20n); // element 2
    writeTap.writeLong(0n); // terminator

    const readTap = new Tap(buffer);
    const results: bigint[] = [];
    readArrayInto(
      readTap,
      (t) => t.readLong(),
      (value) => results.push(value),
    );
    assertEquals(results, [10n, 20n]);
  });

  it("reads negative block count (size-prefixed)", () => {
    const buffer = new ArrayBuffer(30);
    const writeTap = new Tap(buffer);
    writeTap.writeLong(-2n); // negative block count
    writeTap.writeLong(100n); // block size, ignored
    writeTap.writeLong(30n);
    writeTap.writeLong(40n);
    writeTap.writeLong(0n);

    const readTap = new Tap(buffer);
    const results: bigint[] = [];
    readArrayInto(
      readTap,
      (t) => t.readLong(),
      (value) => results.push(value),
    );
    assertEquals(results, [30n, 40n]);
  });

  it("stops at terminator", () => {
    const buffer = new ArrayBuffer(25);
    const writeTap = new Tap(buffer);
    writeTap.writeLong(1n);
    writeTap.writeLong(50n);
    writeTap.writeLong(0n); // terminator
    writeTap.writeLong(1n); // more, but should stop
    writeTap.writeLong(60n);

    const readTap = new Tap(buffer);
    const results: bigint[] = [];
    readArrayInto(
      readTap,
      (t) => t.readLong(),
      (value) => results.push(value),
    );
    assertEquals(results, [50n]);
  });

  it("throws on bigint outside safe integer range", () => {
    const buffer = new ArrayBuffer(15);
    const writeTap = new Tap(buffer);
    // Write a large bigint > MAX_SAFE_INTEGER
    const largeBigInt = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    writeTap.writeLong(largeBigInt);

    const readTap = new Tap(buffer);
    assertThrows(
      () => {
        readArrayInto(
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
