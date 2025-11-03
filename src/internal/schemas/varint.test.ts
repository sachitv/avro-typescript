import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { calculateVarintSize } from './varint.ts';

describe('calculateVarintSize', () => {
  it('should calculate correct sizes for numbers', () => {
    assertEquals(calculateVarintSize(0), 1);
    assertEquals(calculateVarintSize(-1), 1);
    assertEquals(calculateVarintSize(1), 1);
    assertEquals(calculateVarintSize(-2), 1);
    assertEquals(calculateVarintSize(63), 1);
    assertEquals(calculateVarintSize(-64), 1);
    assertEquals(calculateVarintSize(64), 2); // 64 zigzag is 128, 2 bytes
    assertEquals(calculateVarintSize(8191), 2);
    assertEquals(calculateVarintSize(8192), 3);
    assertEquals(calculateVarintSize(1048575), 3);
    assertEquals(calculateVarintSize(1048576), 4);
    assertEquals(calculateVarintSize(134217727), 4);
    assertEquals(calculateVarintSize(134217728), 5);
    assertEquals(calculateVarintSize(2147483647), 5);
    assertEquals(calculateVarintSize(-2147483648), 5);
  });

  it('should calculate correct sizes for bigints', () => {
    assertEquals(calculateVarintSize(0n), 1);
    assertEquals(calculateVarintSize(-1n), 1);
    assertEquals(calculateVarintSize(-2199023255552n), 6);
    assertEquals(calculateVarintSize(2199023255551n), 6);
    assertEquals(calculateVarintSize(-281474976710656n), 7);
    assertEquals(calculateVarintSize(281474976710655n), 7);
    assertEquals(calculateVarintSize(-4611686018427387904n), 9);
    assertEquals(calculateVarintSize(4611686018427387903n), 9);
    assertEquals(calculateVarintSize(9223372036854775807n), 10);
    assertEquals(calculateVarintSize(-9223372036854775808n), 10);
  });
});