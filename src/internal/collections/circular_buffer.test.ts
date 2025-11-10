import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { CircularBuffer } from "./circular_buffer.ts";

describe("CircularBuffer", () => {
  describe("constructor", () => {
    it("creates with valid capacity", () => {
      const buffer = new CircularBuffer(10);
      assertEquals(buffer.capacity(), 10);
      assertEquals(buffer.length(), 0);
      assertEquals(buffer.windowStart(), 0);
      assertEquals(buffer.windowEnd(), 0);
    });

    it("throws on invalid capacity", () => {
      assertThrows(
        () => new CircularBuffer(0),
        RangeError,
        "Capacity must be positive",
      );
      assertThrows(
        () => new CircularBuffer(-1),
        RangeError,
        "Capacity must be positive",
      );
    });
  });

  describe("push", () => {
    it("adds data to empty buffer", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([1, 2, 3]));
      assertEquals(buffer.length(), 3);
      assertEquals(buffer.windowStart(), 0);
      assertEquals(buffer.windowEnd(), 3);
    });

    it("adds data without sliding when within capacity", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([1, 2]));
      buffer.push(new Uint8Array([3, 4]));
      assertEquals(buffer.length(), 4);
      assertEquals(buffer.windowStart(), 0);
      assertEquals(buffer.windowEnd(), 4);
    });

    it("slides window when adding data would exceed capacity", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([1, 2, 3]));
      buffer.push(new Uint8Array([4, 5]));
      assertEquals(buffer.length(), 5);
      assertEquals(buffer.windowStart(), 0);
      assertEquals(buffer.windowEnd(), 5);

      // This should slide the window
      buffer.push(new Uint8Array([6, 7]));
      assertEquals(buffer.length(), 5);
      assertEquals(buffer.windowStart(), 2);
      assertEquals(buffer.windowEnd(), 7);
    });

    it("handles empty data array", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array(0));
      assertEquals(buffer.length(), 0);
      assertEquals(buffer.windowStart(), 0);
      assertEquals(buffer.windowEnd(), 0);
    });

    it("throws when data size exceeds capacity", () => {
      const buffer = new CircularBuffer(3);
      assertThrows(
        () => buffer.push(new Uint8Array([1, 2, 3, 4])),
        RangeError,
        "Data size 4 exceeds buffer capacity 3",
      );
    });

    it("handles multiple slides correctly", () => {
      const buffer = new CircularBuffer(3);
      buffer.push(new Uint8Array([1, 2, 3]));
      assertEquals(buffer.windowStart(), 0);
      assertEquals(buffer.windowEnd(), 3);

      buffer.push(new Uint8Array([4]));
      assertEquals(buffer.windowStart(), 1);
      assertEquals(buffer.windowEnd(), 4);

      buffer.push(new Uint8Array([5]));
      assertEquals(buffer.windowStart(), 2);
      assertEquals(buffer.windowEnd(), 5);

      buffer.push(new Uint8Array([6]));
      assertEquals(buffer.windowStart(), 3);
      assertEquals(buffer.windowEnd(), 6);
    });
  });

  describe("get", () => {
    it("retrieves data from buffer", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([1, 2, 3, 4, 5]));

      const result = buffer.get(1, 3);
      assertEquals(result, new Uint8Array([2, 3, 4]));
    });

    it("throws error for start before window", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([1, 2, 3]));

      assertThrows(
        () => buffer.get(-1, 2),
        RangeError,
        "Start position -1 is before window start 0",
      );
    });

    it("throws error for range beyond window", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([1, 2, 3]));

      assertThrows(
        () => buffer.get(1, 3),
        RangeError,
        "Requested range [1, 4) extends beyond window end 3",
      );
    });

    it("throws error for negative size", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([1, 2, 3]));

      assertThrows(
        () => buffer.get(0, -1),
        RangeError,
        "Size -1 cannot be negative",
      );
    });

    it("returns empty array for zero size", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([1, 2, 3]));

      const result = buffer.get(1, 0);
      assertEquals(result, new Uint8Array(0));
    });

    it("retrieves data after window slide", () => {
      const buffer = new CircularBuffer(3);
      buffer.push(new Uint8Array([1, 2, 3]));
      // This slides window to start at original data index 2 (value is 3).
      buffer.push(new Uint8Array([4, 5]));
      const result = buffer.get(2, 3);
      assertEquals(result, new Uint8Array([3, 4, 5]));
    });

    it("handles edge cases correctly", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([10, 20, 30]));

      // Get first element
      assertEquals(buffer.get(0, 1), new Uint8Array([10]));

      // Get last element
      assertEquals(buffer.get(2, 1), new Uint8Array([30]));

      // Get all elements
      assertEquals(buffer.get(0, 3), new Uint8Array([10, 20, 30]));
    });
  });

  describe("clear", () => {
    it("clears an empty buffer", () => {
      const buffer = new CircularBuffer(5);
      buffer.clear();
      assertEquals(buffer.length(), 0);
      assertEquals(buffer.windowStart(), 0);
      assertEquals(buffer.windowEnd(), 0);
    });

    it("clears a buffer with data", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([1, 2, 3]));
      assertEquals(buffer.length(), 3);

      buffer.clear();
      assertEquals(buffer.length(), 0);
      assertEquals(buffer.windowStart(), 0);
      assertEquals(buffer.windowEnd(), 0);
    });

    it("clears after window slide", () => {
      const buffer = new CircularBuffer(3);
      buffer.push(new Uint8Array([1, 2, 3]));
      buffer.push(new Uint8Array([4])); // slides
      assertEquals(buffer.windowStart(), 1);
      assertEquals(buffer.windowEnd(), 4);

      buffer.clear();
      assertEquals(buffer.length(), 0);
      assertEquals(buffer.windowStart(), 0);
      assertEquals(buffer.windowEnd(), 0);
    });

    it("allows pushing after clear", () => {
      const buffer = new CircularBuffer(5);
      buffer.push(new Uint8Array([1, 2, 3]));
      buffer.clear();
      buffer.push(new Uint8Array([4, 5]));
      assertEquals(buffer.length(), 2);
      assertEquals(buffer.get(0, 2), new Uint8Array([4, 5]));
    });
  });

  describe("integration", () => {
    it("maintains data integrity through multiple operations", () => {
      const buffer = new CircularBuffer(4);

      // Fill buffer
      buffer.push(new Uint8Array([1, 2, 3, 4]));
      assertEquals(buffer.get(0, 4), new Uint8Array([1, 2, 3, 4]));

      // Add more data (should slide)
      buffer.push(new Uint8Array([5, 6]));
      assertEquals(buffer.windowStart(), 2);
      assertEquals(buffer.windowEnd(), 6);
      assertEquals(buffer.get(2, 4), new Uint8Array([3, 4, 5, 6]));

      // Add more data
      buffer.push(new Uint8Array([7]));
      assertEquals(buffer.windowStart(), 3);
      assertEquals(buffer.windowEnd(), 7);
      assertEquals(buffer.get(3, 4), new Uint8Array([4, 5, 6, 7]));
    });

    it("handles Uint8Array data correctly", () => {
      const buffer = new CircularBuffer(6);

      const chunk1 = new Uint8Array([1, 2, 3]);
      const chunk2 = new Uint8Array([4, 5, 6]);
      const chunk3 = new Uint8Array([7, 8, 9]);

      buffer.push(chunk1);
      buffer.push(chunk2);

      const result = buffer.get(0, 6);
      assertEquals(result, new Uint8Array([1, 2, 3, 4, 5, 6]));

      buffer.push(chunk3); // Should slide window
      assertEquals(buffer.get(3, 3), new Uint8Array([4, 5, 6]));
    });

    it("handles Uint8Array subarrays", () => {
      const buffer = new CircularBuffer(5);
      const data = new Uint8Array([10, 20, 30, 40, 50]);

      // Push subarray
      buffer.push(data.subarray(1, 4)); // [20, 30, 40]
      assertEquals(buffer.get(0, 3), new Uint8Array([20, 30, 40]));

      // Push another subarray
      buffer.push(data.subarray(0, 2)); // [10, 20]
      assertEquals(buffer.get(0, 5), new Uint8Array([20, 30, 40, 10, 20]));
    });

    it("handles capacity 1 edge cases", () => {
      const buffer = new CircularBuffer(1);

      // Push single byte
      buffer.push(new Uint8Array([42]));
      assertEquals(buffer.length(), 1);
      assertEquals(buffer.get(0, 1), new Uint8Array([42]));

      // Push another, should slide
      buffer.push(new Uint8Array([43]));
      assertEquals(buffer.length(), 1);
      assertEquals(buffer.windowStart(), 1);
      assertEquals(buffer.windowEnd(), 2);
      assertEquals(buffer.get(1, 1), new Uint8Array([43]));

      // Push empty, no change
      buffer.push(new Uint8Array(0));
      assertEquals(buffer.get(1, 1), new Uint8Array([43]));
    });

    it("verifies buffer integrity with many operations", () => {
      const buffer = new CircularBuffer(10);
      let expected: number[] = [];

      // Perform many push and get operations
      for (let i = 0; i < 50; i++) {
        const data = new Uint8Array([i % 256]);
        buffer.push(data);
        expected.push(i % 256);
        if (expected.length > 10) {
          expected = expected.slice(-10);
        }

        // Verify the current buffer content
        const actual = buffer.get(buffer.windowStart(), buffer.length());
        assertEquals(actual, new Uint8Array(expected));
      }
    });

    it("handles large data within capacity", () => {
      const capacity = 1000;
      const buffer = new CircularBuffer(capacity);
      const largeData = new Uint8Array(capacity);
      for (let i = 0; i < capacity; i++) {
        largeData[i] = i % 256;
      }

      buffer.push(largeData);
      assertEquals(buffer.length(), capacity);
      const retrieved = buffer.get(0, capacity);
      assertEquals(retrieved, largeData);
    });
  });
});
