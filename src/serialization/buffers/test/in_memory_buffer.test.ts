import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  InMemoryReadableBuffer,
  InMemoryWritableBuffer,
} from "../in_memory_buffer.ts";

describe("InMemoryReadableBuffer", () => {
  describe("constructor", () => {
    it("creates with valid ArrayBuffer", () => {
      const buffer = new ArrayBuffer(100);
      const readable = new InMemoryReadableBuffer(buffer);
      assert(readable instanceof InMemoryReadableBuffer);
    });
  });
});

describe("InMemoryBufferBase (via implementations)", () => {
  describe("constructor", () => {
    it("creates view from ArrayBuffer", () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      const writable = new InMemoryWritableBuffer(buffer);
      assert(readable instanceof InMemoryReadableBuffer);
      assert(writable instanceof InMemoryWritableBuffer);
    });
  });

  describe("length", () => {
    it("returns correct length for both implementations", async () => {
      const buffer = new ArrayBuffer(50);
      const readable = new InMemoryReadableBuffer(buffer);
      const writable = new InMemoryWritableBuffer(buffer);

      assertEquals(await readable.length(), 50);
      assertEquals(await writable.length(), 50);
    });

    it("handles zero-length buffer", async () => {
      const buffer = new ArrayBuffer(0);
      const readable = new InMemoryReadableBuffer(buffer);
      const writable = new InMemoryWritableBuffer(buffer);

      assertEquals(await readable.length(), 0);
      assertEquals(await writable.length(), 0);
    });
  });

  describe("checkBounds (via read method)", () => {
    it("throws on negative offset", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(-1, 5),
        RangeError,
        "Offset and size must be non-negative",
      );
    });

    it("throws on negative size", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(0, -1),
        RangeError,
        "Offset and size must be non-negative",
      );
    });

    it("throws on both negative offset and size", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(-5, -1),
        RangeError,
        "Offset and size must be non-negative",
      );
    });

    it("throws when offset + size exceeds buffer length", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(8, 3),
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });

    it("throws when offset equals buffer length but size > 0", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(10, 1),
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });

    it("allows reading exactly to buffer end", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      const result = await readable.read(0, 10);
      assertEquals(result.length, 10);
    });

    it("allows reading zero bytes at any valid offset", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      const result = await readable.read(10, 0);
      assertEquals(result.length, 0);
    });
  });

  describe("checkWriteBounds (via appendBytes method)", () => {
    it("throws on negative offset (via initial constructor)", () => {
      const buffer = new ArrayBuffer(10);
      assertThrows(
        () => new InMemoryWritableBuffer(buffer, -1),
        RangeError,
        "Initial offset must be within buffer bounds",
      );
    });

    it("throws when write would exceed buffer bounds", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      await assertRejects(
        () => writable.appendBytes(new Uint8Array(11)),
        RangeError,
        "Write operation exceeds buffer bounds",
      );
    });

    it("throws when write at offset would exceed buffer bounds", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer, 8);
      await assertRejects(
        () => writable.appendBytes(new Uint8Array(3)),
        RangeError,
        "Write operation exceeds buffer bounds",
      );
    });

    it("allows writing exactly to buffer end", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      await writable.appendBytes(new Uint8Array(10));
      assertEquals(writable._testOnlyOffset(), 10);
    });

    it("allows writing zero bytes", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      await writable.appendBytes(new Uint8Array(0));
      assertEquals(writable._testOnlyOffset(), 0);
    });
  });

  describe("read", () => {
    it("reads data within bounds", async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      const buffer = new ArrayBuffer(data.length);
      new Uint8Array(buffer).set(data);

      const readable = new InMemoryReadableBuffer(buffer);
      const result = await readable.read(1, 3);
      assertEquals(result, new Uint8Array([2, 3, 4]));
    });

    it("reads entire buffer", async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      const buffer = new ArrayBuffer(data.length);
      new Uint8Array(buffer).set(data);

      const readable = new InMemoryReadableBuffer(buffer);
      const result = await readable.read(0, data.length);
      assertEquals(result, data);
    });

    it("reads zero bytes", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      const result = await readable.read(5, 0);
      assertEquals(result, new Uint8Array(0));
    });

    it("throws on negative offset", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(-1, 5),
        RangeError,
        "Offset and size must be non-negative",
      );
    });

    it("throws on negative size", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(0, -1),
        RangeError,
        "Offset and size must be non-negative",
      );
    });

    it("throws on offset beyond buffer", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(10, 1),
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });

    it("throws on size exceeding buffer", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(0, 11),
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });

    it("throws on offset + size exceeding buffer", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(5, 6),
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });

    it("throws when reading at exact buffer end", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(10, 1),
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });
  });
});

describe("InMemoryWritableBuffer", () => {
  describe("constructor", () => {
    it("creates with valid ArrayBuffer", () => {
      const buffer = new ArrayBuffer(100);
      const writable = new InMemoryWritableBuffer(buffer);
      assert(writable instanceof InMemoryWritableBuffer);
      assertEquals(writable._testOnlyOffset(), 0);
    });

    it("creates with initial offset", () => {
      const buffer = new ArrayBuffer(100);
      const writable = new InMemoryWritableBuffer(buffer, 50);
      assertEquals(writable._testOnlyOffset(), 50);
    });

    it("throws on negative initial offset", () => {
      const buffer = new ArrayBuffer(100);
      assertThrows(
        () => new InMemoryWritableBuffer(buffer, -1),
        RangeError,
        "Initial offset must be within buffer bounds",
      );
    });

    it("throws on initial offset beyond buffer", () => {
      const buffer = new ArrayBuffer(100);
      assertThrows(
        () => new InMemoryWritableBuffer(buffer, 101),
        RangeError,
        "Initial offset must be within buffer bounds",
      );
    });

    it("allows initial offset at buffer end", () => {
      const buffer = new ArrayBuffer(100);
      const writable = new InMemoryWritableBuffer(buffer, 100);
      assertEquals(writable._testOnlyOffset(), 100);
    });

    it("allows valid initial offset within buffer", () => {
      const buffer = new ArrayBuffer(100);
      const writable = new InMemoryWritableBuffer(buffer, 50);
      assertEquals(writable._testOnlyOffset(), 50);
    });
  });

  describe("length", () => {
    it("returns correct buffer length", async () => {
      const buffer = new ArrayBuffer(100);
      const writable = new InMemoryWritableBuffer(buffer);
      assertEquals(await writable.length(), 100);
    });
  });

  describe("appendBytes", () => {
    it("writes data within bounds", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      const data = new Uint8Array([1, 2, 3]);

      await writable.appendBytes(data);
      assertEquals(writable._testOnlyOffset(), 3);
      assertEquals(writable._testOnlyRemaining(), 7);

      // Verify data was written
      const view = new Uint8Array(buffer);
      assertEquals(view.slice(0, 3), data);
    });

    it("writes multiple chunks", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      const data1 = new Uint8Array([1, 2, 3]);
      const data2 = new Uint8Array([4, 5, 6]);

      await writable.appendBytes(data1);
      await writable.appendBytes(data2);
      assertEquals(writable._testOnlyOffset(), 6);
      assertEquals(writable._testOnlyRemaining(), 4);

      // Verify data was written
      const view = new Uint8Array(buffer);
      assertEquals(view.slice(0, 6), new Uint8Array([1, 2, 3, 4, 5, 6]));
    });

    it("writes empty data", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      const data = new Uint8Array(0);

      await writable.appendBytes(data);
      assertEquals(writable._testOnlyOffset(), 0);
      assertEquals(writable._testOnlyRemaining(), 10);
    });

    it("writes to exact buffer capacity", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      const data = new Uint8Array(10);
      data.fill(42);

      await writable.appendBytes(data);
      assertEquals(writable._testOnlyOffset(), 10);
      assertEquals(writable._testOnlyRemaining(), 0);

      // Verify data was written
      const view = new Uint8Array(buffer);
      assertEquals(view, data);
    });

    it("throws when writing beyond buffer capacity", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      const data = new Uint8Array(11);

      await assertRejects(
        () => writable.appendBytes(data),
        RangeError,
        "Write operation exceeds buffer bounds",
      );
    });

    it("throws when writing beyond buffer capacity from offset", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer, 8);
      const data = new Uint8Array([1, 2, 3]);

      await assertRejects(
        () => writable.appendBytes(data),
        RangeError,
        "Write operation exceeds buffer bounds",
      );
    });

    it("throws when writing beyond buffer capacity on second write", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);

      await writable.appendBytes(new Uint8Array([1, 2, 3, 4, 5]));

      await assertRejects(
        () => writable.appendBytes(new Uint8Array([1, 2, 3, 4, 5, 6])),
        RangeError,
        "Write operation exceeds buffer bounds",
      );
    });
  });

  describe("isValid", () => {
    it("always returns true", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      assertEquals(await writable.isValid(), true);

      await writable.appendBytes(new Uint8Array([1, 2, 3]));
      assertEquals(await writable.isValid(), true);
    });
  });

  describe("remaining", () => {
    it("calculates remaining space correctly", () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      assertEquals(writable._testOnlyRemaining(), 10);

      writable.appendBytes(new Uint8Array([1, 2, 3]));
      assertEquals(writable._testOnlyRemaining(), 7);
    });

    it("calculates remaining space from initial offset", () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer, 3);
      assertEquals(writable._testOnlyRemaining(), 7);
    });
  });

  describe("edge cases", () => {
    it("handles single byte buffer", async () => {
      const buffer = new ArrayBuffer(1);
      const writable = new InMemoryWritableBuffer(buffer);

      await writable.appendBytes(new Uint8Array([42]));
      assertEquals(writable._testOnlyOffset(), 1);
      assertEquals(writable._testOnlyRemaining(), 0);

      await assertRejects(
        () => writable.appendBytes(new Uint8Array([1])),
        RangeError,
        "Write operation exceeds buffer bounds",
      );
    });

    it("handles zero-sized buffer", async () => {
      const buffer = new ArrayBuffer(0);
      const writable = new InMemoryWritableBuffer(buffer);

      assertEquals(writable._testOnlyOffset(), 0);
      assertEquals(writable._testOnlyRemaining(), 0);

      await assertRejects(
        () => writable.appendBytes(new Uint8Array([1])),
        RangeError,
        "Write operation exceeds buffer bounds",
      );
    });
  });
});

describe("Strict bounds checking methods", () => {
  describe("InMemoryReadableBuffer checkBounds", () => {
    it("throws on negative offset", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(-1, 5),
        RangeError,
        "Offset and size must be non-negative",
      );
    });

    it("throws on negative size", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(0, -1),
        RangeError,
        "Offset and size must be non-negative",
      );
    });

    it("throws when operation exceeds buffer bounds", async () => {
      const buffer = new ArrayBuffer(10);
      const readable = new InMemoryReadableBuffer(buffer);
      await assertRejects(
        () => readable.read(8, 3),
        RangeError,
        "Operation exceeds buffer bounds",
      );
    });
  });

  describe("InMemoryWritableBuffer checkWriteBounds", () => {
    it("throws when write exceeds buffer bounds", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer);
      await assertRejects(
        () => writable.appendBytes(new Uint8Array(11)),
        RangeError,
        "Write operation exceeds buffer bounds",
      );
    });

    it("throws when write at offset exceeds buffer bounds", async () => {
      const buffer = new ArrayBuffer(10);
      const writable = new InMemoryWritableBuffer(buffer, 8);
      await assertRejects(
        () => writable.appendBytes(new Uint8Array(3)),
        RangeError,
        "Write operation exceeds buffer bounds",
      );
    });

    it("throws on negative offset in checkWriteBounds", () => {
      // Create a subclass that can test the protected checkWriteBounds method with negative offset
      class TestWritableBuffer extends InMemoryWritableBuffer {
        public constructor(buf: ArrayBuffer, offset = 0) {
          super(buf, offset);
        }

        // Create a method that calls the protected checkWriteBounds with negative offset
        public testCheckWriteBoundsWithNegativeOffset(): void {
          const data = new Uint8Array([1, 2, 3]);
          // Call the actual protected checkWriteBounds method with negative offset
          this.checkWriteBounds(-1, data);
        }
      }

      const testBuffer = new TestWritableBuffer(new ArrayBuffer(10));
      assertThrows(
        () => testBuffer.testCheckWriteBoundsWithNegativeOffset(),
        RangeError,
        "Offset must be non-negative",
      );
    });
  });
});
