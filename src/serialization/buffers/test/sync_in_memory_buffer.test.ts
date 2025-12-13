import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import {
  SyncInMemoryBufferBase,
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "../sync_in_memory_buffer.ts";
import { ReadBufferError, WriteBufferError } from "../sync_buffer.ts";

class TestSyncInMemoryBuffer extends SyncInMemoryBufferBase {
  public exposeBuffer(): ArrayBuffer {
    return this._testOnlyBuffer();
  }
}

describe("SyncInMemoryBufferBase", () => {
  it("exposes the underlying ArrayBuffer", () => {
    const buffer = new ArrayBuffer(2);
    const testBuffer = new TestSyncInMemoryBuffer(buffer);

    expect(testBuffer.exposeBuffer()).toBe(buffer);
  });
});

describe("SyncInMemoryReadableBuffer", () => {
  it("reads data correctly", () => {
    const buffer = new ArrayBuffer(10);
    const view = new Uint8Array(buffer);
    view.set([1, 2, 3, 4, 5]);

    const readable = new SyncInMemoryReadableBuffer(buffer);
    expect(readable.length()).toBe(10);

    const data = readable.read(0, 4);
    expect(data).toEqual(new Uint8Array([1, 2, 3, 4]));
  });

  it("throws on out of bounds read", () => {
    const buffer = new ArrayBuffer(5);
    const readable = new SyncInMemoryReadableBuffer(buffer);

    expect(() => readable.read(3, 3)).toThrow(ReadBufferError);
  });

  it("canReadMore returns true for valid offsets", () => {
    const buffer = new ArrayBuffer(5);
    const readable = new SyncInMemoryReadableBuffer(buffer);

    expect(readable.canReadMore(0)).toBe(true);
    expect(readable.canReadMore(4)).toBe(true);
    expect(readable.canReadMore(5)).toBe(false);
  });

  it("throws on negative offset", () => {
    const buffer = new ArrayBuffer(5);
    const readable = new SyncInMemoryReadableBuffer(buffer);

    expect(() => readable.read(-1, 1)).toThrow(ReadBufferError);
  });

  it("throws on negative size", () => {
    const buffer = new ArrayBuffer(5);
    const readable = new SyncInMemoryReadableBuffer(buffer);

    expect(() => readable.read(0, -1)).toThrow(ReadBufferError);
  });

  it("canReadMore returns false for negative offset", () => {
    const buffer = new ArrayBuffer(5);
    const readable = new SyncInMemoryReadableBuffer(buffer);

    expect(readable.canReadMore(-1)).toBe(false);
  });

  it("exposes the underlying ArrayBuffer via _testOnlyBuffer", () => {
    const buffer = new ArrayBuffer(3);
    const readable = new SyncInMemoryReadableBuffer(buffer);

    expect(readable._testOnlyBuffer()).toBe(buffer);
  });
});

describe("SyncInMemoryWritableBuffer", () => {
  it("writes and reads data correctly", () => {
    const buffer = new ArrayBuffer(10);
    const writable = new SyncInMemoryWritableBuffer(buffer);

    writable.appendBytes(new Uint8Array([1, 2, 3]));
    writable.appendBytes(new Uint8Array([4, 5]));

    expect(writable._testOnlyOffset()).toBe(5);
    expect(writable._testOnlyRemaining()).toBe(5);

    // Read back using a readable buffer
    const readable = new SyncInMemoryReadableBuffer(buffer);
    const data = readable.read(0, 5);
    expect(data).toEqual(new Uint8Array([1, 2, 3, 4, 5]));
  });

  it("throws on write overflow", () => {
    const buffer = new ArrayBuffer(5);
    const writable = new SyncInMemoryWritableBuffer(buffer);

    expect(() => writable.appendBytes(new Uint8Array(10))).toThrow(
      WriteBufferError,
    );
  });

  it("isValid returns true", () => {
    const buffer = new ArrayBuffer(10);
    const writable = new SyncInMemoryWritableBuffer(buffer);

    expect(writable.isValid()).toBe(true);
    expect(writable.canAppendMore(5)).toBe(true);
  });

  it("exposes the underlying ArrayBuffer via _testOnlyBuffer", () => {
    const buffer = new ArrayBuffer(4);
    const writable = new SyncInMemoryWritableBuffer(buffer);

    writable.appendBytes(new Uint8Array([1]));
    expect(writable._testOnlyBuffer()).toBe(buffer);
  });

  it("getBufferCopy returns written data", () => {
    const buffer = new ArrayBuffer(10);
    const writable = new SyncInMemoryWritableBuffer(buffer);

    writable.appendBytes(new Uint8Array([1, 2, 3]));

    const copy = writable.getBufferCopy();
    expect(new Uint8Array(copy)).toEqual(new Uint8Array([1, 2, 3]));
  });

  it("constructor with initial offset", () => {
    const buffer = new ArrayBuffer(10);
    const writable = new SyncInMemoryWritableBuffer(buffer, 5);

    expect(writable._testOnlyOffset()).toBe(5);
    expect(writable._testOnlyRemaining()).toBe(5);
  });

  it("throws on invalid initial offset negative", () => {
    const buffer = new ArrayBuffer(10);

    expect(() => new SyncInMemoryWritableBuffer(buffer, -1)).toThrow(
      WriteBufferError,
    );
  });

  it("throws on invalid initial offset too large", () => {
    const buffer = new ArrayBuffer(10);

    expect(() => new SyncInMemoryWritableBuffer(buffer, 11)).toThrow(
      WriteBufferError,
    );
  });

  it("appends empty data without advancing offset", () => {
    const buffer = new ArrayBuffer(10);
    const writable = new SyncInMemoryWritableBuffer(buffer);

    writable.appendBytes(new Uint8Array(0));

    expect(writable._testOnlyOffset()).toBe(0);
  });

  it("getBufferCopy with initial offset", () => {
    const buffer = new ArrayBuffer(10);
    const writable = new SyncInMemoryWritableBuffer(buffer, 2);

    writable.appendBytes(new Uint8Array([1, 2, 3]));

    const copy = writable.getBufferCopy();
    expect(new Uint8Array(copy)).toEqual(new Uint8Array([1, 2, 3]));
  });

  it("checkWriteBounds throws on negative offset", () => {
    const buffer = new ArrayBuffer(10);
    const writable = new SyncInMemoryWritableBuffer(buffer);

    expect(() => writable._testCheckWriteBounds(-1, new Uint8Array([1])))
      .toThrow(
        WriteBufferError,
      );
  });
});
