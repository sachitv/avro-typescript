import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import { ReadBufferError } from "../sync_buffer.ts";
import { SyncBlobReadableBuffer } from "../sync_blob_readable_buffer.ts";

describe("SyncBlobReadableBuffer", () => {
  it("reads data from Uint8Array", () => {
    const data = new Uint8Array([10, 20, 30, 40, 50]);
    const buffer = new SyncBlobReadableBuffer(data);

    expect(buffer.length()).toBe(5);

    const result = buffer.read(1, 3);
    expect(result).toEqual(new Uint8Array([20, 30, 40]));
  });

  it("throws ReadBufferError for out of bounds read", () => {
    const data = new Uint8Array([1, 2, 3]);
    const buffer = new SyncBlobReadableBuffer(data);

    expect(() => buffer.read(2, 5)).toThrow(ReadBufferError);
  });

  it("throws ReadBufferError for negative offset", () => {
    const data = new Uint8Array([1, 2, 3]);
    const buffer = new SyncBlobReadableBuffer(data);

    expect(() => buffer.read(-1, 1)).toThrow(ReadBufferError);
  });

  it("throws ReadBufferError for negative size", () => {
    const data = new Uint8Array([1, 2, 3]);
    const buffer = new SyncBlobReadableBuffer(data);

    expect(() => buffer.read(0, -1)).toThrow(ReadBufferError);
  });

  it("canReadMore works correctly", () => {
    const data = new Uint8Array([1, 2, 3]);
    const buffer = new SyncBlobReadableBuffer(data);

    expect(buffer.canReadMore(0)).toBe(true);
    expect(buffer.canReadMore(2)).toBe(true);
    expect(buffer.canReadMore(3)).toBe(false);
  });

  it("handles empty Uint8Array", () => {
    const data = new Uint8Array(0);
    const buffer = new SyncBlobReadableBuffer(data);

    expect(buffer.length()).toBe(0);
    expect(() => buffer.read(0, 1)).toThrow(ReadBufferError);
    expect(buffer.canReadMore(0)).toBe(false);
  });

  it("rethrows non-ReadBufferError from canReadMore read", () => {
    class ThrowingBuffer extends SyncBlobReadableBuffer {
      public override read(_offset: number, _size: number): Uint8Array {
        throw new TypeError("boom");
      }
    }

    const buffer = new ThrowingBuffer(new Uint8Array([1, 2, 3]));
    expect(() => buffer.canReadMore(0)).toThrow(TypeError);
  });
});
