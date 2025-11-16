import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type {
  BinaryDuplexLike,
  BinaryReadable,
  BinaryWritable,
} from "./transport_helpers.ts";
import {
  extractBinary,
  toBinaryDuplex,
  toBinaryReadable,
  toBinaryWritable,
} from "./transport_helpers.ts";

describe("toBinaryDuplex", () => {
  it("passes through BinaryDuplexLike", () => {
    const readable: BinaryReadable = {
      read: () => Promise.resolve(new Uint8Array([1, 2, 3])),
    };
    const writable: BinaryWritable = {
      write: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const input: BinaryDuplexLike = { readable, writable };
    const result = toBinaryDuplex(input);
    assertEquals(result.readable, readable);
    assertEquals(result.writable, writable);
  });
});

describe("toBinaryReadable", () => {
  it("passes through BinaryReadable", () => {
    const readable: BinaryReadable = {
      read: () => Promise.resolve(new Uint8Array([1, 2, 3])),
    };
    const result = toBinaryReadable(readable);
    assertEquals(result, readable);
  });

  it("wraps ReadableStream", async () => {
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1, 2, 3]));
        controller.close();
      },
    });
    const result = toBinaryReadable(stream);
    assert((result as unknown) !== stream);
    const data = await result.read();
    assertEquals(data, new Uint8Array([1, 2, 3]));
    const end = await result.read();
    assertEquals(end, null);
  });

  it("handles undefined value in ReadableStream", async () => {
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue((undefined as unknown) as Uint8Array);
        controller.close();
      },
    });
    const result = toBinaryReadable(stream);
    const data = await result.read();
    assertEquals(data, new Uint8Array(0));
  });

  it("throws on invalid input", () => {
    assertThrows(
      () => toBinaryReadable(({} as unknown) as BinaryReadable),
      TypeError,
      "Unsupported readable transport.",
    );
  });
});

describe("toBinaryWritable", () => {
  it("passes through BinaryWritable", () => {
    const writable: BinaryWritable = {
      write: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const result = toBinaryWritable(writable);
    assertEquals(result, writable);
  });

  it("wraps WritableStream", async () => {
    let written: Uint8Array | undefined;
    const stream = new WritableStream<Uint8Array>({
      write(chunk) {
        written = chunk;
      },
    });
    const result = toBinaryWritable(stream);
    assert((result as unknown) !== stream);
    await result.write(new Uint8Array([1, 2, 3]));
    assertEquals(written, new Uint8Array([1, 2, 3]));
    await result.close();
  });

  it("throws on invalid input", () => {
    assertThrows(
      () => toBinaryWritable(({} as unknown) as BinaryWritable),
      TypeError,
      "Unsupported writable transport.",
    );
  });
});

describe("extractBinary", () => {
  it("handles array buffer inputs", () => {
    const buffer = new ArrayBuffer(4);
    const result = extractBinary(buffer);
    assertEquals(result, new Uint8Array(buffer));
  });

  it("encodes strings using TextEncoder", () => {
    const result = extractBinary("hello");
    assertEquals(result, new TextEncoder().encode("hello"));
  });

  it("throws when receiving a blob", () => {
    const blob = new Blob([new Uint8Array([1, 2, 3])]);
    assertThrows(
      () => extractBinary(blob),
      Error,
      "Blob WebSocket messages are not supported.",
    );
  });

  it("throws on unsupported values", () => {
    assertThrows(
      () => extractBinary(42),
      Error,
      "Unsupported WebSocket message data.",
    );
  });
});
