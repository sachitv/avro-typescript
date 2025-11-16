import { assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type { BinaryReadable } from "../protocol/transports.ts";
import type {
  CallRequestEnvelope,
  CallResponseEnvelope,
} from "../protocol/wire_format/messages.ts";
import type { Message } from "../definitions/message_definition.ts";
import type { ResolverEntry } from "../definitions/protocol_definitions.ts";
import type { Resolver } from "../../../internal/schemas/resolver.ts";
import type { Type } from "../../../internal/schemas/type.ts";
import { ReadableTap } from "../../serialization/tap.ts";
import { createType } from "../../createType/mod.ts";
import type { RecordType } from "../../schemas/record_type.ts";
import type { UnionType } from "../../schemas/union_type.ts";
import {
  drainReadable,
  readRequestPayload,
  readResponsePayload,
  readSingleMessage,
} from "./helpers.ts";

describe("helpers", () => {
  describe("readSingleMessage", () => {
    it("reads a single framed message", async () => {
      const frame = new Uint8Array([1, 2, 3, 4]);
      // Create big-endian 32-bit length prefix
      const length = new Uint8Array(4);
      const view = new DataView(length.buffer);
      view.setUint32(0, frame.length, false); // big-endian
      const zero = new Uint8Array(4); // length 0 terminator
      const chunk = new Uint8Array(length.length + frame.length + zero.length);
      chunk.set(length, 0);
      chunk.set(frame, 4);
      chunk.set(zero, 4 + frame.length);

      let callCount = 0;
      const readable: BinaryReadable = {
        read: () => {
          callCount++;
          if (callCount === 1) return Promise.resolve(chunk);
          return Promise.resolve(null);
        },
      };

      const result = await readSingleMessage(readable);
      assertEquals(result, frame);
    });

    it("throws error when no framed message received", async () => {
      const readable: BinaryReadable = {
        read: () => Promise.resolve(null),
      };

      await assertRejects(
        async () => await readSingleMessage(readable),
        Error,
        "no framed message received",
      );
    });

    it("reads message assembled from multiple chunks", async () => {
      const frame = new Uint8Array([1, 2, 3, 4, 5]);
      // Big-endian length prefix
      const length = new Uint8Array(4);
      new DataView(length.buffer).setUint32(0, frame.length, false);
      const zero = new Uint8Array(4); // terminator

      let callCount = 0;
      const readable: BinaryReadable = {
        read: () => {
          callCount++;
          if (callCount === 1) return Promise.resolve(length); // length header
          if (callCount === 2) return Promise.resolve(frame); // frame data
          if (callCount === 3) return Promise.resolve(zero); // terminator
          return Promise.resolve(null);
        },
      };

      const result = await readSingleMessage(readable);
      assertEquals(result, frame);
    });
  });

  describe("drainReadable", () => {
    it("drains all data from readable", async () => {
      let callCount = 0;
      const readable: BinaryReadable = {
        read: () => {
          callCount++;
          if (callCount <= 3) return Promise.resolve(new Uint8Array([1, 2, 3]));
          return Promise.resolve(null);
        },
      };

      await drainReadable(readable);
      assertEquals(callCount, 4); // 3 data reads + 1 null read
    });

    it("handles empty readable", async () => {
      const readable: BinaryReadable = {
        read: () => Promise.resolve(null),
      };

      await drainReadable(readable);
    });
  });

  describe("readResponsePayload", () => {
    const mockResponseType = { read: () => Promise.resolve("test") };
    const mockErrorType = { read: () => Promise.resolve("test") };
    const mockMessage = {
      name: "test",
      requestType: createType({
        type: "record",
        name: "test",
        fields: [],
      }) as RecordType,
      responseType: mockResponseType as unknown as Type,
      errorType: mockErrorType as unknown as Type,
      oneWay: false,
      toJSON: () => ({}),
    } as Message;

    it("reads error payload when isError is true and resolver has error", async () => {
      const tap = new ReadableTap(new ArrayBuffer(0));
      const envelope: CallResponseEnvelope = {
        isError: true,
        bodyTap: tap,
        metadata: new Map(),
      };
      const resolver: ResolverEntry = {
        error: {
          readerType: mockErrorType,
          read: () => Promise.resolve("error payload"),
        } as unknown as Resolver<unknown>,
      };

      const result = await readResponsePayload(envelope, mockMessage, resolver);
      assertEquals(result, "error payload");
    });

    it("reads error payload when isError is true and no resolver error", async () => {
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setUint32(0, 4, false); // length
      new TextEncoder().encodeInto("test", new Uint8Array(buffer, 4));
      const tap = new ReadableTap(buffer);
      const envelope: CallResponseEnvelope = {
        isError: true,
        bodyTap: tap,
        metadata: new Map(),
      };

      const result = await readResponsePayload(envelope, mockMessage);
      assertEquals(result, "test");
    });

    it("reads response payload when isError is false and resolver has response", async () => {
      const tap = new ReadableTap(new ArrayBuffer(0));
      const envelope: CallResponseEnvelope = {
        isError: false,
        bodyTap: tap,
        metadata: new Map(),
      };
      const resolver: ResolverEntry = {
        response: {
          readerType: mockResponseType,
          read: () => Promise.resolve("response payload"),
        } as unknown as Resolver<unknown>,
      };

      const result = await readResponsePayload(envelope, mockMessage, resolver);
      assertEquals(result, "response payload");
    });

    it("reads response payload when isError is false and no resolver response", async () => {
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setUint32(0, 4, false); // length
      new TextEncoder().encodeInto("test", new Uint8Array(buffer, 4));
      const tap = new ReadableTap(buffer);
      const envelope: CallResponseEnvelope = {
        isError: false,
        bodyTap: tap,
        metadata: new Map(),
      };

      const result = await readResponsePayload(envelope, mockMessage);
      assertEquals(result, "test");
    });
  });

  describe("readRequestPayload", () => {
    const mockRequestType = { read: () => Promise.resolve("test") };
    const mockMessage = {
      name: "test",
      requestType: mockRequestType as unknown,
      responseType: createType("null"),
      errorType: createType("string") as UnionType,
      oneWay: false,
      toJSON: () => ({}),
    } as Message;

    it("reads request payload when resolver has request", async () => {
      const tap = new ReadableTap(new ArrayBuffer(0));
      const envelope: CallRequestEnvelope = {
        messageName: "test",
        bodyTap: tap,
        metadata: new Map(),
      };
      const resolver: ResolverEntry = {
        request: {
          readerType: mockRequestType,
          read: () => Promise.resolve("request payload"),
        } as unknown as Resolver<unknown>,
      };

      const result = await readRequestPayload(envelope, mockMessage, resolver);
      assertEquals(result, "request payload");
    });

    it("reads request payload when no resolver request", async () => {
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setUint32(0, 4, false); // length
      new TextEncoder().encodeInto("test", new Uint8Array(buffer, 4));
      const tap = new ReadableTap(buffer);
      const envelope: CallRequestEnvelope = {
        messageName: "test",
        bodyTap: tap,
        metadata: new Map(),
      };

      const result = await readRequestPayload(envelope, mockMessage);
      assertEquals(result, "test");
    });
  });
});
