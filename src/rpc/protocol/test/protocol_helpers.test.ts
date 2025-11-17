import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { Message } from "../../definitions/message_definition.ts";
import type { ProtocolLike } from "../../definitions/protocol_definitions.ts";
import type { CreateTypeOptions } from "../../../type/create_type.ts";
import type { NamedType } from "../../../schemas/complex/named_type.ts";
import type { Type } from "../../../schemas/type.ts";
import {
  bytesEqual,
  bytesToHex,
  errorToPayload,
  int32ToBytes,
  metadataWithId,
  normalizeProtocolName,
  stringifyProtocol,
  toArrayBuffer,
} from "../protocol_helpers.ts";

describe("protocol helpers", () => {
  it("normalizes names with namespace", () => {
    assertEquals(
      normalizeProtocolName("Calculator", "org.example"),
      "org.example.Calculator",
    );
    assertEquals(
      normalizeProtocolName("org.example.Calculator"),
      "org.example.Calculator",
    );
  });

  it("constructs metadata copies and adds id", () => {
    const base = new Map([["key", new Uint8Array([1, 2, 3])]]);
    const metadata = metadataWithId(7, base) as Map<string, Uint8Array>;
    assertEquals(metadata.get("id"), int32ToBytes(7));
    assertEquals(metadata.get("key"), new Uint8Array([1, 2, 3]));
    assert(metadata.get("key") !== base.get("key")); // Ensure copy
    assert(metadata.has("key"));
  });

  it("compares bytes and renders hex", () => {
    const bytes = new Uint8Array([0x12, 0xab]);
    assertEquals(bytesToHex(bytes), "12ab");
    assert(bytesEqual(bytes, new Uint8Array([0x12, 0xab])));
    assert(!bytesEqual(bytes, new Uint8Array([0x12])));
    assert(!bytesEqual(bytes, new Uint8Array([0x12, 0xac])));
  });

  it("converts bytes to ArrayBuffer", () => {
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const view = toArrayBuffer(bytes);
    assertEquals(view.byteLength, 4);
  });

  it("converts errors to payloads", () => {
    assertEquals(errorToPayload("boom"), { string: "boom" });
    assertEquals(errorToPayload(new Error("boom")), { string: "boom" });
    assertEquals(errorToPayload(123), { string: "123" });
  });

  it("throws on missing protocol name", () => {
    assertThrows(
      () => normalizeProtocolName(""),
      Error,
      "missing protocol name",
    );
  });

  it("constructs metadata from array and adds id", () => {
    const original = new Uint8Array([1, 2, 3]);
    const base: [string, Uint8Array][] = [["key", original]];
    const metadata = metadataWithId(42, base) as Map<string, Uint8Array>;
    assertEquals(metadata.get("id"), int32ToBytes(42));
    assertEquals(metadata.get("key"), new Uint8Array([1, 2, 3]));
    assert(metadata.get("key") !== original); // Ensure copy
  });

  it("constructs metadata from object and adds id", () => {
    const original = new Uint8Array([4, 5, 6]);
    const base = { key: original };
    const metadata = metadataWithId(99, base) as Map<string, Uint8Array>;
    assertEquals(metadata.get("id"), int32ToBytes(99));
    assertEquals(metadata.get("key"), new Uint8Array([4, 5, 6]));
    assert(metadata.get("key") !== original); // Ensure copy
  });

  it("converts int32 to bytes", () => {
    assertEquals(int32ToBytes(0), new Uint8Array([0, 0, 0, 0]));
    assertEquals(int32ToBytes(1), new Uint8Array([0, 0, 0, 1]));
    assertEquals(int32ToBytes(256), new Uint8Array([0, 0, 1, 0]));
  });

  it("stringifies protocol", () => {
    // Mock Message
    const mockMessage = {
      toJSON: () => ({ request: ["string"], response: "string" }),
    } as unknown as Message;

    // Mock NamedType
    const mockNamedType1 = {
      getFullName: () => "TestType",
    } as unknown as NamedType;
    const mockNamedType2 = {
      getFullName: () => "TestType", // duplicate
    } as unknown as NamedType;

    // Mock Type
    const mockType = {
      toJSON: () => ({ type: "string" }),
    } as unknown as Type;

    // Mock ProtocolLike with required methods
    const mockProtocol = {
      getName: () => "TestProtocol",
      getNamedTypes: () => [mockNamedType1, mockNamedType2, mockType],
      getMessages: () => new Map([["testMessage", mockMessage]]),
      hashKey: "test",
      getHashBytes: () => new Uint8Array(),
      getHandler: () => undefined,
      hasListenerResolvers: () => false,
      hasEmitterResolvers: () => false,
      ensureEmitterResolvers: () => {},
      ensureListenerResolvers: () => {},
      getEmitterResolvers: () => undefined,
      getListenerResolvers: () => undefined,
      toString: () => "TestProtocol",
    } as unknown as ProtocolLike;
    const result = stringifyProtocol(mockProtocol);
    const parsed = JSON.parse(result);
    assertEquals(parsed.protocol, "TestProtocol");
    assertEquals(parsed.types.length, 3);
    assertEquals(parsed.messages.testMessage.request, ["string"]);
  });

  it("stringifies protocol with no messages", () => {
    const mockProtocol = {
      getName: () => "EmptyProtocol",
      getNamedTypes: () => [],
      getMessages: () => new Map(),
      hashKey: "test",
      getHashBytes: () => new Uint8Array(),
      getHandler: () => undefined,
      hasListenerResolvers: () => false,
      hasEmitterResolvers: () => false,
      ensureEmitterResolvers: () => {},
      ensureListenerResolvers: () => {},
      getEmitterResolvers: () => undefined,
      getListenerResolvers: () => undefined,
      toString: () => "EmptyProtocol",
    } as unknown as ProtocolLike;
    const result = stringifyProtocol(mockProtocol);
    const parsed = JSON.parse(result);
    assertEquals(parsed.protocol, "EmptyProtocol");
    assertEquals(parsed.types, undefined);
    assertEquals(Object.keys(parsed.messages).length, 0);
  });

  it("stringifies protocol with no types", () => {
    const mockMessage = {
      toJSON: () => ({ request: ["int"], response: "int" }),
    } as unknown as Message;
    const mockProtocol = {
      getName: () => "NoTypesProtocol",
      getNamedTypes: () => [],
      getMessages: () => new Map([["add", mockMessage]]),
      hashKey: "test",
      getHashBytes: () => new Uint8Array(),
      getHandler: () => undefined,
      hasListenerResolvers: () => false,
      hasEmitterResolvers: () => false,
      ensureEmitterResolvers: () => {},
      ensureListenerResolvers: () => {},
      getEmitterResolvers: () => undefined,
      getListenerResolvers: () => undefined,
      toString: () => "NoTypesProtocol",
    } as unknown as ProtocolLike;
    const result = stringifyProtocol(mockProtocol);
    const parsed = JSON.parse(result);
    assertEquals(parsed.protocol, "NoTypesProtocol");
    assertEquals(parsed.types, undefined);
    assertEquals(parsed.messages.add.request, ["int"]);
  });

  it("stringifies protocol with multiple messages", () => {
    const mockMessage1 = {
      toJSON: () => ({ request: ["string"], response: "string" }),
    } as unknown as Message;
    const mockMessage2 = {
      toJSON: () => ({ request: ["int"], response: "int" }),
    } as unknown as Message;
    const mockProtocol = {
      getName: () => "MultiMessageProtocol",
      getNamedTypes: () => [],
      getMessages: () =>
        new Map([["echo", mockMessage1], ["add", mockMessage2]]),
      hashKey: "test",
      getHashBytes: () => new Uint8Array(),
      getHandler: () => undefined,
      hasListenerResolvers: () => false,
      hasEmitterResolvers: () => false,
      ensureEmitterResolvers: () => {},
      ensureListenerResolvers: () => {},
      getEmitterResolvers: () => undefined,
      getListenerResolvers: () => undefined,
      toString: () => "MultiMessageProtocol",
    } as unknown as ProtocolLike;
    const result = stringifyProtocol(mockProtocol);
    const parsed = JSON.parse(result);
    assertEquals(parsed.protocol, "MultiMessageProtocol");
    assertEquals(Object.keys(parsed.messages).length, 2);
    assertEquals(parsed.messages.echo.request, ["string"]);
    assertEquals(parsed.messages.add.request, ["int"]);
  });

  it("relies on toJSON implementations", () => {
    const message = new Message("Ping", {
      request: [{ name: "value", type: "string" }],
      response: "null",
    }, {} as CreateTypeOptions);
    assertEquals(typeof message.toJSON, "function");
    assertEquals(typeof message.requestType.toJSON, "function");
    assertEquals(typeof message.responseType.toJSON, "function");
    assertEquals(typeof message.errorType.toJSON, "function");
  });

  it("handles non-named types", () => {
    const mockType = {
      toJSON: () => ({ type: "record", name: "InlineRecord" }),
    } as unknown as Type;
    const mockProtocol = {
      getName: () => "NonNamedTypesProtocol",
      getNamedTypes: () => [mockType],
      getMessages: () => new Map(),
      hashKey: "test",
      getHashBytes: () => new Uint8Array(),
      getHandler: () => undefined,
      hasListenerResolvers: () => false,
      hasEmitterResolvers: () => false,
      ensureEmitterResolvers: () => {},
      ensureListenerResolvers: () => {},
      getEmitterResolvers: () => undefined,
      getListenerResolvers: () => undefined,
      toString: () => "NonNamedTypesProtocol",
    } as unknown as ProtocolLike;
    const result = stringifyProtocol(mockProtocol);
    const parsed = JSON.parse(result);
    assertEquals(parsed.types.length, 1);
    assertEquals(parsed.types[0].type, "record");
    assertEquals(parsed.types[0].name, "InlineRecord");
  });
});
