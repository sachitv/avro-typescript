import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { ReadableTap } from "../../../serialization/tap.ts";
import {
  _extractOptionalMd5,
  _extractOptionalMetadata,
  _extractOptionalString,
  decodeHandshakeRequest,
  decodeHandshakeResponse,
  encodeHandshakeRequest,
  encodeHandshakeResponse,
  type HandshakeRequestInit,
  type HandshakeResponseInit,
  readHandshakeRequestFromTap,
  readHandshakeResponseFromTap,
} from "./handshake.ts";

function toArrayBuffer(data: Uint8Array): ArrayBuffer {
  return data.slice().buffer;
}

describe("handshake", () => {
  describe("encodeHandshakeRequest and decodeHandshakeRequest", () => {
    it("encodes and decodes with all fields", async () => {
      const meta = new Map<string, Uint8Array>([
        ["trace", new Uint8Array([1, 2, 3])],
      ]);
      const handshake: HandshakeRequestInit = {
        clientHash: new Uint8Array(16).fill(1),
        clientProtocol: "test protocol",
        serverHash: new Uint8Array(16).fill(2),
        meta,
      };

      const encoded = await encodeHandshakeRequest(handshake);
      const decoded = await decodeHandshakeRequest(toArrayBuffer(encoded));

      assertEquals(decoded.clientHash, handshake.clientHash);
      assertEquals(decoded.clientProtocol, handshake.clientProtocol);
      assertEquals(decoded.serverHash, handshake.serverHash);
      assertEquals(decoded.meta, handshake.meta);
    });

    it("encodes and decodes with null clientProtocol", async () => {
      const handshake: HandshakeRequestInit = {
        clientHash: new Uint8Array(16).fill(1),
        serverHash: new Uint8Array(16).fill(2),
      };

      const encoded = await encodeHandshakeRequest(handshake);
      const decoded = await decodeHandshakeRequest(toArrayBuffer(encoded));

      assertEquals(decoded.clientProtocol, null);
    });

    it("encodes and decodes with null meta", async () => {
      const handshake: HandshakeRequestInit = {
        clientHash: new Uint8Array(16).fill(1),
        serverHash: new Uint8Array(16).fill(2),
        meta: null,
      };

      const encoded = await encodeHandshakeRequest(handshake);
      const decoded = await decodeHandshakeRequest(toArrayBuffer(encoded));

      assertEquals(decoded.meta, null);
    });

    it("encodes and decodes with empty meta", async () => {
      const handshake: HandshakeRequestInit = {
        clientHash: new Uint8Array(16).fill(1),
        serverHash: new Uint8Array(16).fill(2),
        meta: new Map(),
      };

      const encoded = await encodeHandshakeRequest(handshake);
      const decoded = await decodeHandshakeRequest(toArrayBuffer(encoded));

      assertEquals(decoded.meta, new Map());
    });

    it("rejects invalid clientHash size", async () => {
      const handshake: HandshakeRequestInit = {
        clientHash: new Uint8Array(15),
        serverHash: new Uint8Array(16),
      };

      await assertRejects(
        () => encodeHandshakeRequest(handshake),
        RangeError,
        "clientHash must contain exactly 16 bytes",
      );
    });

    it("rejects invalid serverHash size", async () => {
      const handshake: HandshakeRequestInit = {
        clientHash: new Uint8Array(16),
        serverHash: new Uint8Array(17),
      };

      await assertRejects(
        () => encodeHandshakeRequest(handshake),
        RangeError,
        "serverHash must contain exactly 16 bytes",
      );
    });

    it("rejects invalid clientProtocol type", async () => {
      const handshake = {
        clientHash: new Uint8Array(16),
        serverHash: new Uint8Array(16),
        clientProtocol: 123,
      } as unknown as HandshakeRequestInit;

      await assertRejects(
        () => encodeHandshakeRequest(handshake),
        TypeError,
        "clientProtocol must be a string or null",
      );
    });
  });

  describe("encodeHandshakeResponse and decodeHandshakeResponse", () => {
    it("encodes and decodes with all fields", async () => {
      const handshake: HandshakeResponseInit = {
        match: "CLIENT",
        serverProtocol: "test protocol",
        serverHash: new Uint8Array(16).fill(9),
        meta: { server: new Uint8Array([9, 9]) },
      };

      const encoded = await encodeHandshakeResponse(handshake);
      const decoded = await decodeHandshakeResponse(toArrayBuffer(encoded));

      assertEquals(decoded.match, handshake.match);
      assertEquals(decoded.serverProtocol, handshake.serverProtocol);
      assertEquals(decoded.serverHash, handshake.serverHash);
      assertEquals(decoded.meta, new Map([["server", new Uint8Array([9, 9])]]));
    });

    it("encodes and decodes with null serverProtocol", async () => {
      const handshake: HandshakeResponseInit = {
        match: "BOTH",
      };

      const encoded = await encodeHandshakeResponse(handshake);
      const decoded = await decodeHandshakeResponse(toArrayBuffer(encoded));

      assertEquals(decoded.serverProtocol, null);
      assertEquals(decoded.serverHash, null);
    });

    it("encodes and decodes with null serverHash", async () => {
      const handshake: HandshakeResponseInit = {
        match: "NONE",
        serverProtocol: "protocol",
      };

      const encoded = await encodeHandshakeResponse(handshake);
      const decoded = await decodeHandshakeResponse(toArrayBuffer(encoded));

      assertEquals(decoded.serverHash, null);
    });

    it("encodes and decodes with null meta", async () => {
      const handshake: HandshakeResponseInit = {
        match: "CLIENT",
        meta: null,
      };

      const encoded = await encodeHandshakeResponse(handshake);
      const decoded = await decodeHandshakeResponse(toArrayBuffer(encoded));

      assertEquals(decoded.meta, null);
    });

    it("encodes and decodes with empty meta", async () => {
      const handshake: HandshakeResponseInit = {
        match: "NONE",
        meta: new Map(),
      };

      const encoded = await encodeHandshakeResponse(handshake);
      const decoded = await decodeHandshakeResponse(toArrayBuffer(encoded));

      assertEquals(decoded.meta, new Map());
    });

    it("rejects invalid serverHash size", async () => {
      const handshake: HandshakeResponseInit = {
        match: "CLIENT",
        serverHash: new Uint8Array(15),
      };

      await assertRejects(
        () => encodeHandshakeResponse(handshake),
        RangeError,
        "serverHash must contain exactly 16 bytes",
      );
    });

    it("rejects invalid serverProtocol type", async () => {
      const handshake = {
        match: "CLIENT",
        serverProtocol: {},
      } as unknown as HandshakeResponseInit;

      await assertRejects(
        () => encodeHandshakeResponse(handshake),
        TypeError,
        "serverProtocol must be a string or null",
      );
    });
  });

  describe("readHandshakeRequestFromTap", () => {
    it("reads from tap", async () => {
      const handshake: HandshakeRequestInit = {
        clientHash: new Uint8Array(16).fill(1),
        clientProtocol: "protocol",
        serverHash: new Uint8Array(16).fill(2),
        meta: { key: new Uint8Array([1]) },
      };

      const encoded = await encodeHandshakeRequest(handshake);
      const tap = new ReadableTap(toArrayBuffer(encoded));
      const decoded = await readHandshakeRequestFromTap(tap);

      assertEquals(decoded.clientHash, handshake.clientHash);
      assertEquals(decoded.clientProtocol, handshake.clientProtocol);
      assertEquals(decoded.serverHash, handshake.serverHash);
      assertEquals(decoded.meta, new Map([["key", new Uint8Array([1])]]));
    });
  });

  describe("readHandshakeResponseFromTap", () => {
    it("reads from tap", async () => {
      const handshake: HandshakeResponseInit = {
        match: "BOTH",
        serverProtocol: "protocol",
        serverHash: new Uint8Array(16).fill(3),
        meta: { key: new Uint8Array([2]) },
      };

      const encoded = await encodeHandshakeResponse(handshake);
      const tap = new ReadableTap(toArrayBuffer(encoded));
      const decoded = await readHandshakeResponseFromTap(tap);

      assertEquals(decoded.match, handshake.match);
      assertEquals(decoded.serverProtocol, handshake.serverProtocol);
      assertEquals(decoded.serverHash, handshake.serverHash);
      assertEquals(decoded.meta, new Map([["key", new Uint8Array([2])]]));
    });
  });

  describe("_extractOptionalString", () => {
    it("throws for invalid union", () => {
      assertThrows(
        () => _extractOptionalString({ invalid: "test" }),
        Error,
        "Unexpected union value",
      );
    });
  });

  describe("_extractOptionalMetadata", () => {
    it("throws for invalid union", () => {
      assertThrows(
        () => _extractOptionalMetadata({ invalid: new Map() }),
        Error,
        "Unexpected union value",
      );
    });
  });

  describe("_extractOptionalMd5", () => {
    it("throws for invalid union", () => {
      assertThrows(
        () => _extractOptionalMd5({ invalid: new Uint8Array(16) }),
        Error,
        "Unexpected union value",
      );
    });
  });
});
