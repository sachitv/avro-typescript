import { assertEquals, assertRejects } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { createType } from "../../../../type/create_type.ts";
import type { Type } from "../../../../schemas/type.ts";
import {
  decodeCallRequest,
  decodeCallRequestEnvelope,
  decodeCallResponse,
  decodeCallResponseEnvelope,
  encodeCallRequest,
  encodeCallResponse,
} from "../messages.ts";
import type { CallRequestInit, CallResponseInit } from "../messages.ts";
import type {
  HandshakeRequestInit,
  HandshakeResponseInit,
} from "../handshake.ts";

function toArrayBuffer(data: Uint8Array): ArrayBuffer {
  return data.slice().buffer;
}

const MAP_OF_BYTES_TYPE = createType({
  type: "map",
  values: "bytes",
});

type PingRequest = { id: bigint };

describe("messages", () => {
  const requestType = createType({
    type: "record",
    name: "PingRequest",
    namespace: "org.example",
    fields: [
      { name: "id", type: "long" },
    ],
  }) as Type<PingRequest>;
  const responseType = createType("string") as Type<string>;
  const errorType = createType(["string"]) as Type<Record<string, unknown>>;

  it("encodes and decodes call requests with handshake", async () => {
    const handshake: HandshakeRequestInit = {
      clientHash: new Uint8Array(16).fill(1),
      clientProtocol: "{}",
      serverHash: new Uint8Array(16).fill(2),
    };
    const request: CallRequestInit<PingRequest> = {
      handshake,
      metadata: { correlation: new Uint8Array([7]) },
      messageName: "ping",
      request: { id: 123n },
      requestType,
    };

    const encoded = await encodeCallRequest(request);
    const decoded = await decodeCallRequest(toArrayBuffer(encoded), {
      requestType,
      expectHandshake: true,
    });

    assertEquals(decoded.handshake?.clientHash, handshake.clientHash);
    assertEquals(
      decoded.metadata,
      new Map([["correlation", new Uint8Array([7])]]),
    );
    assertEquals(decoded.messageName, "ping");
    assertEquals(decoded.request, { id: 123n });
  });

  it("auto-detects handshake when decoding requests without flag", async () => {
    const handshake: HandshakeRequestInit = {
      clientHash: new Uint8Array(16).fill(3),
      clientProtocol: "{}",
      serverHash: new Uint8Array(16).fill(4),
    };
    const request: CallRequestInit<PingRequest> = {
      handshake,
      metadata: { correlation: new Uint8Array([3]) },
      messageName: "ping",
      request: { id: 789n },
      requestType,
    };
    const encoded = await encodeCallRequest(request);
    const decoded = await decodeCallRequest(toArrayBuffer(encoded), {
      requestType,
    });
    const envelope = await decodeCallRequestEnvelope(toArrayBuffer(encoded));

    assertEquals(decoded.handshake?.clientHash, handshake.clientHash);
    assertEquals(envelope.handshake?.serverHash, handshake.serverHash);
    assertEquals(decoded.request, { id: 789n });
  });

  it("encodes and decodes call requests without handshake", async () => {
    const request: CallRequestInit<PingRequest> = {
      metadata: { correlation: new Uint8Array([8]) },
      messageName: "ping",
      request: { id: 456n },
      requestType,
    };

    const encoded = await encodeCallRequest(request);
    const decoded = await decodeCallRequest(toArrayBuffer(encoded), {
      requestType,
      expectHandshake: false,
    });

    assertEquals(decoded.handshake, undefined);
    assertEquals(
      decoded.metadata,
      new Map([["correlation", new Uint8Array([8])]]),
    );
    assertEquals(decoded.messageName, "ping");
    assertEquals(decoded.request, { id: 456n });
  });

  it("exposes call request envelopes for deferred decoding", async () => {
    const request: CallRequestInit<PingRequest> = {
      metadata: { correlation: new Uint8Array([9]) },
      messageName: "ping",
      request: { id: 321n },
      requestType,
    };

    const encoded = await encodeCallRequest(request);
    const envelope = await decodeCallRequestEnvelope(toArrayBuffer(encoded));
    const payload = await requestType.read(envelope.bodyTap);

    assertEquals(envelope.handshake, undefined);
    assertEquals(
      envelope.metadata,
      new Map([["correlation", new Uint8Array([9])]]),
    );
    assertEquals(envelope.messageName, "ping");
    assertEquals(payload, { id: 321n });
  });

  it("rejects call request envelopes with insufficient data for message name", async () => {
    const metadata = new Map<string, Uint8Array>();
    const metadataBytes = new Uint8Array(
      await MAP_OF_BYTES_TYPE.toBuffer(metadata),
    );
    const requestBuffer = new ArrayBuffer(metadataBytes.length);
    new Uint8Array(requestBuffer).set(metadataBytes);

    await assertRejects(
      () => decodeCallRequestEnvelope(requestBuffer),
      Error,
    );
  });

  it("encodes and decodes call responses with errors", async () => {
    const handshake: HandshakeResponseInit = {
      match: "BOTH",
      serverProtocol: "{}",
      serverHash: new Uint8Array(16).fill(9),
    };
    const response: CallResponseInit<string, Record<string, unknown>> = {
      handshake,
      metadata: new Map([["attempt", new Uint8Array([1])]]),
      isError: true,
      payload: { string: "boom" },
      responseType,
      errorType,
    };

    const encoded = await encodeCallResponse(response);
    const decoded = await decodeCallResponse(toArrayBuffer(encoded), {
      responseType,
      errorType,
      expectHandshake: true,
    });

    assertEquals(decoded.handshake?.match, "BOTH");
    assertEquals(decoded.metadata, new Map([["attempt", new Uint8Array([1])]]));
    assertEquals(decoded.isError, true);
    assertEquals(decoded.payload, { string: "boom" });
  });

  it("auto-detects handshake when decoding responses without flag", async () => {
    const handshake: HandshakeResponseInit = {
      match: "CLIENT",
      serverProtocol: "{}",
      serverHash: new Uint8Array(16).fill(5),
    };
    const response: CallResponseInit<string, Record<string, unknown>> = {
      handshake,
      metadata: new Map([["attempt", new Uint8Array([4])]]),
      isError: false,
      payload: "auto",
      responseType,
      errorType,
    };

    const encoded = await encodeCallResponse(response);
    const decoded = await decodeCallResponse(toArrayBuffer(encoded), {
      responseType,
      errorType,
    });
    const envelope = await decodeCallResponseEnvelope(toArrayBuffer(encoded));

    assertEquals(decoded.handshake?.match, handshake.match);
    assertEquals(envelope.handshake?.serverHash, handshake.serverHash);
    assertEquals(decoded.payload, "auto");
  });

  it("encodes and decodes call responses without handshake", async () => {
    const response: CallResponseInit<string, Record<string, unknown>> = {
      metadata: new Map([["attempt", new Uint8Array([3])]]),
      isError: false,
      payload: "pong",
      responseType,
      errorType,
    };

    const encoded = await encodeCallResponse(response);
    const decoded = await decodeCallResponse(toArrayBuffer(encoded), {
      responseType,
      errorType,
      expectHandshake: false,
    });

    assertEquals(decoded.handshake, undefined);
    assertEquals(decoded.metadata, new Map([["attempt", new Uint8Array([3])]]));
    assertEquals(decoded.isError, false);
    assertEquals(decoded.payload, "pong");
  });

  it("exposes call response envelopes for deferred decoding", async () => {
    const response: CallResponseInit<string, Record<string, unknown>> = {
      metadata: new Map([["attempt", new Uint8Array([2])]]),
      isError: false,
      payload: "pong",
      responseType,
      errorType,
    };

    const encoded = await encodeCallResponse(response);
    const envelope = await decodeCallResponseEnvelope(toArrayBuffer(encoded));
    const payload = await responseType.read(envelope.bodyTap);

    assertEquals(envelope.handshake, undefined);
    assertEquals(
      envelope.metadata,
      new Map([["attempt", new Uint8Array([2])]]),
    );
    assertEquals(envelope.isError, false);
    assertEquals(payload, "pong");
  });

  it("rejects call response envelopes with insufficient data for response flag", async () => {
    const metadata = new Map<string, Uint8Array>();
    const metadataBytes = new Uint8Array(
      await MAP_OF_BYTES_TYPE.toBuffer(metadata),
    );
    const responseBuffer = new ArrayBuffer(metadataBytes.length);
    new Uint8Array(responseBuffer).set(metadataBytes);

    await assertRejects(
      () => decodeCallResponseEnvelope(responseBuffer),
      Error,
    );
  });

  it("rejects metadata that is not bytes", async () => {
    const request: CallRequestInit<PingRequest> = {
      metadata: { invalid: "nope" as unknown as Uint8Array },
      messageName: "ping",
      request: { id: 1n },
      requestType,
    };

    await assertRejects(
      () => encodeCallRequest(request),
      TypeError,
      "Metadata value",
    );
  });
});
