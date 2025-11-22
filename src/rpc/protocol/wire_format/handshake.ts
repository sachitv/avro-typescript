import { createType } from "../../../type/create_type.ts";
import type { Type } from "../../../schemas/type.ts";
import type { ReadableTap } from "../../../serialization/tap.ts";
import {
  cloneBytes,
  cloneMetadataMap,
  type MetadataInit,
  type MetadataMap,
  toOptionalMetadataMap,
} from "./metadata.ts";

const handshakeRegistry = new Map<string, Type>();

const HANDSHAKE_REQUEST_TYPE = createType({
  namespace: "org.apache.avro.ipc",
  name: "HandshakeRequest",
  type: "record",
  fields: [
    {
      name: "clientHash",
      type: { name: "MD5", type: "fixed", size: 16 },
    },
    {
      name: "clientProtocol",
      type: ["null", "string"],
      default: null,
    },
    {
      name: "serverHash",
      type: "org.apache.avro.ipc.MD5",
    },
    {
      name: "meta",
      type: ["null", { type: "map", values: "bytes" }],
      default: null,
    },
  ],
}, { registry: handshakeRegistry });

const HANDSHAKE_RESPONSE_TYPE = createType({
  namespace: "org.apache.avro.ipc",
  name: "HandshakeResponse",
  type: "record",
  fields: [
    {
      name: "match",
      type: {
        name: "HandshakeMatch",
        type: "enum",
        symbols: ["BOTH", "CLIENT", "NONE"],
      },
    },
    {
      name: "serverProtocol",
      type: ["null", "string"],
      default: null,
    },
    {
      name: "serverHash",
      type: [
        "null",
        "org.apache.avro.ipc.MD5",
      ],
      default: null,
    },
    {
      name: "meta",
      type: ["null", { type: "map", values: "bytes" }],
      default: null,
    },
  ],
}, { registry: handshakeRegistry });

const STRING_BRANCH = "string";
const MAP_BRANCH = "map";
const MD5_BRANCH = "org.apache.avro.ipc.MD5";

/**
 * Represents the match status between client and server protocols in the Avro RPC handshake.
 * - "BOTH": Both client and server protocols match.
 * - "CLIENT": Only the client protocol is compatible.
 * - "NONE": Neither protocol matches.
 */
export type HandshakeMatch = "BOTH" | "CLIENT" | "NONE";

interface HandshakeRequestRecord {
  clientHash: Uint8Array;
  clientProtocol: null | Record<string, string>;
  serverHash: Uint8Array;
  meta: null | Record<string, MetadataMap>;
}

interface HandshakeResponseRecord {
  match: HandshakeMatch;
  serverProtocol: null | Record<string, string>;
  serverHash: null | Record<string, Uint8Array>;
  meta: null | Record<string, MetadataMap>;
}

/**
 * Initialization interface for a handshake request in the Avro RPC protocol.
 * Used to construct the initial handshake message sent by the client.
 */
export interface HandshakeRequestInit {
  /** MD5 hash of the client's Avro protocol schema. */
  clientHash: Uint8Array;
  /** Optional string representation of the client's Avro protocol schema. */
  clientProtocol?: string | null;
  /** MD5 hash of the server's Avro protocol schema that the client expects. */
  serverHash: Uint8Array;
  /** Optional metadata map for additional handshake information. */
  meta?: MetadataInit | null;
}

/**
 * The processed handshake request message in the Avro RPC protocol.
 * Contains the resolved fields after decoding or reading from a stream.
 */
export interface HandshakeRequestMessage {
  /** MD5 hash of the client's Avro protocol schema. */
  clientHash: Uint8Array;
  /** String representation of the client's Avro protocol schema, or null if not provided. */
  clientProtocol: string | null;
  /** MD5 hash of the server's Avro protocol schema that the client expects. */
  serverHash: Uint8Array;
  /** Metadata map for additional handshake information, or null if not provided. */
  meta: MetadataMap | null;
}

/**
 * Initialization interface for a handshake response in the Avro RPC protocol.
 * Used to construct the response message sent by the server.
 */
export interface HandshakeResponseInit {
  /** The match status indicating compatibility between client and server protocols. */
  match: HandshakeMatch;
  /** Optional string representation of the server's Avro protocol schema. */
  serverProtocol?: string | null;
  /** Optional MD5 hash of the server's Avro protocol schema. */
  serverHash?: Uint8Array | null;
  /** Optional metadata map for additional handshake information. */
  meta?: MetadataInit | null;
}

/**
 * The processed handshake response message in the Avro RPC protocol.
 * Contains the resolved fields after decoding or reading from a stream.
 */
export interface HandshakeResponseMessage {
  /** The match status indicating compatibility between client and server protocols. */
  match: HandshakeMatch;
  /** String representation of the server's Avro protocol schema, or null if not provided. */
  serverProtocol: string | null;
  /** MD5 hash of the server's Avro protocol schema, or null if not provided. */
  serverHash: Uint8Array | null;
  /** Metadata map for additional handshake information, or null if not provided. */
  meta: MetadataMap | null;
}

// @internal
function _assertMd5Size(value: Uint8Array, field: string): void {
  if (value.length !== 16) {
    throw new RangeError(`${field} must contain exactly 16 bytes.`);
  }
}

// @internal
function _wrapUnion<T>(branch: string, value: T): Record<string, T> {
  return { [branch]: value };
}

// @internal
function _createHandshakeRequestRecord(
  message: HandshakeRequestInit | HandshakeRequestMessage,
): HandshakeRequestRecord {
  const clientHash = cloneBytes(message.clientHash, "clientHash");
  _assertMd5Size(clientHash, "clientHash");

  const serverHash = cloneBytes(message.serverHash, "serverHash");
  _assertMd5Size(serverHash, "serverHash");

  const clientProtocol = message.clientProtocol ?? null;
  let clientProtocolUnion: HandshakeRequestRecord["clientProtocol"];
  if (clientProtocol === null) {
    clientProtocolUnion = null;
  } else if (typeof clientProtocol === "string") {
    clientProtocolUnion = _wrapUnion(STRING_BRANCH, clientProtocol);
  } else {
    throw new TypeError("clientProtocol must be a string or null.");
  }

  const meta = toOptionalMetadataMap(message.meta);
  const metaUnion = meta === null ? null : _wrapUnion(MAP_BRANCH, meta);

  return {
    clientHash,
    clientProtocol: clientProtocolUnion,
    serverHash,
    meta: metaUnion,
  };
}

// @internal
function _createHandshakeResponseRecord(
  message: HandshakeResponseInit | HandshakeResponseMessage,
): HandshakeResponseRecord {
  const serverHash = message.serverHash ?? null;
  let serverHashUnion: HandshakeResponseRecord["serverHash"];
  if (serverHash === null) {
    serverHashUnion = null;
  } else {
    const hashBytes = cloneBytes(serverHash, "serverHash");
    _assertMd5Size(hashBytes, "serverHash");
    serverHashUnion = _wrapUnion(MD5_BRANCH, hashBytes);
  }

  const serverProtocol = message.serverProtocol ?? null;
  let serverProtocolUnion: HandshakeResponseRecord["serverProtocol"];
  if (serverProtocol === null) {
    serverProtocolUnion = null;
  } else if (typeof serverProtocol === "string") {
    serverProtocolUnion = _wrapUnion(STRING_BRANCH, serverProtocol);
  } else {
    throw new TypeError("serverProtocol must be a string or null.");
  }

  const meta = toOptionalMetadataMap(message.meta);
  const metaUnion = meta === null ? null : _wrapUnion(MAP_BRANCH, meta);

  return {
    match: message.match,
    serverProtocol: serverProtocolUnion,
    serverHash: serverHashUnion,
    meta: metaUnion,
  };
}

/**
 * Extracts an optional string value from an Avro union type in handshake messages.
 * Handles the union of null and string, returning the string if present or null.
 */
export function _extractOptionalString(
  unionValue: null | Record<string, string>,
): string | null {
  if (unionValue === null) {
    return null;
  }
  if (unionValue[STRING_BRANCH] !== undefined) {
    return unionValue[STRING_BRANCH];
  }
  throw new Error("Unexpected union value for string branch.");
}

/**
 * Extracts an optional metadata map from an Avro union type in handshake messages.
 * Handles the union of null and map, returning a cloned metadata map if present or null.
 */
export function _extractOptionalMetadata(
  unionValue: null | Record<string, MetadataMap>,
): MetadataMap | null {
  if (unionValue === null) {
    return null;
  }
  const map = unionValue[MAP_BRANCH];
  if (map === undefined) {
    throw new Error("Unexpected union value for metadata branch.");
  }
  return cloneMetadataMap(map);
}

/**
 * Extracts an optional MD5 hash from an Avro union type in handshake messages.
 * Handles the union of null and MD5 fixed type, returning cloned bytes if present or null.
 */
export function _extractOptionalMd5(
  unionValue: null | Record<string, Uint8Array>,
): Uint8Array | null {
  if (unionValue === null) {
    return null;
  }
  const value = unionValue[MD5_BRANCH];
  if (value === undefined) {
    throw new Error("Unexpected union value for MD5 branch.");
  }
  return cloneBytes(value, "serverHash");
}

// @internal
function _toHandshakeRequestMessage(
  record: HandshakeRequestRecord,
): HandshakeRequestMessage {
  return {
    clientHash: cloneBytes(record.clientHash, "clientHash"),
    clientProtocol: _extractOptionalString(record.clientProtocol),
    serverHash: cloneBytes(record.serverHash, "serverHash"),
    meta: _extractOptionalMetadata(record.meta),
  };
}

// @internal
function _toHandshakeResponseMessage(
  record: HandshakeResponseRecord,
): HandshakeResponseMessage {
  return {
    match: record.match,
    serverProtocol: _extractOptionalString(record.serverProtocol),
    serverHash: _extractOptionalMd5(record.serverHash),
    meta: _extractOptionalMetadata(record.meta),
  };
}

/**
 * Encodes a handshake request message into a byte buffer for transmission in the Avro RPC protocol.
 * Serializes the request using the predefined Avro schema for handshake requests.
 */
export async function encodeHandshakeRequest(
  message: HandshakeRequestInit,
): Promise<Uint8Array> {
  const record = _createHandshakeRequestRecord(message);
  return new Uint8Array(await HANDSHAKE_REQUEST_TYPE.toBuffer(record));
}

/**
 * Decodes a byte buffer into a handshake request message in the Avro RPC protocol.
 * Deserializes the buffer using the predefined Avro schema for handshake requests.
 */
export async function decodeHandshakeRequest(
  buffer: ArrayBuffer,
): Promise<HandshakeRequestMessage> {
  const record = await HANDSHAKE_REQUEST_TYPE.fromBuffer(
    buffer,
  ) as HandshakeRequestRecord;
  return _toHandshakeRequestMessage(record);
}

/**
 * Reads a handshake request message from a readable tap stream in the Avro RPC protocol.
 * Deserializes the data from the stream using the predefined Avro schema for handshake requests.
 */
export async function readHandshakeRequestFromTap(
  tap: ReadableTap,
): Promise<HandshakeRequestMessage> {
  const record = await HANDSHAKE_REQUEST_TYPE.read(
    tap,
  ) as HandshakeRequestRecord;
  return _toHandshakeRequestMessage(record);
}

/**
 * Encodes a handshake response message into a byte buffer for transmission in the Avro RPC protocol.
 * Serializes the response using the predefined Avro schema for handshake responses.
 */
export async function encodeHandshakeResponse(
  message: HandshakeResponseInit,
): Promise<Uint8Array> {
  const record = _createHandshakeResponseRecord(message);
  return new Uint8Array(await HANDSHAKE_RESPONSE_TYPE.toBuffer(record));
}

/**
 * Decodes a byte buffer into a handshake response message in the Avro RPC protocol.
 * Deserializes the buffer using the predefined Avro schema for handshake responses.
 */
export async function decodeHandshakeResponse(
  buffer: ArrayBuffer,
): Promise<HandshakeResponseMessage> {
  const record = await HANDSHAKE_RESPONSE_TYPE.fromBuffer(
    buffer,
  ) as HandshakeResponseRecord;
  return _toHandshakeResponseMessage(record);
}

/**
 * Reads a handshake response message from a readable tap stream in the Avro RPC protocol.
 * Deserializes the data from the stream using the predefined Avro schema for handshake responses.
 */
export async function readHandshakeResponseFromTap(
  tap: ReadableTap,
): Promise<HandshakeResponseMessage> {
  const record = await HANDSHAKE_RESPONSE_TYPE.read(
    tap,
  ) as HandshakeResponseRecord;
  return _toHandshakeResponseMessage(record);
}
