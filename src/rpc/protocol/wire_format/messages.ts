import { createType } from "../../../type/create_type.ts";
import type { Type } from "../../../schemas/type.ts";
import { ReadableTap } from "../../../serialization/tap.ts";
import { concatUint8Arrays } from "../../../internal/collections/array_utils.ts";
import {
  cloneMetadataMap,
  type MetadataInit,
  type MetadataMap,
  toRequiredMetadataMap,
} from "./metadata.ts";
import {
  encodeHandshakeRequest,
  encodeHandshakeResponse,
  type HandshakeRequestInit,
  type HandshakeRequestMessage,
  type HandshakeResponseInit,
  type HandshakeResponseMessage,
  readHandshakeRequestFromTap,
  readHandshakeResponseFromTap,
} from "./handshake.ts";

const MAP_OF_BYTES_TYPE = createType({
  type: "map",
  values: "bytes",
});

const STRING_TYPE = createType("string");
const BOOLEAN_TYPE = createType("boolean");

async function readOptionalHandshake<T>(
  buffer: ArrayBuffer,
  reader: (tap: ReadableTap) => Promise<T>,
  expectHandshake: boolean,
): Promise<{ handshake?: T; tap: ReadableTap }> {
  const tap = new ReadableTap(buffer);
  if (expectHandshake) {
    const handshake = await reader(tap);
    return { handshake, tap };
  }
  const initialPos = tap.getPos();
  try {
    const handshake = await reader(tap);
    return { handshake, tap };
  } catch {
    return { tap: new ReadableTap(buffer, initialPos) };
  }
}

/**
 * Initialization object for encoding a call request in the Avro RPC wire format.
 * Contains the data needed to construct a call request message, including optional handshake,
 * metadata, message name, request payload, and the Avro type of the request.
 */
export interface CallRequestInit<T> {
  /** Optional handshake request data for protocol negotiation in the Avro RPC wire format. */
  handshake?: HandshakeRequestInit | HandshakeRequestMessage;
  /** Optional metadata map or initialization object for additional message metadata. */
  metadata?: MetadataInit | null;
  /** The name of the RPC message being called. */
  messageName: string;
  /** The request payload data to be sent. */
  request: T;
  /** The Avro type definition for the request payload. */
  requestType: Type<T>;
}

/**
 * Represents a decoded call request message in the Avro RPC wire format.
 * Contains the parsed handshake (if present), metadata, message name, and request payload.
 */
export interface CallRequestMessage<T> {
  /** Optional decoded handshake request message for protocol negotiation. */
  handshake?: HandshakeRequestMessage;
  /** Metadata map containing additional message information. */
  metadata: MetadataMap;
  /** The name of the RPC message being called. */
  messageName: string;
  /** The decoded request payload data. */
  request: T;
}

/**
 * Options for decoding a call request message from the Avro RPC wire format.
 * Specifies the expected request type and whether a handshake is expected.
 */
export interface DecodeCallRequestOptions<T> {
  /** The Avro type definition for the request payload. */
  requestType: Type<T>;
  /** Whether to expect and parse handshake data in the message. */
  expectHandshake?: boolean;
}

/**
 * Envelope for a call request message in the Avro RPC wire format.
 * Contains the handshake (if present), metadata, message name, and a tap for reading the request body.
 */
export interface CallRequestEnvelope {
  /** Optional decoded handshake request message. */
  handshake?: HandshakeRequestMessage;
  /** Metadata map with message metadata. */
  metadata: MetadataMap;
  /** The name of the RPC message. */
  messageName: string;
  /** Readable tap positioned at the start of the request payload for lazy reading. */
  bodyTap: ReadableTap;
}

/**
 * Initialization object for encoding a call response in the Avro RPC wire format.
 * Contains the data needed to construct a call response message, including optional handshake,
 * metadata, error flag, payload, and the Avro types for response and error.
 */
export interface CallResponseInit<TResponse, TError> {
  /** Optional handshake response data for protocol negotiation. */
  handshake?: HandshakeResponseInit | HandshakeResponseMessage;
  /** Optional metadata map or initialization object. */
  metadata?: MetadataInit | null;
  /** Flag indicating whether this response represents an error. */
  isError: boolean;
  /** The response or error payload data. */
  payload: TResponse | TError;
  /** The Avro type definition for the response payload. */
  responseType: Type<TResponse>;
  /** The Avro type definition for the error payload. */
  errorType: Type<TError>;
}

/**
 * Represents a decoded call response message in the Avro RPC wire format.
 * Contains the parsed handshake (if present), metadata, error flag, and payload.
 */
export interface CallResponseMessage<TResponse, TError> {
  /** Optional decoded handshake response message. */
  handshake?: HandshakeResponseMessage;
  /** Metadata map containing additional message information. */
  metadata: MetadataMap;
  /** Flag indicating whether this response represents an error. */
  isError: boolean;
  /** The decoded response or error payload data. */
  payload: TResponse | TError;
}

/**
 * Options for decoding a call response message from the Avro RPC wire format.
 * Specifies the expected response and error types, and whether a handshake is expected.
 */
export interface DecodeCallResponseOptions<TResponse, TError> {
  /** The Avro type definition for the response payload. */
  responseType: Type<TResponse>;
  /** The Avro type definition for the error payload. */
  errorType: Type<TError>;
  /** Whether to expect and parse handshake data in the message. */
  expectHandshake?: boolean;
}

/**
 * Envelope for a call response message in the Avro RPC wire format.
 * Contains the handshake (if present), metadata, error flag, and a tap for reading the response body.
 */
export interface CallResponseEnvelope {
  /** Optional decoded handshake response message. */
  handshake?: HandshakeResponseMessage;
  /** Metadata map with message metadata. */
  metadata: MetadataMap;
  /** Flag indicating whether this response represents an error. */
  isError: boolean;
  /** Readable tap positioned at the start of the response or error payload for lazy reading. */
  bodyTap: ReadableTap;
}

/**
 * Encodes a call request message into a binary format for transmission.
 *
 * The encoded message includes optional handshake data, metadata, message name,
 * and the request payload. The format follows the Avro RPC wire protocol.
 *
 * @param init - The call request initialization object containing handshake,
 *               metadata, message name, request data, and request type.
 * @returns A promise that resolves to the encoded message as a Uint8Array.
 */
export async function encodeCallRequest<T>(
  init: CallRequestInit<T>,
): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];

  if (init.handshake) {
    parts.push(await encodeHandshakeRequest(init.handshake));
  }

  const metadata = toRequiredMetadataMap(init.metadata);
  parts.push(new Uint8Array(await MAP_OF_BYTES_TYPE.toBuffer(metadata)));
  parts.push(new Uint8Array(await STRING_TYPE.toBuffer(init.messageName)));
  parts.push(new Uint8Array(await init.requestType.toBuffer(init.request)));

  return concatUint8Arrays(parts);
}

/**
 * Decodes a binary call request message into a structured object.
 *
 * This function parses the complete message including handshake (if expected),
 * metadata, message name, and request payload.
 *
 * @param buffer - The binary data containing the encoded call request.
 * @param options - Decoding options including the request type and whether
 *                  to expect handshake data.
 * @returns A promise that resolves to the decoded call request message.
 */
export async function decodeCallRequest<T>(
  buffer: ArrayBuffer,
  options: DecodeCallRequestOptions<T>,
): Promise<CallRequestMessage<T>> {
  const { requestType, expectHandshake = false } = options;
  const envelope = await decodeCallRequestEnvelope(buffer, {
    expectHandshake,
  });
  const request = await requestType.read(envelope.bodyTap);

  return {
    handshake: envelope.handshake,
    metadata: envelope.metadata,
    messageName: envelope.messageName,
    request,
  };
}

/**
 * Decodes the envelope of a call request message without parsing the request payload.
 *
 * This function is useful for inspecting message metadata and headers before
 * deciding whether to parse the full request payload. The request data remains
 * in the bodyTap for later reading.
 *
 * @param buffer - The binary data containing the encoded call request.
 * @param options - Options specifying whether to expect handshake data.
 * @returns A promise that resolves to the call request envelope.
 */
export async function decodeCallRequestEnvelope(
  buffer: ArrayBuffer,
  options: { expectHandshake?: boolean } = {},
): Promise<CallRequestEnvelope> {
  const { expectHandshake = false } = options;

  const { handshake: handshakeResult, tap: handshakeTap } =
    await readOptionalHandshake(
      buffer,
      readHandshakeRequestFromTap,
      expectHandshake,
    );
  const handshake = handshakeResult;
  const tap = handshakeTap;

  const metadataMap = await MAP_OF_BYTES_TYPE.read(tap) as MetadataMap;
  const metadata = cloneMetadataMap(metadataMap);
  const messageName = await STRING_TYPE.read(tap) as string;

  return {
    handshake,
    metadata,
    messageName,
    bodyTap: tap,
  };
}

/**
 * Encodes a call response message into a binary format for transmission.
 *
 * The encoded message includes optional handshake data, metadata, error flag,
 * and either the response payload or error payload based on the isError flag.
 *
 * @param init - The call response initialization object containing handshake,
 *               metadata, error flag, payload, and response/error types.
 * @returns A promise that resolves to the encoded message as a Uint8Array.
 */
export async function encodeCallResponse<TResponse, TError>(
  init: CallResponseInit<TResponse, TError>,
): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];

  if (init.handshake) {
    parts.push(await encodeHandshakeResponse(init.handshake));
  }

  const metadata = toRequiredMetadataMap(init.metadata);
  parts.push(new Uint8Array(await MAP_OF_BYTES_TYPE.toBuffer(metadata)));
  parts.push(new Uint8Array(await BOOLEAN_TYPE.toBuffer(init.isError)));

  if (init.isError) {
    parts.push(
      new Uint8Array(await init.errorType.toBuffer(init.payload as TError)),
    );
  } else {
    parts.push(
      new Uint8Array(
        await init.responseType.toBuffer(init.payload as TResponse),
      ),
    );
  }

  return concatUint8Arrays(parts);
}

/**
 * Decodes a binary call response message into a structured object.
 *
 * This function parses the complete message including handshake (if expected),
 * metadata, error flag, and response or error payload based on the error flag.
 *
 * @param buffer - The binary data containing the encoded call response.
 * @param options - Decoding options including response and error types, and
 *                  whether to expect handshake data.
 * @returns A promise that resolves to the decoded call response message.
 */
export async function decodeCallResponse<TResponse, TError>(
  buffer: ArrayBuffer,
  options: DecodeCallResponseOptions<TResponse, TError>,
): Promise<CallResponseMessage<TResponse, TError>> {
  const { responseType, errorType, expectHandshake = false } = options;
  const envelope = await decodeCallResponseEnvelope(buffer, {
    expectHandshake,
  });
  const payload = envelope.isError
    ? await errorType.read(envelope.bodyTap)
    : await responseType.read(envelope.bodyTap);

  return {
    handshake: envelope.handshake,
    metadata: envelope.metadata,
    isError: envelope.isError,
    payload,
  };
}

/**
 * Decodes the envelope of a call response message without parsing the response payload.
 *
 * This function is useful for inspecting message metadata and error status before
 * deciding whether to parse the full response or error payload. The payload data
 * remains in the bodyTap for later reading.
 *
 * @param buffer - The binary data containing the encoded call response.
 * @param options - Options specifying whether to expect handshake data.
 * @returns A promise that resolves to the call response envelope.
 */
export async function decodeCallResponseEnvelope(
  buffer: ArrayBuffer,
  options: { expectHandshake?: boolean } = {},
): Promise<CallResponseEnvelope> {
  const { expectHandshake = false } = options;

  const { handshake: handshakeResult, tap: handshakeTap } =
    await readOptionalHandshake(
      buffer,
      readHandshakeResponseFromTap,
      expectHandshake,
    );
  const handshake: HandshakeResponseMessage | undefined = handshakeResult;
  const tap = handshakeTap;

  const metadataMap = await MAP_OF_BYTES_TYPE.read(tap) as MetadataMap;
  const metadata = cloneMetadataMap(metadataMap);
  const isError = await BOOLEAN_TYPE.read(tap) as boolean;

  return {
    handshake,
    metadata,
    isError,
    bodyTap: tap,
  };
}
