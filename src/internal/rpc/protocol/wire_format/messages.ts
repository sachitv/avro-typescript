import { createType } from "../../../createType/mod.ts";
import type { Type } from "../../../schemas/type.ts";
import { ReadableTap } from "../../../serialization/tap.ts";
import { concatUint8Arrays } from "../../../collections/array_utils.ts";
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

export interface CallRequestInit<T> {
  handshake?: HandshakeRequestInit | HandshakeRequestMessage;
  metadata?: MetadataInit | null;
  messageName: string;
  request: T;
  requestType: Type<T>;
}

export interface CallRequestMessage<T> {
  handshake?: HandshakeRequestMessage;
  metadata: MetadataMap;
  messageName: string;
  request: T;
}

export interface DecodeCallRequestOptions<T> {
  requestType: Type<T>;
  expectHandshake?: boolean;
}

export interface CallRequestEnvelope {
  handshake?: HandshakeRequestMessage;
  metadata: MetadataMap;
  messageName: string;
  bodyTap: ReadableTap;
}

export interface CallResponseInit<TResponse, TError> {
  handshake?: HandshakeResponseInit | HandshakeResponseMessage;
  metadata?: MetadataInit | null;
  isError: boolean;
  payload: TResponse | TError;
  responseType: Type<TResponse>;
  errorType: Type<TError>;
}

export interface CallResponseMessage<TResponse, TError> {
  handshake?: HandshakeResponseMessage;
  metadata: MetadataMap;
  isError: boolean;
  payload: TResponse | TError;
}

export interface DecodeCallResponseOptions<TResponse, TError> {
  responseType: Type<TResponse>;
  errorType: Type<TError>;
  expectHandshake?: boolean;
}

export interface CallResponseEnvelope {
  handshake?: HandshakeResponseMessage;
  metadata: MetadataMap;
  isError: boolean;
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
