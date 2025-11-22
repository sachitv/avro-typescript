import type { ProtocolLike } from "../../rpc/definitions/protocol_definitions.ts";
import type { MetadataInit } from "../../rpc/protocol/wire_format/metadata.ts";
import type { Message } from "../../rpc/definitions/message_definition.ts";

/** TextEncoder instance for encoding strings to UTF-8 bytes. */
export const textEncoder = new TextEncoder();
/** TextDecoder instance for decoding UTF-8 bytes to strings. */
export const textDecoder = new TextDecoder();

/**
 * Normalizes a protocol name by prepending the namespace if the name does not already contain a dot.
 * @param name The protocol name.
 * @param namespace Optional namespace to prepend.
 * @returns The normalized protocol name.
 */
export function normalizeProtocolName(
  name: string,
  namespace?: string,
): string {
  if (!name) {
    throw new Error("missing protocol name");
  }
  if (namespace && !name.includes(".")) {
    return `${namespace}.${name}`;
  }
  return name;
}

/**
 * Serializes a protocol to a JSON string containing the protocol name, types, and messages.
 * @param protocol The protocol to stringify.
 * @returns The JSON string representation of the protocol.
 */
export function stringifyProtocol(protocol: ProtocolLike): string {
  const namedTypes = protocol.getNamedTypes();
  const messages: Record<string, Message> = {};
  for (const [name, message] of protocol.getMessages()) {
    messages[name] = message;
  }
  // The types/messages already implement toJSON, so JSON.stringify will invoke
  // their serialization helpers automatically.
  return JSON.stringify({
    protocol: protocol.getName(),
    types: namedTypes.length ? namedTypes : undefined,
    messages,
  });
}

/**
 * Creates metadata with a given ID, optionally copying from a base metadata.
 * @param id The ID to set in the metadata.
 * @param base Optional base metadata to copy from.
 * @returns The new metadata with the ID.
 */
export function metadataWithId(
  id: number,
  base?: MetadataInit | null,
): MetadataInit {
  const map = new Map<string, Uint8Array>();
  if (base instanceof Map) {
    for (const [key, value] of base) {
      map.set(key, value.slice());
    }
  } else if (Array.isArray(base)) {
    for (const [key, value] of base) {
      map.set(key, value.slice());
    }
  } else if (base && typeof base === "object") {
    for (const [key, value] of Object.entries(base)) {
      map.set(key, (value as Uint8Array).slice());
    }
  }
  map.set("id", int32ToBytes(id));
  return map;
}

/**
 * Converts a 32-bit integer to a Uint8Array of 4 bytes in big-endian order.
 * @param value The integer value to convert.
 * @returns The Uint8Array representation of the integer.
 */
export function int32ToBytes(value: number): Uint8Array {
  const buffer = new ArrayBuffer(4);
  new DataView(buffer).setInt32(0, value, false);
  return new Uint8Array(buffer);
}

/**
 * Converts a Uint8Array to a hexadecimal string representation.
 * @param bytes The bytes to convert.
 * @returns The hexadecimal string.
 */
export function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Checks if two Uint8Arrays are equal by comparing their lengths and contents.
 * @param a The first Uint8Array.
 * @param b The second Uint8Array.
 * @returns True if the arrays are equal, false otherwise.
 */
export function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

/**
 * Converts a Uint8Array to an ArrayBuffer by slicing the underlying buffer.
 * @param bytes The Uint8Array to convert.
 * @returns The corresponding ArrayBuffer.
 */
export function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(
    bytes.byteOffset,
    bytes.byteOffset + bytes.byteLength,
  ) as ArrayBuffer;
}

/**
 * Converts an error or unknown value to a payload object with a string property.
 * @param err The error or value to convert.
 * @returns An object with a 'string' property containing the error message or string representation.
 */
export function errorToPayload(err: unknown): { string: string } {
  if (err instanceof Error) {
    return { string: err.message };
  }
  if (typeof err === "string") {
    return { string: err };
  }
  return { string: String(err) };
}
