import type { ProtocolLike } from "../../rpc/definitions/protocol_definitions.ts";
import type { MetadataInit } from "../../rpc/protocol/wire_format/metadata.ts";
import type { Message } from "../../rpc/definitions/message_definition.ts";

export const textEncoder = new TextEncoder();
export const textDecoder = new TextDecoder();

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

export function int32ToBytes(value: number): Uint8Array {
  const buffer = new ArrayBuffer(4);
  new DataView(buffer).setInt32(0, value, false);
  return new Uint8Array(buffer);
}

export function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

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

export function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(
    bytes.byteOffset,
    bytes.byteOffset + bytes.byteLength,
  ) as ArrayBuffer;
}

export function errorToPayload(err: unknown): { string: string } {
  if (err instanceof Error) {
    return { string: err.message };
  }
  if (typeof err === "string") {
    return { string: err };
  }
  return { string: String(err) };
}
