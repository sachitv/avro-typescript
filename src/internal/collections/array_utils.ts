/** Concatenates an array of Uint8Arrays into a single Uint8Array. */
export function concatUint8Arrays(parts: Uint8Array[]): Uint8Array {
  if (parts.length === 0) {
    return new Uint8Array(0);
  }
  const length = parts.reduce((sum, part) => sum + part.length, 0);
  const result = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

/** Converts an ArrayBuffer or Uint8Array to a Uint8Array. */
export function toUint8Array(data: ArrayBuffer | Uint8Array): Uint8Array {
  return data instanceof Uint8Array ? data : new Uint8Array(data);
}
