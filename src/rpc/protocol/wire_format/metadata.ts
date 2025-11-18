export type MetadataValue = Uint8Array;
export type MetadataInit =
  | Map<string, MetadataValue>
  | Iterable<[string, MetadataValue]>
  | Record<string, MetadataValue>;

export type MetadataMap = Map<string, MetadataValue>;

export function isIterableMetadata(
  value: MetadataInit | undefined | null,
): value is Iterable<[string, MetadataValue]> {
  if (value === undefined || value === null) {
    return false;
  }
  if (value instanceof Map) {
    return true;
  }
  return typeof (value as Iterable<unknown>)[Symbol.iterator] === "function";
}

export function cloneBytes(value: Uint8Array, field: string): Uint8Array {
  if (!(value instanceof Uint8Array)) {
    throw new TypeError(`${field} must be a Uint8Array.`);
  }
  return new Uint8Array(value);
}

export function toMetadataMap(init: MetadataInit): MetadataMap {
  const map = new Map<string, MetadataValue>();
  if (isIterableMetadata(init)) {
    for (const [key, value] of init) {
      if (typeof key !== "string") {
        throw new TypeError("Metadata keys must be strings.");
      }
      map.set(key, cloneBytes(value, `Metadata value for '${key}'`));
    }
    return map;
  }
  for (const [key, value] of Object.entries(init)) {
    map.set(key, cloneBytes(value, `Metadata value for '${key}'`));
  }
  return map;
}

export function toOptionalMetadataMap(
  init: MetadataInit | undefined | null,
): MetadataMap | null {
  if (init === undefined || init === null) {
    return null;
  }
  return toMetadataMap(init);
}

export function toRequiredMetadataMap(
  init: MetadataInit | undefined | null,
): MetadataMap {
  if (init === undefined || init === null) {
    return new Map();
  }
  return toMetadataMap(init);
}

export function cloneMetadataMap(map: MetadataMap): MetadataMap {
  const clone = new Map<string, MetadataValue>();
  for (const [key, value] of map.entries()) {
    clone.set(key, cloneBytes(value, `Metadata value for '${key}'`));
  }
  return clone;
}
