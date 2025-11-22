/**
 * Represents a metadata value as a byte array in Avro RPC metadata.
 */
export type MetadataValue = Uint8Array;
/**
 * Initialization types for Avro RPC metadata, allowing various input formats
 * that can be converted to a MetadataMap.
 */
export type MetadataInit =
  | Map<string, MetadataValue>
  | Iterable<[string, MetadataValue]>
  | Record<string, MetadataValue>;

/**
 * A map of string keys to MetadataValue byte arrays, representing the standard
 * form of Avro RPC metadata.
 */
export type MetadataMap = Map<string, MetadataValue>;

/**
 * Checks if the given value is an iterable metadata initialization type,
 * useful for distinguishing between Map and Record inputs in Avro RPC metadata.
 */
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

/**
 * Clones a Uint8Array to ensure immutability of metadata values in Avro RPC.
 * Throws if the value is not a Uint8Array.
 */
export function cloneBytes(value: Uint8Array, field: string): Uint8Array {
  if (!(value instanceof Uint8Array)) {
    throw new TypeError(`${field} must be a Uint8Array.`);
  }
  return new Uint8Array(value);
}

/**
 * Converts a MetadataInit to a MetadataMap, cloning all byte values for
 * immutability in Avro RPC metadata handling.
 */
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

/**
 * Converts an optional MetadataInit to a MetadataMap or null, used for
 * optional metadata fields in Avro RPC messages.
 */
export function toOptionalMetadataMap(
  init: MetadataInit | undefined | null,
): MetadataMap | null {
  if (init === undefined || init === null) {
    return null;
  }
  return toMetadataMap(init);
}

/**
 * Converts an optional MetadataInit to a MetadataMap, defaulting to an empty
 * map if null or undefined, for required metadata in Avro RPC.
 */
export function toRequiredMetadataMap(
  init: MetadataInit | undefined | null,
): MetadataMap {
  if (init === undefined || init === null) {
    return new Map();
  }
  return toMetadataMap(init);
}

/**
 * Creates a deep clone of a MetadataMap, cloning all byte values to ensure
 * immutability in Avro RPC metadata operations.
 */
export function cloneMetadataMap(map: MetadataMap): MetadataMap {
  const clone = new Map<string, MetadataValue>();
  for (const [key, value] of map.entries()) {
    clone.set(key, cloneBytes(value, `Metadata value for '${key}'`));
  }
  return clone;
}
