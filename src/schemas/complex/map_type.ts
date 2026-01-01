import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/tap_sync.ts";
import { bigIntToSafeNumber } from "../../serialization/conversion.ts";
import { BaseType } from "../base_type.ts";
import { Resolver } from "../resolver.ts";
import type { JSONType, Type } from "../type.ts";
import type { ErrorHook } from "../error.ts";

/**
 * Parameters for creating a MapType.
 */
export interface MapTypeParams<T> {
  /** The type of values in the map. */
  values: Type<T>;
  /** Whether to validate during writes. Defaults to true. */
  validate?: boolean;
}

/**
 * Helper function to read a map from a tap.
 * @param tap The tap to read from.
 * @param readValue Function to read a single value.
 * @param collect Function to collect each key-value pair.
 */
export async function readMapInto<T>(
  tap: ReadableTapLike,
  readValue: (tap: ReadableTapLike) => Promise<T>,
  collect: (key: string, value: T) => void,
): Promise<void> {
  while (true) {
    let rawCount = await tap.readLong();
    if (rawCount === 0n) {
      break;
    }
    if (rawCount < 0n) {
      rawCount = -rawCount;
      await tap.skipLong(); // skip block size
    }
    const count = bigIntToSafeNumber(rawCount, "Map block length");
    for (let i = 0; i < count; i++) {
      const key = await tap.readString();
      const value = await readValue(tap);
      collect(key, value);
    }
  }
}

/**
 * Synchronous helper function to read a map from a tap.
 * @param tap The tap to read from.
 * @param readValue Function to read a single value.
 * @param collect Function to collect each key-value pair.
 */
export function readMapIntoSync<T>(
  tap: SyncReadableTapLike,
  readValue: (tap: SyncReadableTapLike) => T,
  collect: (key: string, value: T) => void,
): void {
  /**
   * Synchronously reads map blocks from the tap and populates the provided map.
   */
  while (true) {
    let rawCount = tap.readLong();
    if (rawCount === 0n) {
      break;
    }
    if (rawCount < 0n) {
      rawCount = -rawCount;
      tap.skipLong();
    }
    const count = bigIntToSafeNumber(rawCount, "Map block length");
    for (let i = 0; i < count; i++) {
      const key = tap.readString();
      const value = readValue(tap);
      collect(key, value);
    }
  }
}

/**
 * Avro `map` type for string-keyed collections of values matching a schema.
 */
export class MapType<T = unknown> extends BaseType<Map<string, T>> {
  readonly #valuesType: Type<T>;

  /**
   * Creates a new MapType.
   * @param params The map type parameters.
   */
  constructor(params: MapTypeParams<T>) {
    super(params.validate ?? true);
    if (!params.values) {
      throw new Error("MapType requires a values type.");
    }
    this.#valuesType = params.values;
  }

  /**
   * Gets the type of values in the map.
   */
  public getValuesType(): Type<T> {
    return this.#valuesType;
  }

  /**
   * Validates if the value is a valid map according to the schema.
   */
  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    if (!(value instanceof Map)) {
      if (errorHook) {
        errorHook(path.slice(), value, this);
      }
      return false;
    }

    let isValid = true;

    for (const [key, entry] of value.entries()) {
      if (typeof key !== "string") {
        if (errorHook) {
          errorHook(path.slice(), value, this);
          isValid = false;
          continue;
        }
        return false;
      }
      const entryPath = errorHook ? [...path, key] : undefined;
      const validEntry = this.#valuesType.check(entry, errorHook, entryPath);
      if (!validEntry) {
        if (!errorHook) {
          return false;
        }
        isValid = false;
      }
    }

    return isValid;
  }

  /**
   * Writes map without validation.
   */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: Map<string, T>,
  ): Promise<void> {
    if (value.size > 0) {
      // Use writeInt for block counts that fit in 32-bit range (avoids BigInt overhead)
      if (value.size <= 0x7FFFFFFF) {
        await tap.writeInt(value.size);
      } else {
        await tap.writeLong(BigInt(value.size));
      }
      for (const [key, entry] of value) {
        await tap.writeString(key);
        await this.#valuesType.writeUnchecked(tap, entry);
      }
    }
    // Terminal 0 marker always fits in an int
    await tap.writeInt(0);
  }

  /**
   * Writes map without validation synchronously.
   */
  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: Map<string, T>,
  ): void {
    if (value.size > 0) {
      // Use writeInt for block counts that fit in 32-bit range (avoids BigInt overhead)
      if (value.size <= 0x7FFFFFFF) {
        tap.writeInt(value.size);
      } else {
        tap.writeLong(BigInt(value.size));
      }
      for (const [key, entry] of value) {
        tap.writeString(key);
        this.#valuesType.writeSyncUnchecked(tap, entry);
      }
    }
    // Terminal 0 marker always fits in an int
    tap.writeInt(0);
  }

  /**
   * Reads a map value from the provided tap.
   */
  public override async read(
    tap: ReadableTapLike,
  ): Promise<Map<string, T>> {
    const result = new Map<string, T>();
    await readMapInto(
      tap,
      async (innerTap) => await this.#valuesType.read(innerTap),
      (key, value) => {
        result.set(key, value);
      },
    );
    return result;
  }

  /**
   * Reads the map from a sync tap without asynchronous operations.
   */
  /**
   * Resolves nested map entries synchronously using the value resolver.
   */
  public override readSync(tap: SyncReadableTapLike): Map<string, T> {
    const result = new Map<string, T>();
    readMapIntoSync(
      tap,
      (innerTap) => this.#valuesType.readSync(innerTap),
      (key, value) => {
        result.set(key, value);
      },
    );
    return result;
  }

  /**
   * Skips over a map value in the tap without reading it.
   * @param tap The readable tap to skip from.
   */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    while (true) {
      const rawCount = await tap.readLong();
      if (rawCount === 0n) {
        break;
      }
      if (rawCount < 0n) {
        const blockSize = await tap.readLong();
        const size = bigIntToSafeNumber(blockSize, "Map block size");
        if (size > 0) {
          await tap.skipFixed(size);
        }
        continue;
      }
      const count = bigIntToSafeNumber(rawCount, "Map block length");
      for (let i = 0; i < count; i++) {
        await tap.skipString();
        await this.#valuesType.skip(tap);
      }
    }
  }

  /**
   * Skips the encoded map blocks on the sync tap efficiently.
   */
  public override skipSync(tap: SyncReadableTapLike): void {
    while (true) {
      const rawCount = tap.readLong();
      if (rawCount === 0n) {
        break;
      }
      if (rawCount < 0n) {
        const blockSize = tap.readLong();
        const size = bigIntToSafeNumber(blockSize, "Map block size");
        if (size > 0) {
          tap.skipFixed(size);
        }
        continue;
      }
      const count = bigIntToSafeNumber(rawCount, "Map block length");
      for (let i = 0; i < count; i++) {
        tap.skipString();
        this.#valuesType.skipSync(tap);
      }
    }
  }

  /**
   * Creates a deep clone of the map from the given value.
   * @param value The value to clone, which can be a Map or a plain object.
   * @returns A new Map instance with cloned values.
   */
  public override cloneFromValue(value: unknown): Map<string, T> {
    const copy = new Map<string, T>();

    if (value instanceof Map) {
      for (const [key, entry] of value.entries()) {
        if (typeof key !== "string") {
          throw new Error("Map keys must be strings to clone.");
        }
        copy.set(key, this.#valuesType.cloneFromValue(entry));
      }
      return copy;
    }

    if (!isPlainObject(value)) {
      throw new Error("Cannot clone non-map value.");
    }

    for (const [key, entry] of Object.entries(value)) {
      copy.set(key, this.#valuesType.cloneFromValue(entry));
    }
    return copy;
  }

  /**
   * Compares two map values. Always throws an error as maps cannot be compared.
   * @param _val1 First map value.
   * @param _val2 Second map value.
   * @returns Never returns, always throws.
   * @throws Always throws an error.
   */
  public override compare(
    _val1: Map<string, T>,
    _val2: Map<string, T>,
  ): number {
    throw new Error("maps cannot be compared");
  }

  /**
   * Generates a random map value.
   * @returns A random Map with string keys and values of the map's value type.
   */
  public override random(): Map<string, T> {
    const result = new Map<string, T>();
    // There should be at least one entry.
    const entries = Math.ceil(Math.random() * 10);
    for (let i = 0; i < entries; i++) {
      const key = crypto.randomUUID();
      result.set(key, this.#valuesType.random());
    }
    return result;
  }

  /**
   * Returns the JSON schema representation of the map type.
   * @returns The JSON representation as JSONType.
   */
  public override toJSON(): JSONType {
    return {
      type: "map",
      values: this.#valuesType.toJSON(),
    };
  }

  /**
   * Compares two encoded buffers. Always throws an error as maps cannot be compared.
   * @param _tap1 The first tap.
   * @param _tap2 The second tap.
   * @returns Never returns, always throws.
   * @throws Always throws an error.
   */
  // deno-lint-ignore require-await
  public override async match(
    _tap1: ReadableTapLike,
    _tap2: ReadableTapLike,
  ): Promise<number> {
    throw new Error("maps cannot be compared");
  }

  /**
   * Raises for maps because they cannot be compared even in sync mode.
   */
  public override matchSync(
    _tap1: SyncReadableTapLike,
    _tap2: SyncReadableTapLike,
  ): number {
    throw new Error("maps cannot be compared");
  }

  /**
   * Creates a resolver for schema evolution from a writer type to this reader type.
   * @param writerType The writer schema type.
   * @returns A resolver for reading the writer type as this type.
   */
  public override createResolver(writerType: Type): Resolver {
    if (!(writerType instanceof MapType)) {
      return super.createResolver(writerType);
    }

    const valueResolver = this.#valuesType.createResolver(
      writerType.getValuesType(),
    ) as Resolver<T>;

    return new MapResolver(this, valueResolver);
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

class MapResolver<T> extends Resolver<Map<string, T>> {
  #valueResolver: Resolver<T>;

  constructor(reader: MapType<T>, valueResolver: Resolver<T>) {
    super(reader);
    this.#valueResolver = valueResolver;
  }

  public override async read(
    tap: ReadableTapLike,
  ): Promise<Map<string, T>> {
    const result = new Map<string, T>();
    await readMapInto(
      tap,
      async (innerTap) => await this.#valueResolver.read(innerTap),
      (key, value) => {
        result.set(key, value);
      },
    );
    return result;
  }

  public override readSync(tap: SyncReadableTapLike): Map<string, T> {
    const result = new Map<string, T>();
    readMapIntoSync(
      tap,
      (innerTap) => this.#valueResolver.readSync(innerTap),
      (key, value) => {
        result.set(key, value);
      },
    );
    return result;
  }
}
