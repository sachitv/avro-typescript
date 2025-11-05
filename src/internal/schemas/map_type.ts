import { Tap } from "../serialization/tap.ts";
import { encode } from "../serialization/text_encoding.ts";
import { bigIntToSafeNumber } from "../serialization/conversion.ts";
import { BaseType } from "./base_type.ts";
import { Resolver } from "./resolver.ts";
import { type JSONType, Type } from "./type.ts";
import { type ErrorHook, throwInvalidError } from "./error.ts";
import { calculateVarintSize } from "./varint.ts";

export interface MapTypeParams<T> {
  values: Type<T>;
}

export function readMapInto<T>(
  tap: Tap,
  readValue: (tap: Tap) => T,
  collect: (key: string, value: T) => void,
): void {
  while (true) {
    let rawCount = tap.readLong();
    if (rawCount === 0n) {
      break;
    }
    if (rawCount < 0n) {
      rawCount = -rawCount;
      tap.skipLong(); // skip block size
    }
    const count = bigIntToSafeNumber(rawCount, "Map block length");
    for (let i = 0; i < count; i++) {
      const key = tap.readString();
      if (key === undefined) {
        throw new Error("Insufficient data for map key");
      }
      const value = readValue(tap);
      collect(key, value);
    }
  }
}

export class MapType<T = unknown> extends BaseType<Map<string, T>> {
  readonly #valuesType: Type<T>;

  constructor(params: MapTypeParams<T>) {
    super();
    if (!params.values) {
      throw new Error("MapType requires a values type.");
    }
    this.#valuesType = params.values;
  }

  public getValuesType(): Type<T> {
    return this.#valuesType;
  }

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

  public override write(tap: Tap, value: Map<string, T>): void {
    if (!(value instanceof Map)) {
      throwInvalidError([], value, this);
    }

    if (value.size > 0) {
      tap.writeLong(BigInt(value.size));
      for (const [key, entry] of value) {
        if (typeof key !== "string") {
          throwInvalidError([], value, this);
        }
        tap.writeString(key);
        this.#valuesType.write(tap, entry);
      }
    }
    tap.writeLong(0n);
  }

  public override read(tap: Tap): Map<string, T> {
    const result = new Map<string, T>();
    readMapInto(
      tap,
      (innerTap) => this.#valuesType.read(innerTap),
      (key, value) => {
        result.set(key, value);
      },
    );
    return result;
  }

  public override skip(tap: Tap): void {
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
        this.#valuesType.skip(tap);
      }
    }
  }

  public override toBuffer(value: Map<string, T>): ArrayBuffer {
    if (!(value instanceof Map)) {
      throwInvalidError([], value, this);
    }

    const serializedEntries: Array<{
      keyBytes: Uint8Array;
      valueBytes: Uint8Array;
    }> = [];

    let totalSize = 1; // final zero block terminator

    for (const [key, entry] of value.entries()) {
      if (typeof key !== "string") {
        throwInvalidError([], value, this);
      }
      const keyBytes = encode(key);
      const valueBytes = new Uint8Array(this.#valuesType.toBuffer(entry));
      totalSize += calculateVarintSize(keyBytes.length) + keyBytes.length;
      totalSize += valueBytes.length;
      serializedEntries.push({ keyBytes, valueBytes });
    }

    if (serializedEntries.length > 0) {
      totalSize += calculateVarintSize(serializedEntries.length);
    }

    const buffer = new ArrayBuffer(totalSize);
    const tap = new Tap(buffer);

    if (serializedEntries.length > 0) {
      tap.writeLong(BigInt(serializedEntries.length));
      for (const { keyBytes, valueBytes } of serializedEntries) {
        tap.writeLong(BigInt(keyBytes.length));
        tap.writeFixed(keyBytes);
        tap.writeFixed(valueBytes);
      }
    }
    tap.writeLong(0n);

    return buffer;
  }

  public override clone(
    value: Map<string, T>,
    opts?: Record<string, unknown>,
  ): Map<string, T> {
    if (!(value instanceof Map)) {
      throw new Error("Cannot clone non-map value.");
    }
    const copy = new Map<string, T>();
    for (const [key, entry] of value.entries()) {
      if (typeof key !== "string") {
        throw new Error("Map keys must be strings to clone.");
      }
      copy.set(key, this.#valuesType.clone(entry, opts));
    }
    return copy;
  }

  public override compare(
    _val1: Map<string, T>,
    _val2: Map<string, T>,
  ): number {
    throw new Error("maps cannot be compared");
  }

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

  public override toJSON(): JSONType {
    return {
      type: "map",
      values: this.#valuesType.toJSON(),
    };
  }

  public override match(_tap1: Tap, _tap2: Tap): number {
    throw new Error("maps cannot be compared");
  }

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

class MapResolver<T> extends Resolver<Map<string, T>> {
  #valueResolver: Resolver<T>;

  constructor(reader: MapType<T>, valueResolver: Resolver<T>) {
    super(reader);
    this.#valueResolver = valueResolver;
  }

  public override read(tap: Tap): Map<string, T> {
    const result = new Map<string, T>();
    readMapInto(
      tap,
      (innerTap) => this.#valueResolver.read(innerTap),
      (key, value) => {
        result.set(key, value);
      },
    );
    return result;
  }
}
