import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../../serialization/tap.ts";
import { encode } from "../../serialization/text_encoding.ts";
import { bigIntToSafeNumber } from "../../serialization/conversion.ts";
import { BaseType } from "../base_type.ts";
import { Resolver } from "../resolver.ts";
import type { JSONType, Type } from "../type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";
import { calculateVarintSize } from "../../internal/varint.ts";

export interface MapTypeParams<T> {
  values: Type<T>;
}

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
      if (key === undefined) {
        throw new Error("Insufficient data for map key");
      }
      const value = await readValue(tap);
      collect(key, value);
    }
  }
}

/**
 * Avro `map` type for string-keyed collections of values matching a schema.
 */
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

  public override async write(
    tap: WritableTapLike,
    value: Map<string, T>,
  ): Promise<void> {
    if (!(value instanceof Map)) {
      throwInvalidError([], value, this);
    }

    if (value.size > 0) {
      await tap.writeLong(BigInt(value.size));
      for (const [key, entry] of value) {
        if (typeof key !== "string") {
          throwInvalidError([], value, this);
        }
        await tap.writeString(key);
        await this.#valuesType.write(tap, entry);
      }
    }
    await tap.writeLong(0n);
  }

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

  public override async toBuffer(value: Map<string, T>): Promise<ArrayBuffer> {
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
      const valueBytes = new Uint8Array(await this.#valuesType.toBuffer(entry));
      totalSize += calculateVarintSize(keyBytes.length) + keyBytes.length;
      totalSize += valueBytes.length;
      serializedEntries.push({ keyBytes, valueBytes });
    }

    if (serializedEntries.length > 0) {
      totalSize += calculateVarintSize(serializedEntries.length);
    }

    const buffer = new ArrayBuffer(totalSize);
    const tap = new WritableTap(buffer);

    if (serializedEntries.length > 0) {
      await tap.writeLong(BigInt(serializedEntries.length));
      for (const { keyBytes, valueBytes } of serializedEntries) {
        await tap.writeLong(BigInt(keyBytes.length));
        await tap.writeFixed(keyBytes);
        await tap.writeFixed(valueBytes);
      }
    }
    await tap.writeLong(0n);

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

  // deno-lint-ignore require-await
  public override async match(
    _tap1: ReadableTapLike,
    _tap2: ReadableTapLike,
  ): Promise<number> {
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
}
