import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import { bigIntToSafeNumber } from "../../serialization/conversion.ts";
import { BaseType } from "../base_type.ts";
import { Resolver } from "../resolver.ts";
import type { JSONType, Type } from "../type.ts";
import type { ErrorHook } from "../error.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/tap_sync.ts";
import type { DirectSyncReadableTap } from "../../serialization/direct_tap_sync.ts";

/**
 * Helper function to read an array from a tap.
 * @param tap The tap to read from.
 * @param readElement Function to read a single element.
 * @param collect Function to collect each read element.
 */
export async function readArrayInto<T>(
  tap: ReadableTapLike,
  readElement: (tap: ReadableTapLike) => Promise<T>,
  collect: (value: T) => void,
): Promise<void> {
  while (true) {
    const rawCount = await tap.readLong();
    if (rawCount === 0n) {
      break;
    }
    let count = bigIntToSafeNumber(rawCount, "Array block length");
    if (count < 0) {
      // Negative counts signal a size-prefixed block; absolute value is
      // the element count and the trailing long is the block's byte size.
      count = -count;
      await tap.skipLong();
    }
    for (let i = 0; i < count; i++) {
      collect(await readElement(tap));
    }
  }
}

/**
 * Synchronous helper function to read an array from a tap.
 * @param tap The tap to read from.
 * @param readElement Function to read a single element.
 * @param collect Function to collect each read element.
 */
export function readArrayIntoSync<T>(
  tap: SyncReadableTapLike,
  readElement: (tap: SyncReadableTapLike) => T,
  collect: (value: T) => void,
): void {
  /**
   * Reads repeated blocks from the tap synchronously and collects decoded elements.
   */
  while (true) {
    const rawCount = tap.readLong();
    if (rawCount === 0n) {
      break;
    }
    let count = bigIntToSafeNumber(rawCount, "Array block length");
    if (count < 0) {
      count = -count;
      tap.skipLong();
    }
    for (let i = 0; i < count; i++) {
      collect(readElement(tap));
    }
  }
}

/**
 * Parameters for creating an ArrayType.
 */
export interface ArrayTypeParams<T> {
  /** The type of items in the array. */
  items: Type<T>;
  /** Whether to validate during writes. Defaults to true. */
  validate?: boolean;
}

/**
 * Avro `array` type for homogeneous collections of items described by a schema.
 */
export class ArrayType<T = unknown> extends BaseType<T[]> {
  readonly #itemsType: Type<T>;
  #primitiveKind: string | null | undefined = undefined;

  /**
   * Creates a new ArrayType.
   * @param params The array type parameters.
   */
  constructor(params: ArrayTypeParams<T>) {
    super(params.validate ?? true);
    if (!params.items) {
      throw new Error("ArrayType requires an items type.");
    }
    this.#itemsType = params.items;
  }

  #getPrimitiveKind(): string | null {
    if (this.#primitiveKind === undefined) {
      const json = this.#itemsType.toJSON();
      if (
        typeof json === "string" &&
        (json === "int" || json === "long" || json === "float" ||
          json === "double" || json === "boolean" || json === "string")
      ) {
        this.#primitiveKind = json;
      } else {
        this.#primitiveKind = null;
      }
    }
    return this.#primitiveKind;
  }

  /**
   * Gets the type of items in the array.
   */
  public getItemsType(): Type<T> {
    return this.#itemsType;
  }

  /**
   * Overrides the base check method to validate array values.
   * @param value The value to check.
   * @param errorHook Optional error hook for validation errors.
   * @param path The current path in the schema.
   * @returns True if the value is a valid array of items, false otherwise.
   */
  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    if (!Array.isArray(value)) {
      if (errorHook) {
        errorHook(path.slice(), value, this);
      }
      return false;
    }

    let isValid = true;
    for (let i = 0; i < value.length; i++) {
      const element = value[i];
      // Optimization: Use push/pop instead of spread to avoid allocating
      // a new array for every element during recursion
      if (errorHook) {
        path.push(String(i));
      }
      const validElement = this.#itemsType.check(
        element,
        errorHook,
        path,
      );
      if (errorHook) {
        path.pop();
      }
      if (!validElement) {
        if (!errorHook) {
          return false;
        }
        isValid = false;
      }
    }
    return isValid;
  }

  /**
   * Writes array without validation.
   */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: T[],
  ): Promise<void> {
    if (value.length > 0) {
      // Use writeInt for block counts that fit in 32-bit range (avoids BigInt overhead)
      if (value.length <= 0x7FFFFFFF) {
        await tap.writeInt(value.length);
      } else {
        await tap.writeLong(BigInt(value.length));
      }
      for (const element of value) {
        await this.#itemsType.writeUnchecked(tap, element);
      }
    }
    // Terminal 0 marker always fits in an int
    await tap.writeInt(0);
  }

  /**
   * Writes array without validation synchronously.
   */
  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: T[],
  ): void {
    if (value.length > 0) {
      // Use writeInt for block counts that fit in 32-bit range (avoids BigInt overhead)
      if (value.length <= 0x7FFFFFFF) {
        tap.writeInt(value.length);
      } else {
        tap.writeLong(BigInt(value.length));
      }
      for (const element of value) {
        this.#itemsType.writeSyncUnchecked(tap, element);
      }
    }
    // Terminal 0 marker always fits in an int
    tap.writeInt(0);
  }

  /**
   * Overrides the base skip method to skip over an array in the tap.
   * @param tap The tap to skip from.
   */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    // Skip blocks until terminator.
    while (true) {
      const rawCount = await tap.readLong();
      if (rawCount === 0n) {
        break;
      }
      let count = bigIntToSafeNumber(rawCount, "Array block length");
      if (count < 0) {
        // Negative block count indicates that the next long is the total byte
        // length for the block. Taking the absolute value yields the element
        // count while the size lets us skip the entire block efficiently.
        count = -count;
        const blockSize = Number(await tap.readLong());
        if (blockSize > 0) {
          await tap.skipFixed(blockSize);
        }
      } else {
        for (let i = 0; i < count; i++) {
          await this.#itemsType.skip(tap);
        }
      }
    }
  }

  /**
   * Advances the sync tap past the encoded array without decoding elements.
   */
  public override skipSync(tap: SyncReadableTapLike): void {
    while (true) {
      const rawCount = tap.readLong();
      if (rawCount === 0n) {
        break;
      }
      let count = bigIntToSafeNumber(rawCount, "Array block length");
      if (count < 0) {
        count = -count;
        const blockSize = Number(tap.readLong());
        if (blockSize > 0) {
          tap.skipFixed(blockSize);
        }
      } else {
        for (let i = 0; i < count; i++) {
          this.#itemsType.skipSync(tap);
        }
      }
    }
  }

  /**
   * Overrides the base read method to deserialize an array.
   * @param tap The tap to read from.
   * @returns The deserialized array.
   */
  public override async read(tap: ReadableTapLike): Promise<T[]> {
    const result: T[] = [];
    await readArrayInto(
      tap,
      async (innerTap) => await this.#itemsType.read(innerTap),
      (value) => {
        result.push(value);
      },
    );
    return result;
  }

  /**
   * Reads the entire array synchronously from the tap.
   * Optimized: inlined loop, uses readInt() for block counts, pre-sizes array.
   */
  public override readSync(tap: SyncReadableTapLike): T[] {
    const primitiveKind = this.#getPrimitiveKind();
    if (
      primitiveKind !== null &&
      typeof (tap as DirectSyncReadableTap).readIntArrayInto === "function"
    ) {
      return this.#readSyncBulk(tap as DirectSyncReadableTap, primitiveKind);
    }

    const result: T[] = [];
    const itemsType = this.#itemsType;
    while (true) {
      let count = tap.readInt();
      if (count === 0) break;
      if (count < 0) {
        count = -count;
        tap.skipLong();
      }
      const startIdx = result.length;
      result.length = startIdx + count;
      for (let i = 0; i < count; i++) {
        result[startIdx + i] = itemsType.readSync(tap);
      }
    }
    return result;
  }

  #readSyncBulk(tap: DirectSyncReadableTap, kind: string): T[] {
    const result: unknown[] = [];
    while (true) {
      let count = tap.readInt();
      if (count === 0) break;
      if (count < 0) {
        count = -count;
        tap.skipLong();
      }
      const startIdx = result.length;
      result.length = startIdx + count;

      switch (kind) {
        case "int":
          tap.readIntArrayInto(result as number[], startIdx, count);
          break;
        case "long":
          tap.readLongArrayInto(result as bigint[], startIdx, count);
          break;
        case "float":
          tap.readFloatArrayInto(result as number[], startIdx, count);
          break;
        case "double":
          tap.readDoubleArrayInto(result as number[], startIdx, count);
          break;
        case "boolean":
          tap.readBooleanArrayInto(result as boolean[], startIdx, count);
          break;
        case "string":
          tap.readStringArrayInto(result as string[], startIdx, count);
          break;
      }
    }
    return result as T[];
  }

  /**
   * Overrides the base cloneFromValue method to clone an array.
   * @param value The value to clone.
   * @returns The cloned array.
   */
  public override cloneFromValue(value: unknown): T[] {
    if (!Array.isArray(value)) {
      throw new Error("Cannot clone non-array value.");
    }
    return value.map((element) => this.#itemsType.cloneFromValue(element));
  }

  /**
   * Overrides the base compare method to compare two arrays.
   * @param val1 The first array.
   * @param val2 The second array.
   * @returns Negative if val1 < val2, 0 if equal, positive if val1 > val2.
   */
  public override compare(val1: T[], val2: T[]): number {
    const len = Math.min(val1.length, val2.length);
    for (let i = 0; i < len; i++) {
      const comparison = this.#itemsType.compare(val1[i], val2[i]);
      if (comparison !== 0) {
        return comparison;
      }
    }
    if (val1.length === val2.length) {
      return 0;
    }
    return val1.length < val2.length ? -1 : 1;
  }

  /**
   * Overrides the base random method to generate a random array.
   * @returns A randomly generated array.
   */
  public override random(): T[] {
    // There should be at least one element.
    const length = Math.ceil(Math.random() * 10);
    const result: T[] = [];
    for (let i = 0; i < length; i++) {
      result.push(this.#itemsType.random());
    }
    return result;
  }

  /**
   * Returns the JSON schema representation of this array type.
   */
  public override toJSON(): JSONType {
    return {
      type: "array",
      items: this.#itemsType.toJSON(),
    };
  }

  /**
   * Compares two serialized arrays for ordering.
   * @param tap1 The first tap to compare.
   * @param tap2 The second tap to compare.
   * @returns A negative number if tap1 < tap2, zero if equal, positive if tap1 > tap2.
   */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    let n1 = await this.#readArraySize(tap1);
    let n2 = await this.#readArraySize(tap2);
    let f: number;
    while (n1 !== 0n && n2 !== 0n) {
      f = await this.#itemsType.match(tap1, tap2);
      if (f !== 0) {
        return f;
      }
      if (n1 > 0n) {
        n1--;
      }
      if (n1 === 0n) {
        n1 = await this.#readArraySize(tap1);
      }
      if (n2 > 0n) {
        n2--;
      }
      if (n2 === 0n) {
        n2 = await this.#readArraySize(tap2);
      }
    }
    if (n1 === n2) {
      return 0;
    }
    if (n1 < n2) {
      return -1;
    }
    return 1;
  }

  /**
   * Compares two sync taps that encode arrays for ordering.
   */
  public override matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    let n1 = this.#readArraySizeSync(tap1);
    let n2 = this.#readArraySizeSync(tap2);
    while (n1 !== 0n && n2 !== 0n) {
      const f = this.#itemsType.matchSync(tap1, tap2);
      if (f !== 0) {
        return f;
      }
      if (n1 > 0n) {
        n1--;
      }
      if (n1 === 0n) {
        n1 = this.#readArraySizeSync(tap1);
      }
      if (n2 > 0n) {
        n2--;
      }
      if (n2 === 0n) {
        n2 = this.#readArraySizeSync(tap2);
      }
    }
    if (n1 === n2) {
      return 0;
    }
    if (n1 < n2) {
      return -1;
    }
    return 1;
  }

  async #readArraySize(tap: ReadableTapLike): Promise<bigint> {
    let n = await tap.readLong();
    if (n < 0n) {
      n = -n;
      await tap.skipLong(); // skip size
    }
    return n;
  }

  /**
   * Reads the next block header synchronously, respecting size-prefixed markers.
   */
  #readArraySizeSync(tap: SyncReadableTapLike): bigint {
    let n = tap.readLong();
    if (n < 0n) {
      n = -n;
      tap.skipLong();
    }
    return n;
  }

  /**
   * Creates a resolver for schema evolution between array types.
   * @param writerType The writer's array type.
   * @returns A resolver for reading data written with the writer type.
   */
  public override createResolver(writerType: Type): Resolver {
    if (!(writerType instanceof ArrayType)) {
      return super.createResolver(writerType);
    }

    const itemResolver = this.#itemsType.createResolver(
      writerType.getItemsType(),
    ) as Resolver<T>;

    return new ArrayResolver<T>(this, itemResolver);
  }
}

class ArrayResolver<T> extends Resolver<T[]> {
  #itemResolver: Resolver<T>;

  constructor(
    reader: ArrayType<T>,
    itemResolver: Resolver<T>,
  ) {
    super(reader);
    this.#itemResolver = itemResolver;
  }

  public override async read(tap: ReadableTapLike): Promise<T[]> {
    const result: T[] = [];
    // Reuse the generic array-block reader so we respect negative block counts
    // (size-prefixed blocks) just like the base implementation.
    await readArrayInto(
      tap,
      async (innerTap) => await this.#itemResolver.read(innerTap),
      (value) => {
        result.push(value);
      },
    );
    return result;
  }

  public override readSync(tap: SyncReadableTapLike): T[] {
    const result: T[] = [];
    const itemResolver = this.#itemResolver;
    while (true) {
      let count = tap.readInt();
      if (count === 0) break;
      if (count < 0) {
        count = -count;
        tap.skipLong();
      }
      const startIdx = result.length;
      result.length = startIdx + count;
      for (let i = 0; i < count; i++) {
        result[startIdx + i] = itemResolver.readSync(tap);
      }
    }
    return result;
  }
}
