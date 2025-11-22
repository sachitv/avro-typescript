import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../../serialization/tap.ts";
import { bigIntToSafeNumber } from "../../serialization/conversion.ts";
import { BaseType } from "../base_type.ts";
import { Resolver } from "../resolver.ts";
import type { JSONType, Type } from "../type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";
import { calculateVarintSize } from "../../internal/varint.ts";

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
 * Parameters for creating an ArrayType.
 */
export interface ArrayTypeParams<T> {
  /** The type of items in the array. */
  items: Type<T>;
}

/**
 * Avro `array` type for homogeneous collections of items described by a schema.
 */
export class ArrayType<T = unknown> extends BaseType<T[]> {
  readonly #itemsType: Type<T>;

  /**
   * Creates a new ArrayType.
   * @param params The array type parameters.
   */
  constructor(params: ArrayTypeParams<T>) {
    super();
    if (!params.items) {
      throw new Error("ArrayType requires an items type.");
    }
    this.#itemsType = params.items;
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
      const elementPath = errorHook ? [...path, String(i)] : undefined;
      const validElement = this.#itemsType.check(
        element,
        errorHook,
        elementPath,
      );
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
   * Overrides the base write method to serialize an array.
   * @param tap The tap to write to.
   * @param value The array to write.
   */
  public override async write(
    tap: WritableTapLike,
    value: T[],
  ): Promise<void> {
    if (!Array.isArray(value)) {
      throwInvalidError([], value, this);
    }

    if (value.length > 0) {
      await tap.writeLong(BigInt(value.length));
      for (const element of value) {
        await this.#itemsType.write(tap, element);
      }
    }
    await tap.writeLong(0n);
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
   * Overrides the base toBuffer method to serialize an array to a buffer.
   * @param value The array to serialize.
   * @returns The serialized buffer.
   */
  public override async toBuffer(value: T[]): Promise<ArrayBuffer> {
    if (!Array.isArray(value)) {
      throwInvalidError([], value, this);
    }

    const elementBuffers = value.length === 0 ? [] : await Promise.all(
      value.map(async (element) =>
        new Uint8Array(await this.#itemsType.toBuffer(element))
      ),
    );

    let totalSize = 1; // final zero block terminator
    if (value.length > 0) {
      totalSize += calculateVarintSize(value.length);
      for (const buf of elementBuffers) {
        totalSize += buf.byteLength;
      }
    }

    const buffer = new ArrayBuffer(totalSize);
    const tap = new WritableTap(buffer);

    if (value.length > 0) {
      await tap.writeLong(BigInt(value.length));
      for (const buf of elementBuffers) {
        await tap.writeFixed(buf);
      }
    }
    await tap.writeLong(0n);

    return buffer;
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
    return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
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
}
