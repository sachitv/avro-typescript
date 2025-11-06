import { Tap } from "../serialization/tap.ts";
import { bigIntToSafeNumber } from "../serialization/conversion.ts";
import { BaseType } from "./base_type.ts";
import { Resolver } from "./resolver.ts";
import { type JSONType, Type } from "./type.ts";
import { type ErrorHook, throwInvalidError } from "./error.ts";
import { calculateVarintSize } from "./varint.ts";

export async function readArrayInto<T>(
  tap: Tap,
  readElement: (tap: Tap) => Promise<T>,
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

export interface ArrayTypeParams<T> {
  items: Type<T>;
}

export class ArrayType<T = unknown> extends BaseType<T[]> {
  readonly #itemsType: Type<T>;

  constructor(params: ArrayTypeParams<T>) {
    super();
    if (!params.items) {
      throw new Error("ArrayType requires an items type.");
    }
    this.#itemsType = params.items;
  }

  public getItemsType(): Type<T> {
    return this.#itemsType;
  }

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

  public override async write(tap: Tap, value: T[]): Promise<void> {
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

  public override async skip(tap: Tap): Promise<void> {
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

  public override async read(tap: Tap): Promise<T[]> {
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
    const tap = new Tap(buffer);

    if (value.length > 0) {
      await tap.writeLong(BigInt(value.length));
      for (const buf of elementBuffers) {
        await tap.writeFixed(buf);
      }
    }
    await tap.writeLong(0n);

    return buffer;
  }

  public override clone(value: T[], opts?: Record<string, unknown>): T[] {
    if (!Array.isArray(value)) {
      throw new Error("Cannot clone non-array value.");
    }
    return value.map((element) => this.#itemsType.clone(element, opts));
  }

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

  public override random(): T[] {
    // There should be at least one element.
    const length = Math.ceil(Math.random() * 10);
    const result: T[] = [];
    for (let i = 0; i < length; i++) {
      result.push(this.#itemsType.random());
    }
    return result;
  }

  public override toJSON(): JSONType {
    return {
      type: "array",
      items: this.#itemsType.toJSON(),
    };
  }

  public override async match(tap1: Tap, tap2: Tap): Promise<number> {
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

  async #readArraySize(tap: Tap): Promise<bigint> {
    let n = await tap.readLong();
    if (n < 0n) {
      n = -n;
      await tap.skipLong(); // skip size
    }
    return n;
  }

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

  public override async read(tap: Tap): Promise<T[]> {
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
