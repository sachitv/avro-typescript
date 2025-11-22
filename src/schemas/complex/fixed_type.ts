import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../../serialization/tap.ts";
import { NamedType } from "./named_type.ts";
import { Resolver } from "../resolver.ts";
import type { JSONType, Type } from "../type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";
import type { ResolvedNames } from "./resolve_names.ts";
import { compareUint8Arrays } from "../../serialization/compare_bytes.ts";

export interface FixedTypeParams extends ResolvedNames {
  size: number;
}

/**
 * Avro `fixed` type representing a fixed-length byte sequence.
 */
export class FixedType extends NamedType<Uint8Array> {
  #size: number;

  constructor(params: FixedTypeParams) {
    const { size, ...names } = params;

    if (!Number.isInteger(size) || size < 1) {
      throw new Error(
        `Invalid fixed size: ${size}. Size must be a positive integer.`,
      );
    }

    super(names);
    this.#size = size;
  }

  public sizeBytes(): number {
    return this.#size;
  }

  /**
   * Serializes a value into an ArrayBuffer using the exact fixed size.
   * @param value The value to serialize.
   * @returns The serialized ArrayBuffer.
   */
  public override async toBuffer(value: Uint8Array): Promise<ArrayBuffer> {
    this.check(value, throwInvalidError, []);
    const size = this.sizeBytes();
    const buf = new ArrayBuffer(size);
    const tap = new WritableTap(buf);
    await this.write(tap, value);
    return buf;
  }

  /**
   * Skips a fixed-size value by advancing the tap by the fixed size.
   * @param tap The tap to skip from.
   */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipFixed(this.sizeBytes());
  }

  public getSize(): number {
    return this.#size;
  }

  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = value instanceof Uint8Array && value.length === this.#size;

    if (!isValid && errorHook) {
      errorHook(path.slice(), value, this);
    }

    return isValid;
  }

  public override async read(tap: ReadableTapLike): Promise<Uint8Array> {
    const result = await tap.readFixed(this.#size);
    if (result === undefined) {
      throw new Error("Insufficient data for fixed type");
    }
    return result;
  }

  public override async write(
    tap: WritableTapLike,
    value: Uint8Array,
  ): Promise<void> {
    if (!(value instanceof Uint8Array) || value.length !== this.#size) {
      throwInvalidError([], value, this);
    }
    await tap.writeFixed(value, this.#size);
  }

  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchFixed(tap2, this.#size);
  }

  public override compare(val1: Uint8Array, val2: Uint8Array): number {
    if (!(val1 instanceof Uint8Array) || !(val2 instanceof Uint8Array)) {
      throw new Error("Fixed comparison requires Uint8Array values.");
    }

    if (val1.length !== this.#size || val2.length !== this.#size) {
      throw new Error(`Fixed values must be exactly ${this.#size} bytes.`);
    }

    return compareUint8Arrays(val1, val2);
  }

  public override clone(value: unknown): Uint8Array {
    let bytes: Uint8Array;
    if (value instanceof Uint8Array) {
      bytes = value;
    } else if (typeof value === "string") {
      bytes = FixedType.#fromJsonString(value);
    } else {
      throwInvalidError([], value, this);
    }
    this.check(bytes, throwInvalidError, []);
    return new Uint8Array(bytes);
  }

  public override random(): Uint8Array {
    const bytes = new Uint8Array(this.#size);
    for (let i = 0; i < this.#size; i++) {
      bytes[i] = Math.floor(Math.random() * 256);
    }
    return bytes;
  }

  public override toJSON(): JSONType {
    return {
      name: this.getFullName(),
      type: "fixed",
      size: this.#size,
    };
  }

  public override createResolver(writerType: Type): Resolver {
    if (!(writerType instanceof FixedType)) {
      return super.createResolver(writerType);
    }

    const acceptableNames = new Set<string>([
      this.getFullName(),
      ...this.getAliases(),
    ]);

    const writerNames = new Set<string>([
      writerType.getFullName(),
      ...writerType.getAliases(),
    ]);

    const hasCompatibleName = Array.from(writerNames).some((name) =>
      acceptableNames.has(name)
    );

    if (!hasCompatibleName) {
      throw new Error(
        `Schema evolution not supported from writer type: ${writerType.getFullName()} to reader type: ${this.getFullName()}`,
      );
    }

    if (this.#size !== writerType.getSize()) {
      throw new Error(
        `Cannot resolve fixed types with different sizes: writer has ${writerType.getSize()}, reader has ${this.#size}`,
      );
    }

    // If sizes match and names are compatible, we can use the reader's read method directly
    return new FixedResolver(this);
  }

  static #fromJsonString(value: string): Uint8Array {
    const bytes = new Uint8Array(value.length);
    for (let i = 0; i < value.length; i++) {
      bytes[i] = value.charCodeAt(i) & 0xff;
    }
    return bytes;
  }
}

class FixedResolver extends Resolver<Uint8Array> {
  constructor(reader: FixedType) {
    super(reader);
  }

  public override async read(tap: ReadableTapLike): Promise<Uint8Array> {
    const reader = this.readerType as FixedType;
    return await reader.read(tap);
  }
}
