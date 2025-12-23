import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/sync_tap.ts";
import { NamedType } from "./named_type.ts";
import { Resolver } from "../resolver.ts";
import type { JSONType, Type } from "../type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";
import type { ResolvedNames } from "./resolve_names.ts";
import { compareUint8Arrays } from "../../serialization/compare_bytes.ts";

/**
 * Parameters for creating a FixedType.
 */
export interface FixedTypeParams extends ResolvedNames {
  /** The size in bytes. */
  size: number;
  /** Whether to validate during writes. Defaults to true. */
  validate?: boolean;
}

/**
 * Avro `fixed` type representing a fixed-length byte sequence.
 */
export class FixedType extends NamedType<Uint8Array> {
  #size: number;

  /**
   * Creates a new FixedType.
   * @param params The fixed type parameters.
   */
  constructor(params: FixedTypeParams) {
    const { size, ...names } = params;

    if (!Number.isInteger(size) || size < 1) {
      throw new Error(
        `Invalid fixed size: ${size}. Size must be a positive integer.`,
      );
    }

    super(names, params.validate ?? true);
    this.#size = size;
  }

  /**
   * Skips a fixed-size value by advancing the tap by the fixed size.
   * @param tap The tap to skip from.
   */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipFixed(this.#size);
  }

  /**
   * Advances a sync tap past the fixed payload without reading.
   */
  public override skipSync(tap: SyncReadableTapLike): void {
    tap.skipFixed(this.#size);
  }

  /**
   * Gets the size in bytes. 
   * This is a public method so that it can be called in the resolver.
   */
  public getSize(): number {
    return this.#size;
  }

  /**
   * Checks if the value is a valid fixed-size byte array.
   * @param value The value to check.
   * @param errorHook Optional error hook for invalid values.
   * @param path The path for error reporting.
   * @returns True if valid, false otherwise.
   */
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

  /**
   * Reads a fixed-size byte array from the tap.
   * @param tap The tap to read from.
   * @returns The read byte array.
   */
  public override async read(tap: ReadableTapLike): Promise<Uint8Array> {
    return await tap.readFixed(this.#size);
  }

  /**
   * Reads the fixed bytes synchronously from a tap.
   */
  public override readSync(tap: SyncReadableTapLike): Uint8Array {
    return tap.readFixed(this.#size);
  }

  /**
   * Writes a fixed-size byte array to the tap.
   * @param tap The tap to write to.
   * @param value The byte array to write.
   */
  /**
   * Writes fixed bytes without validation.
   */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: Uint8Array,
  ): Promise<void> {
    await tap.writeFixed(value);
  }

  /**
   * Writes fixed bytes without validation synchronously.
   */
  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: Uint8Array,
  ): void {
    tap.writeFixed(value);
  }

  protected override byteLength(_value: Uint8Array): number {
    return this.#size;
  }

  /**
   * Matches two fixed-size byte arrays from the taps.
   * @param tap1 The first tap.
   * @param tap2 The second tap.
   * @returns The comparison result.
   */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchFixed(tap2, this.#size);
  }

  /**
   * Compares two sync taps that decode fixed values.
   */
  public override matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    return tap1.matchFixed(tap2, this.#size);
  }

  /**
   * Compares two fixed values for ordering.
   * @param val1 The first value to compare.
   * @param val2 The second value to compare.
   * @returns A negative number if val1 < val2, zero if equal, positive if val1 > val2.
   */
  public override compare(val1: Uint8Array, val2: Uint8Array): number {
    if (!(val1 instanceof Uint8Array) || !(val2 instanceof Uint8Array)) {
      throw new Error("Fixed comparison requires Uint8Array values.");
    }

    if (val1.length !== this.#size || val2.length !== this.#size) {
      throw new Error(`Fixed values must be exactly ${this.#size} bytes.`);
    }

    return compareUint8Arrays(val1, val2);
  }

  /**
   * Clones a value into a Uint8Array, validating it against the fixed size.
   * @param value The value to clone, either a Uint8Array or a string.
   * @returns A new Uint8Array copy of the value.
   */
  public override cloneFromValue(value: unknown): Uint8Array {
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

  /**
   * Generates a random Uint8Array of the fixed size.
   * @returns A random Uint8Array with the fixed length.
   */
  public override random(): Uint8Array {
    const bytes = new Uint8Array(this.#size);
    for (let i = 0; i < this.#size; i++) {
      bytes[i] = Math.floor(Math.random() * 256);
    }
    return bytes;
  }

  /**
   * Converts the fixed type to its JSON schema representation.
   * @returns The JSON type object.
   */
  public override toJSON(): JSONType {
    return {
      name: this.getFullName(),
      type: "fixed",
      size: this.#size,
    };
  }

  /**
   * Creates a resolver for schema evolution between fixed types.
   * @param writerType The writer's type to resolve against.
   * @returns A resolver for reading data written with the writer type.
   */
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

  public override readSync(tap: SyncReadableTapLike): Uint8Array {
    const reader = this.readerType as FixedType;
    return reader.readSync(tap);
  }
}
