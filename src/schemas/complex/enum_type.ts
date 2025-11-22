import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../../serialization/tap.ts";
import { Resolver } from "../resolver.ts";
import type { JSONType, Type } from "../type.ts";
import { NamedType } from "./named_type.ts";
import { isValidName, type ResolvedNames } from "./resolve_names.ts";
import { calculateVarintSize } from "../../internal/varint.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";

/**
 * Parameters for creating an EnumType.
 */
export interface EnumTypeParams extends ResolvedNames {
  /** The allowed symbols for the enum. */
  symbols: string[];
  /** Optional default value. */
  default?: string;
}

/**
 * Avro enum type implemented in TypeScript.
 * Values are represented as strings selected from the provided symbol set.
 */
export class EnumType extends NamedType<string> {
  readonly #symbols: string[];
  readonly #indices: Map<string, number>;
  readonly #default?: string;

  /**
   * Creates a new EnumType.
   * @param params The enum type parameters.
   */
  constructor(params: EnumTypeParams) {
    if (!(Array.isArray(params.symbols)) || params.symbols.length === 0) {
      throw new Error("EnumType requires a non-empty symbols array.");
    }

    const { symbols, default: defaultValue, ...nameInfo } = params;
    super(nameInfo);
    this.#symbols = symbols.slice();
    this.#indices = new Map<string, number>();

    this.#symbols.forEach((symbol, index) => {
      if (!isValidName(symbol)) {
        throw new Error(`Invalid enum symbol: ${symbol}`);
      }
      if (this.#indices.has(symbol)) {
        throw new Error(`Duplicate enum symbol: ${symbol}`);
      }
      this.#indices.set(symbol, index);
    });

    if (defaultValue !== undefined) {
      if (!this.#symbols.includes(defaultValue)) {
        throw new Error("Default value must be a member of the symbols array.");
      }
      this.#default = defaultValue;
    }
  }

  /**
   * Gets the symbols allowed in this enum.
   */
  public getSymbols(): string[] {
    return this.#symbols.slice();
  }

  /**
   * Gets the default value, if any.
   */
  public getDefault(): string | undefined {
    return this.#default;
  }

  /**
   * Validates if the value is a valid enum symbol.
   * @param value The value to check.
   * @param errorHook Optional error hook for validation errors.
   * @param path The current path in the schema.
   * @returns True if the value is a valid enum symbol, false otherwise.
   */
  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = typeof value === "string" && this.#indices.has(value);
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  /**
   * Serializes the enum value by writing its index.
   * @param tap The tap to write to.
   * @param value The enum value to write.
   */
  public override async write(
    tap: WritableTapLike,
    value: string,
  ): Promise<void> {
    const index = this.#indices.get(value);
    if (index === undefined) {
      throwInvalidError([], value, this);
    }
    await tap.writeLong(BigInt(index));
  }

  /**
   * Skips the enum value in the tap.
   */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipLong();
  }

  /**
   * Reads the enum value from the tap.
   */
  public override async read(tap: ReadableTapLike): Promise<string> {
    const rawIndex = await tap.readLong();
    const index = Number(rawIndex);
    if (
      !Number.isSafeInteger(index) || index < 0 || index >= this.#symbols.length
    ) {
      throw new Error(
        `Invalid enum index ${rawIndex.toString()} for ${this.getFullName()}`,
      );
    }
    return this.#symbols[index];
  }

  /**
   * Converts the enum value to its binary buffer representation.
   */
  public override async toBuffer(value: string): Promise<ArrayBuffer> {
    this.check(value, throwInvalidError, []);
    const index = this.#indices.get(value)!;
    const size = calculateVarintSize(index);
    const buffer = new ArrayBuffer(size);
    const tap = new WritableTap(buffer);
    await this.write(tap, value);
    return buffer;
  }

  /** Clones a value into a string. */
  public override cloneFromValue(value: unknown): string {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    return value as string;
  }

  /** Compares two strings. */
  public override compare(val1: string, val2: string): number {
    const i1 = this.#indices.get(val1);
    const i2 = this.#indices.get(val2);
    if (i1 === undefined || i2 === undefined) {
      throw new Error("Cannot compare values not present in the enum.");
    }
    return i1 - i2;
  }

  /**
   * Generates a random symbol from the enum.
   */
  public override random(): string {
    const idx = Math.floor(Math.random() * this.#symbols.length);
    return this.#symbols[idx];
  }

  /**
   * Returns the JSON representation of the enum type.
   */
  public override toJSON(): JSONType {
    return "enum";
  }

  /**
   * Compares two encoded enum values for equality.
   */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchLong(tap2);
  }

  /**
   * Creates a resolver for schema evolution between enum types.
   */
  public override createResolver(writerType: Type): Resolver {
    if (!(writerType instanceof EnumType)) {
      return super.createResolver(writerType);
    }

    const acceptableNames = new Set<string>([
      this.getFullName(),
      ...this.getAliases(),
    ]);
    const writerName = writerType.getFullName();
    if (!acceptableNames.has(writerName)) {
      throw new Error(
        `Schema evolution not supported from writer type: ${writerType.getFullName()} to reader type: ${this.getFullName()}`,
      );
    }

    const writerSymbols = writerType.getSymbols();
    const allSymbolsSupported = writerSymbols.every((symbol) =>
      this.#indices.has(symbol)
    );

    if (allSymbolsSupported) {
      return new class extends Resolver<string> {
        #writer: EnumType;

        constructor(reader: EnumType, writer: EnumType) {
          super(reader);
          this.#writer = writer;
        }

        public override async read(tap: ReadableTapLike): Promise<string> {
          return await this.#writer.read(tap);
        }
      }(this, writerType);
    } else if (this.#default !== undefined) {
      return new class extends Resolver<string> {
        #writer: EnumType;
        #reader: EnumType;

        constructor(reader: EnumType, writer: EnumType) {
          super(reader);
          this.#reader = reader;
          this.#writer = writer;
        }

        public override async read(tap: ReadableTapLike): Promise<string> {
          const writerSymbol = await this.#writer.read(tap);
          if (this.#reader.#indices.has(writerSymbol)) {
            return writerSymbol;
          } else {
            return this.#reader.#default!;
          }
        }
      }(this, writerType);
    } else {
      throw new Error(
        `Schema evolution not supported from writer type: ${writerType.getFullName()} to reader type: ${this.getFullName()}`,
      );
    }
  }
}
