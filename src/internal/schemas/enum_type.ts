import { Tap } from "../serialization/tap.ts";
import { Resolver } from "./resolver.ts";
import { Type } from "./type.ts";
import { NamedType } from "./named_type.ts";
import { NAME_PATTERN, type ResolvedNames } from "./resolve_names.ts";
import { calculateVarintSize } from "./varint.ts";
import { type ErrorHook, throwInvalidError } from "./error.ts";

export interface EnumTypeParams extends ResolvedNames {
  symbols: string[];
}

/**
 * Avro enum type implemented in TypeScript.
 * Values are represented as strings selected from the provided symbol set.
 */
export class EnumType extends NamedType<string> {
  readonly #symbols: string[];
  readonly #indices: Map<string, number>;

  constructor(params: EnumTypeParams) {
    if (!(Array.isArray(params.symbols)) || params.symbols.length === 0) {
      throw new Error("EnumType requires a non-empty symbols array.");
    }

    const { symbols, ...nameInfo } = params;
    super(nameInfo);
    this.#symbols = symbols.slice();
    this.#indices = new Map<string, number>();

    this.#symbols.forEach((symbol, index) => {
      if (!NAME_PATTERN.test(symbol)) {
        throw new Error(`Invalid enum symbol: ${symbol}`);
      }
      if (this.#indices.has(symbol)) {
        throw new Error(`Duplicate enum symbol: ${symbol}`);
      }
      this.#indices.set(symbol, index);
    });
  }

  public getSymbols(): string[] {
    return this.#symbols.slice();
  }

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

  public override write(tap: Tap, value: string): void {
    const index = this.#indices.get(value);
    if (index === undefined) {
      throwInvalidError([], value, this);
    }
    tap.writeLong(BigInt(index));
  }

  public override read(tap: Tap): string {
    const rawIndex = tap.readLong();
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

  public override toBuffer(value: string): ArrayBuffer {
    this.check(value, throwInvalidError, []);
    const index = this.#indices.get(value)!;
    const size = calculateVarintSize(index);
    const buffer = new ArrayBuffer(size);
    const tap = new Tap(buffer);
    this.write(tap, value);
    return buffer;
  }

  public override clone(value: string): string {
    if (!this.check(value)) {
      throw new Error(`Invalid enum value: ${value}`);
    }
    return value;
  }

  public override compare(val1: string, val2: string): number {
    const i1 = this.#indices.get(val1);
    const i2 = this.#indices.get(val2);
    if (i1 === undefined || i2 === undefined) {
      throw new Error("Cannot compare values not present in the enum.");
    }
    return i1 - i2;
  }

  public override random(): string {
    const idx = Math.floor(Math.random() * this.#symbols.length);
    return this.#symbols[idx];
  }

  public override toJSON(): string {
    return "enum";
  }

  public override createResolver(writerType: Type): Resolver {
    if (!(writerType instanceof EnumType)) {
      return super.createResolver(writerType);
    }

    const acceptableNames = new Set<string>([
      this.getFullName(),
      ...this.getAliases(),
    ]);
    const writerName = writerType.getFullName();
    const writerSymbols = writerType.getSymbols();
    const allSymbolsSupported = writerSymbols.every((symbol) =>
      this.#indices.has(symbol)
    );

    if (acceptableNames.has(writerName) && allSymbolsSupported) {
      return new class extends Resolver<string> {
        #writer: EnumType;

        constructor(reader: EnumType, writer: EnumType) {
          super(reader);
          this.#writer = writer;
        }

        public override read(tap: Tap): string {
          return this.#writer.read(tap);
        }
      }(this, writerType);
    }

    throw new Error(
      `Schema evolution not supported from writer type: ${writerType.getFullName()} to reader type: ${this.getFullName()}`,
    );
  }
}
