import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import { Resolver } from "../resolver.ts";
import type { NamedType } from "../complex/named_type.ts";
import { Type } from "../type.ts";
import type { JSONType } from "../type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";

/**
 * Base class for Avro logical types that wrap an underlying physical type
 * while exposing a richer TypeScript representation.
 */
export abstract class LogicalType<TValue, TUnderlying> extends Type<TValue> {
  protected readonly underlyingType: Type<TUnderlying>;

  protected constructor(underlyingType: Type<TUnderlying>) {
    super();
    this.underlyingType = underlyingType;
  }

  public getUnderlyingType(): Type<TUnderlying> {
    return this.underlyingType;
  }

  protected abstract isInstance(value: unknown): value is TValue;
  protected abstract toUnderlying(value: TValue): TUnderlying;
  protected abstract fromUnderlying(value: TUnderlying): TValue;

  public convertFromUnderlying(value: TUnderlying): TValue {
    return this.fromUnderlying(value);
  }

  protected canReadFromLogical(
    _writer: LogicalType<unknown, unknown>,
  ): boolean {
    return _writer.constructor === this.constructor;
  }

  public override async toBuffer(value: TValue): Promise<ArrayBuffer> {
    this.ensureValid(value, []);
    const underlying = this.toUnderlying(value);
    return await this.underlyingType.toBuffer(underlying);
  }

  public override async fromBuffer(buffer: ArrayBuffer): Promise<TValue> {
    const underlying = await this.underlyingType.fromBuffer(buffer);
    return this.fromUnderlying(underlying);
  }

  public override isValid(
    value: unknown,
    opts?: { errorHook?: ErrorHook },
  ): boolean {
    return this.check(value, opts?.errorHook, []);
  }

  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    if (!this.isInstance(value)) {
      if (errorHook) {
        errorHook(path.slice(), value, this);
      }
      return false;
    }

    let underlying: TUnderlying;
    try {
      underlying = this.toUnderlying(value);
    } catch {
      if (errorHook) {
        errorHook(path.slice(), value, this);
      }
      return false;
    }

    return this.underlyingType.check(underlying, errorHook, path);
  }

  public override clone(value: unknown): TValue {
    this.check(value, throwInvalidError, []);
    const typedValue = value as TValue;
    const cloned = this.underlyingType.clone(this.toUnderlying(typedValue));
    return this.fromUnderlying(cloned);
  }

  public override compare(val1: TValue, val2: TValue): number {
    const u1 = this.toUnderlying(val1);
    const u2 = this.toUnderlying(val2);
    return this.underlyingType.compare(u1, u2);
  }

  public override random(): TValue {
    const underlying = this.underlyingType.random();
    return this.fromUnderlying(underlying);
  }

  public override async write(
    tap: WritableTapLike,
    value: TValue,
  ): Promise<void> {
    this.ensureValid(value, []);
    await this.underlyingType.write(tap, this.toUnderlying(value));
  }

  public override async read(tap: ReadableTapLike): Promise<TValue> {
    const underlying = await this.underlyingType.read(tap);
    return this.fromUnderlying(underlying);
  }

  public override async skip(tap: ReadableTapLike): Promise<void> {
    await this.underlyingType.skip(tap);
  }

  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await this.underlyingType.match(tap1, tap2);
  }

  public override createResolver(writerType: Type): Resolver {
    if (writerType instanceof LogicalType) {
      if (!this.canReadFromLogical(writerType)) {
        throw new Error(
          "Schema evolution not supported between incompatible logical types.",
        );
      }
      const resolver = this.underlyingType.createResolver(
        writerType.getUnderlyingType() as Type<TUnderlying>,
      );
      return new LogicalResolver(this, resolver);
    }

    const resolver = this.underlyingType.createResolver(
      writerType as Type<TUnderlying>,
    );
    return new LogicalResolver(this, resolver);
  }

  protected ensureValid(value: TValue, path: string[]): void {
    this.check(value, throwInvalidError, path);
  }

  public abstract override toJSON(): JSONType;
}

class LogicalResolver<TValue, TUnderlying> extends Resolver<TValue> {
  #logicalType: LogicalType<TValue, TUnderlying>;
  #resolver: Resolver<TUnderlying>;

  constructor(
    logicalType: LogicalType<TValue, TUnderlying>,
    resolver: Resolver<TUnderlying>,
  ) {
    super(logicalType);
    this.#logicalType = logicalType;
    this.#resolver = resolver;
  }

  public override async read(tap: ReadableTapLike): Promise<TValue> {
    const underlying = await this.#resolver.read(tap);
    return this.#logicalType.convertFromUnderlying(underlying);
  }
}

/**
 * Logical type variant for named Avro schemas (record, enum, fixed) that keeps
 * track of the fully qualified name and aliases of the underlying type.
 */
export abstract class NamedLogicalType<TValue, TUnderlying>
  extends LogicalType<TValue, TUnderlying> {
  protected readonly namedType: NamedType<TUnderlying>;

  protected constructor(namedType: NamedType<TUnderlying>) {
    super(namedType);
    this.namedType = namedType;
  }

  public getFullName(): string {
    return this.namedType.getFullName();
  }

  public getNamespace(): string {
    return this.namedType.getNamespace();
  }

  public getAliases(): string[] {
    return this.namedType.getAliases();
  }
}

/**
 * Helper to attach a logicalType annotation to an underlying Avro schema JSON.
 */
export function withLogicalTypeJSON(
  underlying: JSONType,
  logicalType: string,
  extras: Record<string, unknown> = {},
): JSONType {
  if (typeof underlying === "string") {
    return { type: underlying, logicalType, ...extras };
  }
  if (
    underlying && typeof underlying === "object" && !Array.isArray(underlying)
  ) {
    return { ...underlying, logicalType, ...extras };
  }
  throw new Error(
    "Unsupported underlying schema for logical type serialization.",
  );
}
