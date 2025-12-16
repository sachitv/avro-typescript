import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/sync_tap.ts";
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
  /** The underlying type that this logical type wraps. */
  protected readonly underlyingType: Type<TUnderlying>;
  readonly #validateWrites: boolean;

  /**
   * Constructs a new LogicalType.
   * @param underlyingType - The underlying type to wrap.
   */
  protected constructor(underlyingType: Type<TUnderlying>, validate = true) {
    super();
    this.underlyingType = underlyingType;
    this.#validateWrites = validate;
  }

  /**
   * Gets the underlying type that this logical type wraps.
   */
  public getUnderlyingType(): Type<TUnderlying> {
    return this.underlyingType;
  }

  /**
   * Checks if a value is an instance of the logical type's value.
   */
  protected abstract isInstance(value: unknown): value is TValue;
  /**
   * Converts the logical value to its underlying representation.
   */
  protected abstract toUnderlying(value: TValue): TUnderlying;
  /**
   * Converts the underlying representation to the logical value.
   */
  protected abstract fromUnderlying(value: TUnderlying): TValue;

  /**
   * Converts a value from the underlying type to the logical type value.
   * @param value The underlying value.
   */
  public convertFromUnderlying(value: TUnderlying): TValue {
    return this.fromUnderlying(value);
  }

  /**
   * Determines if this logical type is compatible with the writer logical type for reading.
   */
  protected canReadFromLogical(
    _writer: LogicalType<unknown, unknown>,
  ): boolean {
    return _writer.constructor === this.constructor;
  }

  /**
   * Serializes the logical value to an ArrayBuffer.
   * @param value The logical value to serialize.
   */
  public override async toBuffer(value: TValue): Promise<ArrayBuffer> {
    if (this.#validateWrites) {
      this.ensureValid(value, []);
    }
    const underlying = this.toUnderlying(value);
    return await this.underlyingType.toBuffer(underlying);
  }

  /**
   * Deserializes a logical value from an ArrayBuffer.
   * @param buffer The buffer to deserialize from.
   */
  public override async fromBuffer(buffer: ArrayBuffer): Promise<TValue> {
    const underlying = await this.underlyingType.fromBuffer(buffer);
    return this.fromUnderlying(underlying);
  }

  /**
   * Delegates synchronous buffer serialization to the underlying type.
   */
  public override toSyncBuffer(value: TValue): ArrayBuffer {
    if (this.#validateWrites) {
      this.ensureValid(value, []);
    }
    const underlying = this.toUnderlying(value);
    return this.underlyingType.toSyncBuffer(underlying);
  }

  /**
   * Converts a synchronous buffer into the logical value by decoding the underlying type.
   */
  public override fromSyncBuffer(buffer: ArrayBuffer): TValue {
    const underlying = this.underlyingType.fromSyncBuffer(buffer);
    return this.fromUnderlying(underlying);
  }

  /**
   * Checks if the given value is valid for this type.
   * @param value The value to validate.
   * @param opts Optional validation options.
   */
  public override isValid(
    value: unknown,
    opts?: { errorHook?: ErrorHook },
  ): boolean {
    return this.check(value, opts?.errorHook, []);
  }

  /**
   * Validates the value against this type, reporting errors via the hook if provided.
   * @param value The value to check.
   * @param errorHook Optional hook for reporting validation errors.
   * @param path The path to the value in the schema.
   */
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

  /**
   * Clones a value, ensuring it is valid for this type.
   */
  public override cloneFromValue(value: unknown): TValue {
    this.check(value, throwInvalidError, []);
    const typedValue = value as TValue;
    const cloned = this.underlyingType.cloneFromValue(
      this.toUnderlying(typedValue),
    );
    return this.fromUnderlying(cloned);
  }

  /**
   * Compares two values by comparing their underlying representations.
   */
  public override compare(val1: TValue, val2: TValue): number {
    const u1 = this.toUnderlying(val1);
    const u2 = this.toUnderlying(val2);
    return this.underlyingType.compare(u1, u2);
  }

  /**
   * Generates a random value by generating a random underlying value and converting it.
   */
  public override random(): TValue {
    const underlying = this.underlyingType.random();
    return this.fromUnderlying(underlying);
  }

  /**
   * Writes the value to the tap by writing the underlying value.
   */
  public override async write(
    tap: WritableTapLike,
    value: TValue,
  ): Promise<void> {
    if (!this.#validateWrites) {
      await this.writeUnchecked(tap, value);
      return;
    }
    this.ensureValid(value, []);
    await this.underlyingType.write(tap, this.toUnderlying(value));
  }

  /**
   * Writes the logical value synchronously via the underlying type's writer.
   */
  public override writeSync(tap: SyncWritableTapLike, value: TValue): void {
    if (!this.#validateWrites) {
      this.writeSyncUnchecked(tap, value);
      return;
    }
    this.ensureValid(value, []);
    this.underlyingType.writeSync(tap, this.toUnderlying(value));
  }

  /**
   * Writes logical value without validation.
   * Applies the logical transform but uses the underlying type's unchecked writer.
   */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: TValue,
  ): Promise<void> {
    const underlying = this.toUnderlying(value);
    await this.underlyingType.writeUnchecked(tap, underlying);
  }

  /**
   * Writes logical value without validation synchronously.
   * Applies the logical transform but uses the underlying type's unchecked writer.
   */
  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: TValue,
  ): void {
    const underlying = this.toUnderlying(value);
    this.underlyingType.writeSyncUnchecked(tap, underlying);
  }

  /** Reads the value from the tap. */
  public override async read(tap: ReadableTapLike): Promise<TValue> {
    const underlying = await this.underlyingType.read(tap);
    return this.fromUnderlying(underlying);
  }

  /**
   * Reads the logical value synchronously from the underlying type.
   */
  public override readSync(tap: SyncReadableTapLike): TValue {
    const underlying = this.underlyingType.readSync(tap);
    return this.fromUnderlying(underlying);
  }

  /** Skips the value in the tap. */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await this.underlyingType.skip(tap);
  }

  /**
   * Skips the logical value by skipping the underlying representation.
   */
  public override skipSync(tap: SyncReadableTapLike): void {
    this.underlyingType.skipSync(tap);
  }

  /** Matches the value in the taps. */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await this.underlyingType.match(tap1, tap2);
  }

  /**
   * Compares two sync taps by delegating to the underlying type match.
   */
  public override matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    return this.underlyingType.matchSync(tap1, tap2);
  }

  /**
   * Creates a resolver for reading from the writer type, handling logical types specially.
   */
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

  /**
   * Ensures the value is valid for this logical type.
   * @param value The value to validate.
   * @param path The path for error reporting.
   */
  protected ensureValid(value: TValue, path: string[]): void {
    this.check(value, throwInvalidError, path);
  }

  /**
   * Returns the JSON representation of this logical type.
   */
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

  /**
   * Synchronously resolves logical values emitted by resolved streams.
   */
  public override readSync(tap: SyncReadableTapLike): TValue {
    const underlying = this.#resolver.readSync(tap);
    return this.#logicalType.convertFromUnderlying(underlying);
  }
}

/**
 * Logical type variant for named Avro schemas (record, enum, fixed) that keeps
 * track of the fully qualified name and aliases of the underlying type.
 */
/**
 * Logical type variant for named Avro schemas (record, enum, fixed) that keeps
 * track of the fully qualified name and aliases of the underlying type.
 */
export abstract class NamedLogicalType<TValue, TUnderlying>
  extends LogicalType<TValue, TUnderlying> {
  /** The underlying named type. */
  protected readonly namedType: NamedType<TUnderlying>;

  /**
   * Creates a new NamedLogicalType.
   * @param namedType The underlying named type.
   */
  protected constructor(namedType: NamedType<TUnderlying>, validate = true) {
    super(namedType, validate);
    this.namedType = namedType;
  }

  /**
   * Gets the full name of the underlying type.
   */
  public getFullName(): string {
    return this.namedType.getFullName();
  }

  /**
   * Gets the namespace of the underlying type.
   */
  public getNamespace(): string {
    return this.namedType.getNamespace();
  }

  /**
   * Gets the aliases of the underlying type.
   */
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
