import type { IntType } from "../primitive/int_type.ts";
import type { LongType } from "../primitive/long_type.ts";
import { LogicalType, withLogicalTypeJSON } from "./logical_type.ts";
import { bigIntToSafeNumber } from "../../serialization/conversion.ts";
import type { JSONType } from "../type.ts";

const MILLIS_PER_DAY = 86_400_000;
const MICROS_PER_DAY = 86_400_000_000n;

/**
 * Logical type for `date`.
 * Represents the number of days since the epoch.
 */
export class DateLogicalType extends LogicalType<number, number> {
  /**
   * Creates a new DateLogicalType.
   * @param underlying The underlying int type.
   */
  constructor(underlying: IntType) {
    super(underlying);
  }

  /** Checks if the logical type can read from the underlying type. */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof DateLogicalType;
  }

  /** Checks if the value is a valid instance of this logical type. */
  /** Checks if the value is a valid instance of this logical type. */
  protected override isInstance(value: unknown): value is number {
    return typeof value === "number" && Number.isInteger(value);
  }

  /** Converts the logical value to the underlying representation. */
  protected override toUnderlying(value: number): number {
    return value;
  }

  /** Converts the underlying value to the logical representation. */
  protected override fromUnderlying(value: number): number {
    return value;
  }

  /** Generates a random value for testing. */
  public override random(): number {
    const today = Math.floor(Date.now() / MILLIS_PER_DAY);
    return today + Math.floor(Math.random() * 2000) - 1000;
  }

  /** Returns the JSON representation of this type. */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(this.getUnderlyingType().toJSON(), "date");
  }
}

/**
 * Logical type for `time-millis`.
 * Represents the time of day in milliseconds since midnight.
 */
export class TimeMillisLogicalType extends LogicalType<number, number> {
  /**
   * Creates a new TimeMillisLogicalType.
   * @param underlying The underlying int type.
   */
  constructor(underlying: IntType) {
    super(underlying);
  }

  /** Checks if the logical type can read from the underlying type. */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof TimeMillisLogicalType;
  }

  /** Checks if the value is a valid instance of this logical type. */
  protected override isInstance(value: unknown): value is number {
    return typeof value === "number" && Number.isInteger(value) &&
      value >= 0 && value < MILLIS_PER_DAY;
  }

  /** Converts the logical value to the underlying representation. */
  protected override toUnderlying(value: number): number {
    return value;
  }

  /** Converts the underlying value to the logical representation. */
  protected override fromUnderlying(value: number): number {
    return value;
  }

  /** Generates a random value for testing. */
  public override random(): number {
    return Math.floor(Math.random() * MILLIS_PER_DAY);
  }

  /** Returns the JSON representation of this type. */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "time-millis",
    );
  }
}

/**
 * Logical type for `time-micros`.
 * Represents the time of day in microseconds since midnight.
 */
export class TimeMicrosLogicalType extends LogicalType<bigint, bigint> {
  /**
   * Creates a new TimeMicrosLogicalType.
   * @param underlying The underlying long type.
   */
  constructor(underlying: LongType) {
    super(underlying);
  }

  /** Checks if the logical type can read from the underlying type. */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof TimeMicrosLogicalType;
  }

  /** Checks if the value is a valid instance of this logical type. */
  /** Checks if the value is a valid instance of this logical type. */
  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint" && value >= 0n && value < MICROS_PER_DAY;
  }

  /** Converts the logical value to the underlying representation. */
  protected override toUnderlying(value: bigint): bigint {
    return value;
  }

  /** Converts the underlying value to the logical representation. */
  protected override fromUnderlying(value: bigint): bigint {
    return value;
  }

  /** Generates a random value for testing. */
  public override random(): bigint {
    return BigInt(Math.floor(Math.random() * Number(MICROS_PER_DAY)));
  }

  /** Returns the JSON representation of this type. */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "time-micros",
    );
  }
}

/**
 * Logical type for `timestamp-millis`.
 * Represents an instant in time as the number of milliseconds since the epoch.
 */
export class TimestampMillisLogicalType extends LogicalType<Date, bigint> {
  /**
   * Creates a new TimestampMillisLogicalType.
   * @param underlying The underlying long type.
   */
  constructor(underlying: LongType) {
    super(underlying);
  }

  /** Checks if the logical type can read from the underlying type. */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof TimestampMillisLogicalType;
  }

  /** Checks if the value is a valid instance of this logical type. */
  protected override isInstance(value: unknown): value is Date {
    return value instanceof Date && !Number.isNaN(value.getTime());
  }

  /** Converts the logical value to the underlying representation. */
  protected override toUnderlying(value: Date): bigint {
    const millis = value.getTime();
    if (!Number.isFinite(millis)) {
      throw new Error("Invalid Date value for timestamp-millis logical type.");
    }
    return BigInt(Math.trunc(millis));
  }

  /** Converts the underlying value to the logical representation. */
  protected override fromUnderlying(value: bigint): Date {
    const millis = bigIntToSafeNumber(value, "timestamp-millis");
    return new Date(millis);
  }

  /** Generates a random value for testing. */
  public override random(): Date {
    const now = Date.now();
    const offset = Math.floor(Math.random() * 1_000_000_000) - 500_000_000;
    return new Date(now + offset);
  }

  /** Compares two Date values. */
  public override compare(val1: Date, val2: Date): number {
    const diff = val1.getTime() - val2.getTime();
    return diff < 0 ? -1 : diff > 0 ? 1 : 0;
  }

  /** Returns the JSON representation of this type. */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "timestamp-millis",
    );
  }
}

/**
 * Logical type for `timestamp-micros`.
 * Represents an instant in time as the number of microseconds since the epoch.
 */
export class TimestampMicrosLogicalType extends LogicalType<bigint, bigint> {
  /**
   * Creates a new TimestampMicrosLogicalType.
   * @param underlying The underlying long type.
   */
  constructor(underlying: LongType) {
    super(underlying);
  }

  /** Checks if the logical type can read from the underlying type. */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof TimestampMicrosLogicalType;
  }

  /** Checks if the value is a valid instance of this logical type. */
  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint";
  }

  /** Converts the logical value to the underlying representation. */
  protected override toUnderlying(value: bigint): bigint {
    return value;
  }

  /** Converts the underlying value to the logical representation. */
  protected override fromUnderlying(value: bigint): bigint {
    return value;
  }

  /** Generates a random value for testing. */
  public override random(): bigint {
    const nowMicros = BigInt(Math.trunc(Date.now())) * 1000n;
    const offset = BigInt(Math.floor(Math.random() * 1_000_000)) - 500_000n;
    return nowMicros + offset;
  }

  /** Returns the JSON representation of this type. */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "timestamp-micros",
    );
  }
}

/**
 * Logical type for `timestamp-nanos`.
 * Represents an instant in time as the number of nanoseconds since the epoch.
 */
export class TimestampNanosLogicalType extends LogicalType<bigint, bigint> {
  /**
   * Creates a new TimestampNanosLogicalType.
   * @param underlying The underlying long type.
   */
  constructor(underlying: LongType) {
    super(underlying);
  }

  /** Checks if the logical type can read from the underlying type. */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof TimestampNanosLogicalType;
  }

  /** Checks if the value is a valid instance of this logical type. */
  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint";
  }

  /** Converts the logical value to the underlying representation. */
  protected override toUnderlying(value: bigint): bigint {
    return value;
  }

  /** Converts the underlying value to the logical representation. */
  protected override fromUnderlying(value: bigint): bigint {
    return value;
  }

  /** Generates a random value for testing. */
  public override random(): bigint {
    const nowNanos = BigInt(Math.trunc(Date.now())) * 1_000_000n;
    const offset = BigInt(Math.floor(Math.random() * 1_000_000)) - 500_000n;
    return nowNanos + offset;
  }

  /** Returns the JSON representation of this type. */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "timestamp-nanos",
    );
  }
}

/**
 * Logical type for `local-timestamp-millis`.
 * Represents a timestamp in a local time zone, in milliseconds.
 */
export class LocalTimestampMillisLogicalType
  extends LogicalType<number, bigint> {
  /**
   * Creates a new LocalTimestampMillisLogicalType.
   * @param underlying The underlying long type.
   */
  constructor(underlying: LongType) {
    super(underlying);
  }

  /** Checks if the logical type can read from the underlying type. */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof LocalTimestampMillisLogicalType;
  }

  /** Checks if the value is a valid instance of this logical type. */
  protected override isInstance(value: unknown): value is number {
    return typeof value === "number" && Number.isFinite(value) &&
      Number.isInteger(value);
  }

  /** Converts the logical value to the underlying representation. */
  protected override toUnderlying(value: number): bigint {
    return BigInt(value);
  }

  /** Converts the underlying value to the logical representation. */
  protected override fromUnderlying(value: bigint): number {
    return bigIntToSafeNumber(value, "local-timestamp-millis");
  }

  /** Generates a random value for testing. */
  public override random(): number {
    const now = Date.now();
    const offset = Math.floor(Math.random() * 1_000_000_000) - 500_000_000;
    return Math.trunc(now + offset);
  }

  /** Returns the JSON representation of this type. */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "local-timestamp-millis",
    );
  }
}

/**
 * Logical type for `local-timestamp-micros`.
 * Represents a timestamp in a local time zone, in microseconds.
 */
export class LocalTimestampMicrosLogicalType
  extends LogicalType<bigint, bigint> {
  /**
   * Creates a new LocalTimestampMicrosLogicalType.
   * @param underlying The underlying long type.
   */
  constructor(underlying: LongType) {
    super(underlying);
  }

  /** Checks if the logical type can read from the underlying type. */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof LocalTimestampMicrosLogicalType;
  }

  /** Checks if the value is a valid instance of this logical type. */
  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint";
  }

  /** Converts the logical value to the underlying representation. */
  protected override toUnderlying(value: bigint): bigint {
    return value;
  }

  /** Converts the underlying value to the logical representation. */
  protected override fromUnderlying(value: bigint): bigint {
    return value;
  }

  /** Generates a random value for testing. */
  public override random(): bigint {
    const nowMicros = BigInt(Math.trunc(Date.now())) * 1000n;
    const offset = BigInt(Math.floor(Math.random() * 1_000_000)) - 500_000n;
    return nowMicros + offset;
  }

  /** Returns the JSON representation of this type. */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "local-timestamp-micros",
    );
  }
}

/**
 * Logical type for `local-timestamp-nanos`.
 * Represents a timestamp in a local time zone, in nanoseconds.
 */
export class LocalTimestampNanosLogicalType
  extends LogicalType<bigint, bigint> {
  /**
   * Creates a new LocalTimestampNanosLogicalType.
   * @param underlying The underlying long type.
   */
  constructor(underlying: LongType) {
    super(underlying);
  }

  /** Checks if the logical type can read from the underlying type. */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof LocalTimestampNanosLogicalType;
  }

  /** Checks if the value is a valid instance of this logical type. */
  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint";
  }

  /** Converts the logical value to the underlying representation. */
  protected override toUnderlying(value: bigint): bigint {
    return value;
  }

  /** Converts the underlying value to the logical representation. */
  protected override fromUnderlying(value: bigint): bigint {
    return value;
  }

  /** Generates a random value for testing. */
  public override random(): bigint {
    const nowNanos = BigInt(Math.trunc(Date.now())) * 1_000_000n;
    const offset = BigInt(Math.floor(Math.random() * 1_000_000)) - 500_000n;
    return nowNanos + offset;
  }

  /** Returns the JSON representation of this type. */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "local-timestamp-nanos",
    );
  }
}
