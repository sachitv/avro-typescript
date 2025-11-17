import type { IntType } from "../primitive/int_type.ts";
import type { LongType } from "../primitive/long_type.ts";
import { LogicalType, withLogicalTypeJSON } from "./logical_type.ts";
import { bigIntToSafeNumber } from "../../serialization/conversion.ts";

const MILLIS_PER_DAY = 86_400_000;
const MICROS_PER_DAY = 86_400_000_000n;

export class DateLogicalType extends LogicalType<number, number> {
  constructor(underlying: IntType) {
    super(underlying);
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof DateLogicalType;
  }

  protected override isInstance(value: unknown): value is number {
    return typeof value === "number" && Number.isInteger(value);
  }

  protected override toUnderlying(value: number): number {
    return value;
  }

  protected override fromUnderlying(value: number): number {
    return value;
  }

  public override random(): number {
    const today = Math.floor(Date.now() / MILLIS_PER_DAY);
    return today + Math.floor(Math.random() * 2000) - 1000;
  }

  public override toJSON() {
    return withLogicalTypeJSON(this.getUnderlyingType().toJSON(), "date");
  }
}

export class TimeMillisLogicalType extends LogicalType<number, number> {
  constructor(underlying: IntType) {
    super(underlying);
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof TimeMillisLogicalType;
  }

  protected override isInstance(value: unknown): value is number {
    return typeof value === "number" && Number.isInteger(value) &&
      value >= 0 && value < MILLIS_PER_DAY;
  }

  protected override toUnderlying(value: number): number {
    return value;
  }

  protected override fromUnderlying(value: number): number {
    return value;
  }

  public override random(): number {
    return Math.floor(Math.random() * MILLIS_PER_DAY);
  }

  public override toJSON() {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "time-millis",
    );
  }
}

export class TimeMicrosLogicalType extends LogicalType<bigint, bigint> {
  constructor(underlying: LongType) {
    super(underlying);
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof TimeMicrosLogicalType;
  }

  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint" && value >= 0n && value < MICROS_PER_DAY;
  }

  protected override toUnderlying(value: bigint): bigint {
    return value;
  }

  protected override fromUnderlying(value: bigint): bigint {
    return value;
  }

  public override random(): bigint {
    return BigInt(Math.floor(Math.random() * Number(MICROS_PER_DAY)));
  }

  public override toJSON() {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "time-micros",
    );
  }
}

export class TimestampMillisLogicalType extends LogicalType<Date, bigint> {
  constructor(underlying: LongType) {
    super(underlying);
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof TimestampMillisLogicalType;
  }

  protected override isInstance(value: unknown): value is Date {
    return value instanceof Date && !Number.isNaN(value.getTime());
  }

  protected override toUnderlying(value: Date): bigint {
    const millis = value.getTime();
    if (!Number.isFinite(millis)) {
      throw new Error("Invalid Date value for timestamp-millis logical type.");
    }
    return BigInt(Math.trunc(millis));
  }

  protected override fromUnderlying(value: bigint): Date {
    const millis = bigIntToSafeNumber(value, "timestamp-millis");
    return new Date(millis);
  }

  public override random(): Date {
    const now = Date.now();
    const offset = Math.floor(Math.random() * 1_000_000_000) - 500_000_000;
    return new Date(now + offset);
  }

  public override compare(val1: Date, val2: Date): number {
    const diff = val1.getTime() - val2.getTime();
    return diff < 0 ? -1 : diff > 0 ? 1 : 0;
  }

  public override toJSON() {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "timestamp-millis",
    );
  }
}

export class TimestampMicrosLogicalType extends LogicalType<bigint, bigint> {
  constructor(underlying: LongType) {
    super(underlying);
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof TimestampMicrosLogicalType;
  }

  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint";
  }

  protected override toUnderlying(value: bigint): bigint {
    return value;
  }

  protected override fromUnderlying(value: bigint): bigint {
    return value;
  }

  public override random(): bigint {
    const nowMicros = BigInt(Math.trunc(Date.now())) * 1000n;
    const offset = BigInt(Math.floor(Math.random() * 1_000_000)) - 500_000n;
    return nowMicros + offset;
  }

  public override toJSON() {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "timestamp-micros",
    );
  }
}

export class TimestampNanosLogicalType extends LogicalType<bigint, bigint> {
  constructor(underlying: LongType) {
    super(underlying);
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof TimestampNanosLogicalType;
  }

  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint";
  }

  protected override toUnderlying(value: bigint): bigint {
    return value;
  }

  protected override fromUnderlying(value: bigint): bigint {
    return value;
  }

  public override random(): bigint {
    const nowNanos = BigInt(Math.trunc(Date.now())) * 1_000_000n;
    const offset = BigInt(Math.floor(Math.random() * 1_000_000)) - 500_000n;
    return nowNanos + offset;
  }

  public override toJSON() {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "timestamp-nanos",
    );
  }
}

export class LocalTimestampMillisLogicalType
  extends LogicalType<number, bigint> {
  constructor(underlying: LongType) {
    super(underlying);
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof LocalTimestampMillisLogicalType;
  }

  protected override isInstance(value: unknown): value is number {
    return typeof value === "number" && Number.isFinite(value) &&
      Number.isInteger(value);
  }

  protected override toUnderlying(value: number): bigint {
    return BigInt(value);
  }

  protected override fromUnderlying(value: bigint): number {
    return bigIntToSafeNumber(value, "local-timestamp-millis");
  }

  public override random(): number {
    const now = Date.now();
    const offset = Math.floor(Math.random() * 1_000_000_000) - 500_000_000;
    return Math.trunc(now + offset);
  }

  public override toJSON() {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "local-timestamp-millis",
    );
  }
}

export class LocalTimestampMicrosLogicalType
  extends LogicalType<bigint, bigint> {
  constructor(underlying: LongType) {
    super(underlying);
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof LocalTimestampMicrosLogicalType;
  }

  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint";
  }

  protected override toUnderlying(value: bigint): bigint {
    return value;
  }

  protected override fromUnderlying(value: bigint): bigint {
    return value;
  }

  public override random(): bigint {
    const nowMicros = BigInt(Math.trunc(Date.now())) * 1000n;
    const offset = BigInt(Math.floor(Math.random() * 1_000_000)) - 500_000n;
    return nowMicros + offset;
  }

  public override toJSON() {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "local-timestamp-micros",
    );
  }
}

export class LocalTimestampNanosLogicalType
  extends LogicalType<bigint, bigint> {
  constructor(underlying: LongType) {
    super(underlying);
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof LocalTimestampNanosLogicalType;
  }

  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint";
  }

  protected override toUnderlying(value: bigint): bigint {
    return value;
  }

  protected override fromUnderlying(value: bigint): bigint {
    return value;
  }

  public override random(): bigint {
    const nowNanos = BigInt(Math.trunc(Date.now())) * 1_000_000n;
    const offset = BigInt(Math.floor(Math.random() * 1_000_000)) - 500_000n;
    return nowNanos + offset;
  }

  public override toJSON() {
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "local-timestamp-nanos",
    );
  }
}
