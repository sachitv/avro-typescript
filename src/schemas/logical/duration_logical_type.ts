import type { FixedType } from "../complex/fixed_type.ts";
import {
  type LogicalType,
  NamedLogicalType,
  withLogicalTypeJSON,
} from "./logical_type.ts";
import type { JSONType } from "../type.ts";

/**
 * Represents a duration value with months, days, and milliseconds.
 */
export interface DurationValue {
  /** Number of months. */
  months: number;
  /** Number of days. */
  days: number;
  /** Number of milliseconds. */
  millis: number;
}

const MAX_UINT32 = 0xffffffff;

/**
 * Logical type for `duration`.
 * Represents a duration of time.
 */
export class DurationLogicalType
  extends NamedLogicalType<DurationValue, Uint8Array> {
  /**
   * Creates a new DurationLogicalType.
   * @param underlying The underlying fixed type (must be 12 bytes).
   */
  constructor(underlying: FixedType) {
    super(underlying);
    if (underlying.getSize() !== 12) {
      throw new Error("Duration logical type requires fixed size of 12 bytes.");
    }
  }

  /**
   * Checks if this logical type can read from the given writer logical type.
   * @param writer The writer logical type.
   * @returns True if compatible.
   */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof DurationLogicalType;
  }

  /**
   * Checks if the value is an instance of DurationValue.
   * @param value The value to check.
   * @returns True if the value is a DurationValue.
   */
  protected override isInstance(value: unknown): value is DurationValue {
    return isDurationValue(value);
  }

  /**
   * Converts the logical DurationValue to its underlying Uint8Array representation.
   * @param value The DurationValue to convert.
   * @returns The Uint8Array representation.
   */
  protected override toUnderlying(value: DurationValue): Uint8Array {
    // The value is always valid here since isInstance ensures it.
    const buffer = new ArrayBuffer(12);
    const view = new DataView(buffer);
    view.setUint32(0, value.months, true);
    view.setUint32(4, value.days, true);
    view.setUint32(8, value.millis, true);
    return new Uint8Array(buffer);
  }

  /**
   * Converts the underlying Uint8Array to a DurationValue.
   * @param value The underlying bytes (must be 12 bytes).
   * @returns The duration value.
   */
  protected override fromUnderlying(value: Uint8Array): DurationValue {
    if (value.length !== 12) {
      throw new Error("Duration bytes must be 12 bytes long.");
    }
    const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
    return {
      months: view.getUint32(0, true),
      days: view.getUint32(4, true),
      millis: view.getUint32(8, true),
    };
  }

  /**
   * Compares two DurationValue instances lexicographically.
   * @param a The first duration value.
   * @param b The second duration value.
   * @returns -1 if a < b, 0 if equal, 1 if a > b.
   */
  public override compare(a: DurationValue, b: DurationValue): number {
    if (a.months !== b.months) {
      return a.months < b.months ? -1 : 1;
    }
    if (a.days !== b.days) {
      return a.days < b.days ? -1 : 1;
    }
    if (a.millis !== b.millis) {
      return a.millis < b.millis ? -1 : 1;
    }
    return 0;
  }

  /**
   * Generates a random DurationValue.
   * @returns A random duration value.
   */
  public override random(): DurationValue {
    return {
      months: Math.floor(Math.random() * 1200),
      days: Math.floor(Math.random() * 365),
      millis: Math.floor(Math.random() * 86_400_000),
    };
  }

  /**
   * Returns the JSON representation of this type.
   * @returns The JSON type.
   */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(this.getUnderlyingType().toJSON(), "duration");
  }
}

function isDurationValue(value: unknown): value is DurationValue {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const maybe = value as Partial<DurationValue>;
  return isUint32(maybe.months) && isUint32(maybe.days) &&
    isUint32(maybe.millis);
}

function isUint32(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) &&
    value >= 0 && value <= MAX_UINT32;
}
