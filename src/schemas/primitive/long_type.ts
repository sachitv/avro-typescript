import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../../serialization/tap.ts";
import {
  type SyncReadableTapLike,
  SyncWritableTap,
  type SyncWritableTapLike,
} from "../../serialization/sync_tap.ts";
import { PrimitiveType } from "./primitive_type.ts";
import type { JSONType, Type } from "../type.ts";
import { Resolver } from "../resolver.ts";
import { IntType } from "./int_type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";
import { calculateVarintSize } from "../../internal/varint.ts";

const MIN_LONG = -(1n << 63n);
const MAX_LONG = (1n << 63n) - 1n;

/**
 * Long type (64-bit).
 */
export class LongType extends PrimitiveType<bigint> {
  constructor(validate = true) {
    super(validate);
  }

  /** Checks if the value is a valid long. */
  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = typeof value === "bigint" && value >= MIN_LONG &&
      value <= MAX_LONG;
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  /** Reads a long value from the tap. */
  public override async read(tap: ReadableTapLike): Promise<bigint> {
    return await tap.readLong();
  }

  /** Writes a long value to the tap. */
  public override async write(
    tap: WritableTapLike,
    value: bigint,
  ): Promise<void> {
    if (!this.validateWrites) {
      await this.writeUnchecked(tap, value);
      return;
    }
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    await tap.writeLong(value);
  }

  public override async writeUnchecked(
    tap: WritableTapLike,
    value: bigint,
  ): Promise<void> {
    await tap.writeLong(value);
  }

  /** Skips a long value in the tap. */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipLong();
  }

  /** Converts a bigint value to its buffer representation. */
  public override async toBuffer(value: bigint): Promise<ArrayBuffer> {
    if (this.validateWrites) {
      this.check(value, throwInvalidError, []);
    }
    // For long, allocate exact size based on value
    const size = calculateVarintSize(value);
    const buf = new ArrayBuffer(size);
    const tap = new WritableTap(buf);
    await this.write(tap, value);
    return buf;
  }

  /** Converts a bigint value to its buffer representation synchronously. */
  public override toSyncBuffer(value: bigint): ArrayBuffer {
    if (this.validateWrites) {
      this.check(value, throwInvalidError, []);
    }
    // For long, allocate exact size based on value
    const size = calculateVarintSize(value);
    const buf = new ArrayBuffer(size);
    const tap = new SyncWritableTap(buf);
    this.writeSync(tap, value);
    return buf;
  }

  /**
   * Compares two long values.
   */
  public override compare(val1: bigint, val2: bigint): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  /**
   * Generates a random long value.
   */
  public override random(): bigint {
    return BigInt(Math.floor(Math.random() * 1000));
  }

  /** Clones and validates a value as a bigint. */
  public override cloneFromValue(value: unknown): bigint {
    if (typeof value === "bigint") {
      this.check(value, throwInvalidError, []);
      return value;
    }

    if (typeof value === "number" && Number.isInteger(value)) {
      const candidate = BigInt(value);
      this.check(candidate, throwInvalidError, []);
      return candidate;
    }

    throwInvalidError([], value, this);
  }

  /** Creates a resolver for reading from the writer type. */
  public override createResolver(writerType: Type): Resolver {
    if (writerType instanceof IntType) {
      // Long can promote from int (32-bit to 64-bit)
      return new class extends Resolver {
        public override async read(tap: ReadableTapLike): Promise<bigint> {
          const intValue = await tap.readInt();
          return BigInt(intValue);
        }

        public override readSync(tap: SyncReadableTapLike): bigint {
          const intValue = tap.readInt();
          return BigInt(intValue);
        }
      }(this);
    } else {
      return super.createResolver(writerType);
    }
  }

  /** Returns the JSON representation of the type. */
  public override toJSON(): JSONType {
    return "long";
  }

  /** Compares two taps for long equality. */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchLong(tap2);
  }

  /** Reads a long value synchronously from the tap. */
  public override readSync(tap: SyncReadableTapLike): bigint {
    return tap.readLong();
  }

  /** Writes a long value synchronously to the tap. */
  public override writeSync(
    tap: SyncWritableTapLike,
    value: bigint,
  ): void {
    if (!this.validateWrites) {
      this.writeSyncUnchecked(tap, value);
      return;
    }
    // Fast path: inline validation for performance
    if (
      typeof value !== "bigint" || value < -(1n << 63n) ||
      value > (1n << 63n) - 1n
    ) {
      throwInvalidError([], value, this);
    }
    tap.writeLong(value);
  }

  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: bigint,
  ): void {
    tap.writeLong(value);
  }

  /** Skips a long value synchronously in the tap. */
  public override skipSync(tap: SyncReadableTapLike): void {
    tap.skipLong();
  }

  /** Compares two taps synchronously for long equality. */
  public override matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    return tap1.matchLong(tap2);
  }
}
