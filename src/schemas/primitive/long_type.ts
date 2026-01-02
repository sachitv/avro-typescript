import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/tap_sync.ts";
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
  /** Creates a new long type. */
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
      errorHook(path.slice(), value, this);
    }
    return isValid;
  }

  /** Reads a long value from the tap. */
  public override async read(tap: ReadableTapLike): Promise<bigint> {
    return await tap.readLong();
  }

  /** Writes a long value to the tap without validation. */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: bigint,
  ): Promise<void> {
    await tap.writeLong(value);
  }

  /** Returns the encoded byte length of the given value. */
  protected override byteLength(value: bigint): number {
    return calculateVarintSize(value);
  }

  /** Skips a long value in the tap. */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipLong();
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

  /** Writes a long value synchronously to the tap without validation. */
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
