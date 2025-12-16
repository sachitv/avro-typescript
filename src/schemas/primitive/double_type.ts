import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/sync_tap.ts";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";
import type { JSONType, Type } from "../type.ts";
import { Resolver } from "../resolver.ts";
import { IntType } from "./int_type.ts";
import { LongType } from "./long_type.ts";
import { FloatType } from "./float_type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";

/**
 * Double type (64-bit).
 */
export class DoubleType extends FixedSizeBaseType<number> {
  constructor(validate = true) {
    super(validate);
  }

  /** Checks if the value is a valid double. */
  public check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = typeof value === "number";
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  /** Reads a double value from the tap. */
  public override async read(tap: ReadableTapLike): Promise<number> {
    return await tap.readDouble();
  }

  /** Writes a double value to the tap. */
  public override async write(
    tap: WritableTapLike,
    value: number,
  ): Promise<void> {
    if (!this.validateWrites) {
      await this.writeUnchecked(tap, value);
      return;
    }
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    await tap.writeDouble(value);
  }

  public override async writeUnchecked(
    tap: WritableTapLike,
    value: number,
  ): Promise<void> {
    await tap.writeDouble(value);
  }

  /** Skips a double value in the tap. */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipDouble();
  }

  /** Reads a double value from the sync tap. */
  public readSync(tap: SyncReadableTapLike): number {
    return tap.readDouble();
  }

  /** Writes a double value to the sync tap. */
  public writeSync(tap: SyncWritableTapLike, value: number): void {
    if (!this.validateWrites) {
      this.writeSyncUnchecked(tap, value);
      return;
    }
    // Fast path: inline validation for performance
    if (typeof value !== "number") {
      throwInvalidError([], value, this);
    }
    tap.writeDouble(value);
  }

  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: number,
  ): void {
    tap.writeDouble(value);
  }

  /** Skips a double value in the sync tap. */
  public override skipSync(tap: SyncReadableTapLike): void {
    tap.skipDouble();
  }

  /** Matches two sync taps for equality. */
  public matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    return tap1.matchDouble(tap2);
  }

  /**
   * Gets the size in bytes.
   */
  public sizeBytes(): number {
    return 8; // 8 bytes
  }

  /** Clones the value to a number. */
  public override cloneFromValue(value: unknown): number {
    this.check(value, throwInvalidError, []);
    return value as number;
  }

  /**
   * Compares two double values.
   */
  public override compare(val1: number, val2: number): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  /**
   * Generates a random double value.
   */
  public override random(): number {
    return Math.random();
  }

  /** Creates a resolver for the given writer type. */
  public override createResolver(writerType: Type): Resolver {
    if (writerType instanceof IntType) {
      // Double can promote from int (32-bit to 64-bit double)
      return new class extends Resolver {
        public override async read(tap: ReadableTapLike): Promise<number> {
          const intValue = await tap.readInt();
          return intValue;
        }

        public override readSync(tap: SyncReadableTapLike): number {
          const intValue = tap.readInt();
          return intValue;
        }
      }(this);
    } else if (writerType instanceof LongType) {
      // Double can promote from long (64-bit to 64-bit double, lossy for large values)
      return new class extends Resolver {
        public override async read(tap: ReadableTapLike): Promise<number> {
          const longValue = await tap.readLong();
          return Number(longValue);
        }

        public override readSync(tap: SyncReadableTapLike): number {
          const longValue = tap.readLong();
          return Number(longValue);
        }
      }(this);
    } else if (writerType instanceof FloatType) {
      // Double can promote from float (32-bit to 64-bit double)
      return new class extends Resolver {
        public override async read(tap: ReadableTapLike): Promise<number> {
          const floatValue = await tap.readFloat();
          return floatValue;
        }

        public override readSync(tap: SyncReadableTapLike): number {
          const floatValue = tap.readFloat();
          return floatValue;
        }
      }(this);
    } else {
      return super.createResolver(writerType);
    }
  }

  /** Returns the JSON representation of the type. */
  public override toJSON(): JSONType {
    return "double";
  }

  /** Matches two taps for equality. */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchDouble(tap2);
  }
}
