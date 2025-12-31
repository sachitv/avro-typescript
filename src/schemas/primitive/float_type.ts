import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/tap_sync.ts";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";
import type { JSONType, Type } from "../type.ts";
import { Resolver } from "../resolver.ts";
import { IntType } from "./int_type.ts";
import { LongType } from "./long_type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";

/**
 * Float type (32-bit).
 */
export class FloatType extends FixedSizeBaseType<number> {
  /** Creates a new float type. */
  constructor(validate = true) {
    super(validate);
  }

  /** Checks if the value is a valid float. */
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

  /** Reads a float value from the tap. */
  public async read(tap: ReadableTapLike): Promise<number> {
    return await tap.readFloat();
  }

  /** Reads a float value from the sync tap. */
  public readSync(tap: SyncReadableTapLike): number {
    return tap.readFloat();
  }

  /** Writes a float value to the tap without validation. */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: number,
  ): Promise<void> {
    await tap.writeFloat(value);
  }

  /** Writes a float value synchronously to the tap without validation. */
  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: number,
  ): void {
    tap.writeFloat(value);
  }

  /** Skips a float value in the tap. */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipFloat();
  }

  /**
   * Gets the size in bytes.
   */
  public sizeBytes(): number {
    return 4; // 4 bytes
  }

  /** Clones a value to a float. */
  public override cloneFromValue(value: unknown): number {
    this.check(value, throwInvalidError, []);
    return value as number;
  }

  /**
   * Compares two float values.
   */
  public compare(val1: number, val2: number): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  /**
   * Generates a random float value.
   */
  public random(): number {
    return Math.random() * 1000;
  }

  /** Creates a resolver for schema evolution. */
  public override createResolver(writerType: Type): Resolver {
    if (writerType instanceof IntType) {
      // Float can promote from int (32-bit to 32-bit float)
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
      // Float can promote from long (64-bit to 32-bit float, lossy)
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
    } else {
      return super.createResolver(writerType);
    }
  }

  /** Returns the JSON representation. */
  public override toJSON(): JSONType {
    return "float";
  }

  /** Matches float values between taps. */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchFloat(tap2);
  }

  /** Matches float values between sync taps. */
  public matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    return tap1.matchFloat(tap2);
  }
}
