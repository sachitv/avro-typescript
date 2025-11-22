import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
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

  /** Writes a float value to the tap. */
  public async write(tap: WritableTapLike, value: number): Promise<void> {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    await tap.writeFloat(value);
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
      }(this);
    } else if (writerType instanceof LongType) {
      // Float can promote from long (64-bit to 32-bit float, lossy)
      return new class extends Resolver {
        public override async read(tap: ReadableTapLike): Promise<number> {
          const longValue = await tap.readLong();
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
}
