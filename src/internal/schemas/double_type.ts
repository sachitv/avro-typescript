import { Tap } from "../serialization/tap.ts";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";
import { type JSONType, Type } from "./type.ts";
import { Resolver } from "./resolver.ts";
import { IntType } from "./int_type.ts";
import { LongType } from "./long_type.ts";
import { FloatType } from "./float_type.ts";
import { ErrorHook, throwInvalidError } from "./error.ts";

/**
 * Double type (64-bit).
 */
export class DoubleType extends FixedSizeBaseType<number> {
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

  public override read(tap: Tap): number {
    const val = tap.readDouble();
    if (val === undefined) {
      throw new Error("Insufficient data for double");
    }
    return val;
  }

  public override write(tap: Tap, value: number): void {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    tap.writeDouble(value);
  }

  public override skip(tap: Tap): void {
    tap.skipDouble();
  }

  public sizeBytes(): number {
    return 8; // 8 bytes
  }

  public override clone(value: number): number {
    this.check(value, throwInvalidError, []);
    return value;
  }

  public override compare(val1: number, val2: number): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  public override random(): number {
    return Math.random();
  }

  public override createResolver(writerType: Type): Resolver {
    if (writerType instanceof IntType) {
      // Double can promote from int (32-bit to 64-bit double)
      return new class extends Resolver {
        public override read(tap: Tap): number {
          const intValue = tap.readInt();
          return intValue;
        }
      }(this);
    } else if (writerType instanceof LongType) {
      // Double can promote from long (64-bit to 64-bit double, lossy for large values)
      return new class extends Resolver {
        public override read(tap: Tap): number {
          const longValue = tap.readLong();
          return Number(longValue);
        }
      }(this);
    } else if (writerType instanceof FloatType) {
      // Double can promote from float (32-bit to 64-bit double)
      return new class extends Resolver {
        public override read(tap: Tap): number {
          const floatValue = tap.readFloat();
          if (floatValue === undefined) {
            throw new Error("Insufficient data for float");
          }
          return floatValue;
        }
      }(this);
    } else {
      return super.createResolver(writerType);
    }
  }

  public override toJSON(): JSONType {
    return "double";
  }
}
