import { Tap } from "../serialization/tap.ts";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";
import { type JSONType, Type } from "./type.ts";
import { Resolver } from "./resolver.ts";
import { IntType } from "./int_type.ts";
import { LongType } from "./long_type.ts";
import { ErrorHook, throwInvalidError } from "./error.ts";

/**
 * Float type (32-bit).
 */
export class FloatType extends FixedSizeBaseType<number> {
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

  public read(tap: Tap): number {
    const val = tap.readFloat();
    if (val === undefined) {
      throw new Error("Insufficient data for float");
    }
    return val;
  }

  public write(tap: Tap, value: number): void {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    tap.writeFloat(value);
  }

  public override skip(tap: Tap): void {
    tap.skipFloat();
  }

  public sizeBytes(): number {
    return 4; // 4 bytes
  }

  public override clone(value: number): number {
    this.check(value, throwInvalidError, []);
    return value;
  }

  public compare(val1: number, val2: number): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  public random(): number {
    return Math.random() * 1000;
  }

  public override createResolver(writerType: Type): Resolver {
    if (writerType instanceof IntType) {
      // Float can promote from int (32-bit to 32-bit float)
      return new class extends Resolver {
        public override read(tap: Tap): number {
          const intValue = tap.readInt();
          return intValue;
        }
      }(this);
    } else if (writerType instanceof LongType) {
      // Float can promote from long (64-bit to 32-bit float, lossy)
      return new class extends Resolver {
        public override read(tap: Tap): number {
          const longValue = tap.readLong();
          return Number(longValue);
        }
      }(this);
    } else {
      return super.createResolver(writerType);
    }
  }

  public override toJSON(): JSONType {
    return "float";
  }
}
