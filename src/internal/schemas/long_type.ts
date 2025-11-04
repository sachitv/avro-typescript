import { Tap } from "../serialization/tap.ts";
import { PrimitiveType } from "./primitive_type.ts";
import { type JSONType, Type } from "./type.ts";
import { Resolver } from "./resolver.ts";
import { IntType } from "./int_type.ts";
import { ErrorHook, throwInvalidError } from "./error.ts";
import { calculateVarintSize } from "./varint.ts";

const MIN_LONG = -(1n << 63n);
const MAX_LONG = (1n << 63n) - 1n;

/**
 * Long type (64-bit).
 */
export class LongType extends PrimitiveType<bigint> {
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

  public override read(tap: Tap): bigint {
    return tap.readLong();
  }

  public override write(tap: Tap, value: bigint): void {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    tap.writeLong(value);
  }

  public override skip(tap: Tap): void {
    tap.skipLong();
  }

  public override toBuffer(value: bigint): ArrayBuffer {
    this.check(value, throwInvalidError, []);
    // For long, allocate exact size based on value
    const size = calculateVarintSize(value);
    const buf = new ArrayBuffer(size);
    const tap = new Tap(buf);
    this.write(tap, value);
    return buf;
  }

  public override compare(val1: bigint, val2: bigint): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  public override random(): bigint {
    return BigInt(Math.floor(Math.random() * 1000));
  }

  public override createResolver(writerType: Type): Resolver {
    if (writerType instanceof IntType) {
      // Long can promote from int (32-bit to 64-bit)
      return new class extends Resolver {
        public override read(tap: Tap): bigint {
          const intValue = tap.readInt();
          return BigInt(intValue);
        }
      }(this);
    } else {
      return super.createResolver(writerType);
    }
  }

  public override toJSON(): JSONType {
    return "long";
  }
}
