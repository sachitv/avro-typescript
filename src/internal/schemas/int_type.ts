import { Tap } from '../serialization/tap.ts';
import { PrimitiveType } from './primitive_type.ts';
import { calculateVarintSize } from './varint.ts';
import { ErrorHook, throwInvalidError } from './error.ts';

/**
 * Int type (32-bit).
 */
export class IntType extends PrimitiveType<number> {
  public override check(value: unknown, errorHook?: ErrorHook, path: string[] = []): boolean {
    const isValid = typeof value === 'number' && Number.isInteger(value) && value >= -2147483648 && value <= 2147483647;
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  public override read(tap: Tap): number {
    return tap.readInt();
  }

  public override write(tap: Tap, value: number): void {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    tap.writeInt(value);
  }

  public override toBuffer(value: number): ArrayBuffer {
    // For int, allocate exact size based on value
    const size = calculateVarintSize(value);
    const buf = new ArrayBuffer(size);
    const tap = new Tap(buf);
    this.write(tap, value);
    return buf;
  }

  public override compare(val1: number, val2: number): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  public override random(): number {
    return Math.floor(Math.random() * 1000);
  }

  public override toJSON(): string {
    return "int";
  }
}