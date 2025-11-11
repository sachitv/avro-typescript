import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../serialization/tap.ts";
import { PrimitiveType } from "./primitive_type.ts";
import type { JSONType } from "./type.ts";
import { calculateVarintSize } from "./varint.ts";
import { type ErrorHook, throwInvalidError } from "./error.ts";

/**
 * Int type (32-bit).
 */
export class IntType extends PrimitiveType<number> {
  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = typeof value === "number" && Number.isInteger(value) &&
      value >= -2147483648 && value <= 2147483647;
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  public override async read(tap: ReadableTapLike): Promise<number> {
    return await tap.readInt();
  }

  public override async write(
    tap: WritableTapLike,
    value: number,
  ): Promise<void> {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    await tap.writeInt(value);
  }

  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipInt();
  }

  public override async toBuffer(value: number): Promise<ArrayBuffer> {
    this.check(value, throwInvalidError, []);
    // For int, allocate exact size based on value
    const size = calculateVarintSize(value);
    const buf = new ArrayBuffer(size);
    const tap = new WritableTap(buf);
    await this.write(tap, value);
    return buf;
  }

  public override compare(val1: number, val2: number): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  public override random(): number {
    return Math.floor(Math.random() * 1000);
  }

  public override toJSON(): JSONType {
    return "int";
  }

  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchInt(tap2);
  }
}
