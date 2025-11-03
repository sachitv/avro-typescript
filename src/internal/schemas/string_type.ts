import { Tap } from "../serialization/tap.ts";
import { PrimitiveType } from "./primitive_type.ts";
import { Type } from "./type.ts";
import { Resolver } from "./resolver.ts";
import { ErrorHook, throwInvalidError } from "./error.ts";
import { calculateVarintSize } from "./varint.ts";
import { decode, encode } from "../serialization/text_encoding.ts";

/**
 * String type.
 */
export class StringType extends PrimitiveType<string> {
  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = typeof value === "string";
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  public override toBuffer(value: string): ArrayBuffer {
    this.check(value, throwInvalidError, []);
    const strBytes = encode(value);
    const lengthSize = calculateVarintSize(strBytes.length);
    const buf = new ArrayBuffer(lengthSize + strBytes.length);
    const tap = new Tap(buf);
    this.write(tap, value);
    return buf;
  }

  public override read(tap: Tap): string {
    const val = tap.readString();
    if (val === undefined) {
      throw new Error("Insufficient data for string");
    }
    return val;
  }

  public override write(tap: Tap, value: string): void {
    if (typeof value !== "string") {
      throwInvalidError([], value, this);
    }
    tap.writeString(value);
  }

  public override skip(tap: Tap): void {
    tap.skipString();
  }

  public override compare(val1: string, val2: string): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  public override random(): string {
    return Math.random().toString(36).substring(2, 10);
  }

  public override createResolver(writerType: Type): Resolver {
    if (writerType.toJSON() === "bytes") {
      // String can promote from bytes. We use an anonymous class here to avoid a
      // cyclic dependency between this file and the bytes type file.
      return new class extends Resolver {
        public override read(tap: Tap): string {
          const bytes = tap.readBytes();
          if (bytes === undefined) {
            throw new Error("Insufficient data for bytes");
          }
          // Convert bytes to string (assuming UTF-8)
          return decode(bytes);
        }
      }(this);
    } else {
      return super.createResolver(writerType);
    }
  }

  public toJSON(): string {
    return "string";
  }
}
