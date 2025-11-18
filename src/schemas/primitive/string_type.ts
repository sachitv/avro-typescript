import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../../serialization/tap.ts";
import { PrimitiveType } from "./primitive_type.ts";
import type { JSONType, Type } from "../type.ts";
import { Resolver } from "../resolver.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";
import { calculateVarintSize } from "../../internal/varint.ts";
import { decode, encode } from "../../serialization/text_encoding.ts";

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

  public override async toBuffer(value: string): Promise<ArrayBuffer> {
    this.check(value, throwInvalidError, []);
    const strBytes = encode(value);
    const lengthSize = calculateVarintSize(strBytes.length);
    const buf = new ArrayBuffer(lengthSize + strBytes.length);
    const tap = new WritableTap(buf);
    await this.write(tap, value);
    return buf;
  }

  public override async read(tap: ReadableTapLike): Promise<string> {
    const val = await tap.readString();
    if (val === undefined) {
      throw new Error("Insufficient data for string");
    }
    return val;
  }

  public override async write(
    tap: WritableTapLike,
    value: string,
  ): Promise<void> {
    if (typeof value !== "string") {
      throwInvalidError([], value, this);
    }
    await tap.writeString(value);
  }

  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipString();
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
        public override async read(tap: ReadableTapLike): Promise<string> {
          const bytes = await tap.readBytes();
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

  public override toJSON(): JSONType {
    return "string";
  }

  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchString(tap2);
  }
}
