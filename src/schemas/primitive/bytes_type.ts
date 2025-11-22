import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../../serialization/tap.ts";
import { PrimitiveType } from "./primitive_type.ts";
import type { JSONType, Type } from "./../type.ts";
import { Resolver } from "./../resolver.ts";
import { type ErrorHook, throwInvalidError } from "./../error.ts";
import { calculateVarintSize } from "../../internal/varint.ts";

/**
 * Bytes type.
 */
export class BytesType extends PrimitiveType<Uint8Array> {
  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = value instanceof Uint8Array;
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  public override async read(tap: ReadableTapLike): Promise<Uint8Array> {
    return await tap.readBytes();
  }

  public override async write(
    tap: WritableTapLike,
    value: Uint8Array,
  ): Promise<void> {
    if (!(value instanceof Uint8Array)) {
      throwInvalidError([], value, this);
    }
    await tap.writeBytes(value);
  }

  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipBytes();
  }

  public override async toBuffer(value: Uint8Array): Promise<ArrayBuffer> {
    this.check(value, throwInvalidError, []);
    // Pre-allocate buffer based on value length for efficiency
    const lengthSize = calculateVarintSize(value.length);
    const totalSize = lengthSize + value.length;
    const buf = new ArrayBuffer(totalSize);
    const tap = new WritableTap(buf);
    await this.write(tap, value);
    return buf;
  }

  public override createResolver(writerType: Type): Resolver {
    if (writerType.toJSON() === "string") {
      // Bytes can promote from string. We use an anonymous class here to avoid a
      // cyclic dependency between this file and the string type file.
      return new class extends Resolver {
        public override async read(
          tap: ReadableTapLike,
        ): Promise<Uint8Array> {
          const str = await tap.readString();
          // Convert string to bytes (assuming UTF-8)
          const encoder = new TextEncoder();
          return encoder.encode(str);
        }
      }(this);
    } else {
      return super.createResolver(writerType);
    }
  }

  public override compare(val1: Uint8Array, val2: Uint8Array): number {
    const len1 = val1.length;
    const len2 = val2.length;
    const len = Math.min(len1, len2);
    for (let i = 0; i < len; i++) {
      if (val1[i] !== val2[i]) {
        return val1[i] < val2[i] ? -1 : 1;
      }
    }
    return len1 < len2 ? -1 : len1 > len2 ? 1 : 0;
  }

  public override cloneFromValue(value: unknown): Uint8Array {
    let bytes: Uint8Array;
    if (value instanceof Uint8Array) {
      bytes = value;
    } else if (typeof value === "string") {
      bytes = BytesType.#fromJsonString(value);
    } else {
      throwInvalidError([], value, this);
    }
    this.check(bytes, throwInvalidError, []);
    return new Uint8Array(bytes);
  }

  public override random(): Uint8Array {
    // Generate at least one byte.
    const len = Math.ceil(Math.random() * 31) + 1;
    const buf = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      buf[i] = Math.floor(Math.random() * 256);
    }
    return buf;
  }

  static #fromJsonString(value: string): Uint8Array {
    const bytes = new Uint8Array(value.length);
    for (let i = 0; i < value.length; i++) {
      bytes[i] = value.charCodeAt(i) & 0xff;
    }
    return bytes;
  }

  public override toJSON(): JSONType {
    return "bytes";
  }

  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchBytes(tap2);
  }
}
