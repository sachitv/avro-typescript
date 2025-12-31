import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/tap_sync.ts";
import { PrimitiveType } from "./primitive_type.ts";
import type { JSONType, Type } from "./../type.ts";
import { Resolver } from "./../resolver.ts";
import { type ErrorHook, throwInvalidError } from "./../error.ts";
import { calculateVarintSize } from "../../internal/varint.ts";

/**
 * Bytes type.
 */
export class BytesType extends PrimitiveType<Uint8Array> {
  /** Creates a new bytes type. */
  constructor(validate = true) {
    super(validate);
  }

  /** Checks if the value is a valid bytes array. */
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

  /**
   * Reads a byte array from the tap.
   */
  public override async read(tap: ReadableTapLike): Promise<Uint8Array> {
    return await tap.readBytes();
  }

  /**
   * Reads a byte array from the sync tap.
   */
  public override readSync(tap: SyncReadableTapLike): Uint8Array {
    return tap.readBytes();
  }

  /** Writes a byte array to the tap without validation. */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: Uint8Array,
  ): Promise<void> {
    await tap.writeBytes(value);
  }

  /** Returns the encoded byte length of the given value. */
  protected override byteLength(value: Uint8Array): number {
    return calculateVarintSize(value.length) + value.length;
  }

  /** Writes a byte array synchronously to the tap without validation. */
  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: Uint8Array,
  ): void {
    tap.writeBytes(value);
  }

  /**
   * Skips a byte array in the tap.
   */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipBytes();
  }

  /**
   * Skips a byte array in the sync tap.
   */
  public override skipSync(tap: SyncReadableTapLike): void {
    tap.skipBytes();
  }

  /**
   * Creates a resolver for the writer type.
   */
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

        public override readSync(
          tap: SyncReadableTapLike,
        ): Uint8Array {
          const str = tap.readString();
          // Convert string to bytes (assuming UTF-8)
          const encoder = new TextEncoder();
          return encoder.encode(str);
        }
      }(this);
    } else {
      return super.createResolver(writerType);
    }
  }

  /**
   * Compares two byte arrays.
   */
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

  /**
   * Clones a byte array value.
   */
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

  /**
   * Generates a random byte array.
   */
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

  /** Returns the JSON representation of the type. */
  public override toJSON(): JSONType {
    return "bytes";
  }

  /** Matches bytes between two taps. */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchBytes(tap2);
  }

  /** Matches bytes between two sync taps. */
  public override matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    return tap1.matchBytes(tap2);
  }
}
