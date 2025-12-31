import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/sync_tap.ts";
import { PrimitiveType } from "./primitive_type.ts";
import type { JSONType, Type } from "../type.ts";
import { Resolver } from "../resolver.ts";
import type { ErrorHook } from "../error.ts";
import { decode, utf8ByteLength } from "../../serialization/text_encoding.ts";
import { calculateVarintSize } from "../../internal/varint.ts";

/**
 * String type.
 */
export class StringType extends PrimitiveType<string> {
  constructor(validate = true) {
    super(validate);
  }

  /** Checks if the value is a valid string. */
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

  /** Reads a string value from the tap. */
  public override async read(tap: ReadableTapLike): Promise<string> {
    return await tap.readString();
  }

  public override async writeUnchecked(
    tap: WritableTapLike,
    value: string,
  ): Promise<void> {
    await tap.writeString(value);
  }

  protected override byteLength(value: string): number {
    const length = utf8ByteLength(value);
    return calculateVarintSize(length) + length;
  }

  /** Skips a string value in the tap. */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipString();
  }

  /**
   * Compares two string values.
   */
  public override compare(val1: string, val2: string): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  /**
   * Generates a random string value.
   */
  public override random(): string {
    return Math.random().toString(36).substring(2, 10);
  }

  /** Creates a resolver for reading from a writer type. */
  public override createResolver(writerType: Type): Resolver {
    if (writerType.toJSON() === "bytes") {
      // String can promote from bytes. We use an anonymous class here to avoid a
      // cyclic dependency between this file and the bytes type file.
      return new class extends Resolver {
        public override async read(tap: ReadableTapLike): Promise<string> {
          const bytes = await tap.readBytes();
          // Convert bytes to string (assuming UTF-8)
          return decode(bytes);
        }

        public override readSync(tap: SyncReadableTapLike): string {
          const bytes = tap.readBytes();
          // Convert bytes to string (assuming UTF-8)
          return decode(bytes);
        }
      }(this);
    } else {
      return super.createResolver(writerType);
    }
  }

  /** Returns the JSON representation of the string type. */
  public override toJSON(): JSONType {
    return "string";
  }

  /** Matches two readable taps for string equality. */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchString(tap2);
  }

  /** Reads a string value synchronously from the tap. */
  public override readSync(tap: SyncReadableTapLike): string {
    return tap.readString();
  }

  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: string,
  ): void {
    tap.writeString(value);
  }

  /** Skips a string value synchronously in the tap. */
  public override skipSync(tap: SyncReadableTapLike): void {
    tap.skipString();
  }

  /** Matches two readable taps synchronously for string equality. */
  public override matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    return tap1.matchString(tap2);
  }
}
