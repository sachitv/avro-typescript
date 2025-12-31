import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/tap_sync.ts";
import { PrimitiveType } from "./primitive_type.ts";
import type { JSONType } from "../type.ts";
import { calculateVarintSize } from "../../internal/varint.ts";
import type { ErrorHook } from "../error.ts";

/**
 * Int type (32-bit).
 */
export class IntType extends PrimitiveType<number> {
  /** Creates a new int type. */
  constructor(validate = true) {
    super(validate);
  }

  /** Checks if the value is a valid 32-bit integer. */
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

  /** Reads a 32-bit integer from the tap. */
  public override async read(tap: ReadableTapLike): Promise<number> {
    return await tap.readInt();
  }

  /** Writes a 32-bit integer to the tap without validation. */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: number,
  ): Promise<void> {
    await tap.writeInt(value);
  }

  /** Returns the encoded byte length of the given value. */
  protected override byteLength(value: number): number {
    return calculateVarintSize(value);
  }

  /** Skips a 32-bit integer in the tap. */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipInt();
  }

  /**
   * Compares two int values.
   */
  public override compare(val1: number, val2: number): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  /**
   * Generates a random int value.
   */
  public override random(): number {
    return Math.floor(Math.random() * 1000);
  }

  /** Returns the JSON schema representation for int type. */
  public override toJSON(): JSONType {
    return "int";
  }

  /** Matches two 32-bit integers in the taps. */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchInt(tap2);
  }

  /** Reads a 32-bit integer synchronously from the tap. */
  public override readSync(tap: SyncReadableTapLike): number {
    return tap.readInt();
  }

  /** Writes a 32-bit integer synchronously to the tap without validation. */
  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: number,
  ): void {
    tap.writeInt(value);
  }

  /** Skips a 32-bit integer synchronously in the tap. */
  public override skipSync(tap: SyncReadableTapLike): void {
    tap.skipInt();
  }

  /** Matches two 32-bit integers synchronously in the taps. */
  public override matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    return tap1.matchInt(tap2);
  }
}
