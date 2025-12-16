import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/sync_tap.ts";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";
import type { JSONType } from "../type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";

/**
 * Boolean type.
 */
export class BooleanType extends FixedSizeBaseType<boolean> {
  constructor(validate = true) {
    super(validate);
  }

  /**
   * Validates if the value is a boolean.
   */
  public check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = typeof value === "boolean";
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  /**
   * Reads a boolean value from the tap.
   */
  public async read(tap: ReadableTapLike): Promise<boolean> {
    return await tap.readBoolean();
  }

  /**
   * Writes a boolean value to the tap.
   */
  public async write(
    tap: WritableTapLike,
    value: boolean,
  ): Promise<void> {
    if (!this.validateWrites) {
      await this.writeUnchecked(tap, value);
      return;
    }
    if (typeof value !== "boolean") {
      throwInvalidError([], value, this);
    }
    await tap.writeBoolean(value);
  }

  public override async writeUnchecked(
    tap: WritableTapLike,
    value: boolean,
  ): Promise<void> {
    await tap.writeBoolean(value);
  }

  /**
   * Skips a boolean value in the tap.
   */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipBoolean();
  }

  /**
   * Gets the size in bytes.
   */
  public sizeBytes(): number {
    return 1; // 1 byte
  }

  /**
   * Clones a boolean value from the given value.
   */
  public override cloneFromValue(value: unknown): boolean {
    this.check(value, throwInvalidError, []);
    return value as boolean;
  }

  /**
   * Compares two boolean values.
   */
  public compare(val1: boolean, val2: boolean): number {
    return val1 === val2 ? 0 : val1 ? 1 : -1;
  }

  /**
   * Generates a random boolean value.
   */
  public random(): boolean {
    return Math.random() < 0.5;
  }

  /**
   * Returns the JSON representation of the boolean type.
   */
  public override toJSON(): JSONType {
    return "boolean";
  }

  /**
   * Matches two boolean values from the taps.
   */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchBoolean(tap2);
  }

  /**
   * Reads a boolean value synchronously from the tap.
   */
  public override readSync(tap: SyncReadableTapLike): boolean {
    return tap.readBoolean();
  }

  /**
   * Writes a boolean value synchronously to the tap.
   */
  public override writeSync(
    tap: SyncWritableTapLike,
    value: boolean,
  ): void {
    if (!this.validateWrites) {
      this.writeSyncUnchecked(tap, value);
      return;
    }
    if (typeof value !== "boolean") {
      throwInvalidError([], value, this);
    }
    tap.writeBoolean(value);
  }

  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: boolean,
  ): void {
    tap.writeBoolean(value);
  }

  /**
   * Matches two boolean values synchronously from the taps.
   */
  public override matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    return tap1.matchBoolean(tap2);
  }
}
