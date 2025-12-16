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
 * Null type.
 */
export class NullType extends FixedSizeBaseType<null> {
  constructor(validate = true) {
    super(validate);
  }

  /**
   * Checks if the value is null.
   */
  public check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = value === null;
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  /**
   * Reads a null value from the tap.
   */
  public override async read(_tap: ReadableTapLike): Promise<null> {
    return await Promise.resolve(null);
  }

  /**
   * Writes a null value to the tap.
   */
  public override async write(
    _tap: WritableTapLike,
    value: null,
  ): Promise<void> {
    if (!this.validateWrites) {
      await this.writeUnchecked(_tap, value);
      return;
    }
    if (value !== null) {
      throwInvalidError([], value, this);
    }
    await Promise.resolve();
  }

  public override async writeUnchecked(
    _tap: WritableTapLike,
    _value: null,
  ): Promise<void> {
    await Promise.resolve();
  }

  /**
   * Skips a null value in the tap.
   */
  public override async skip(_tap: ReadableTapLike): Promise<void> {
    // Null takes no space
  }

  /**
   * Gets the size in bytes.
   */
  public sizeBytes(): number {
    return 0; // Null takes no space
  }

  /**
   * Clones a null value from the given value.
   */
  public override cloneFromValue(value: unknown): null {
    this.check(value, throwInvalidError, []);
    return value as null;
  }

  /**
   * Compares two null values.
   */
  public compare(_val1: null, _val2: null): number {
    return 0;
  }

  /**
   * Generates a random null value.
   */
  public random(): null {
    return null;
  }

  /** Returns the JSON representation of the type. */
  public override toJSON(): JSONType {
    return "null";
  }

  /** Compares two null values in the taps. */
  public override async match(
    _tap1: ReadableTapLike,
    _tap2: ReadableTapLike,
  ): Promise<number> {
    return await Promise.resolve(0);
  }

  /**
   * Reads a null value synchronously from the tap.
   */
  public override readSync(_tap: SyncReadableTapLike): null {
    return null;
  }

  /**
   * Writes a null value synchronously to the tap.
   */
  public override writeSync(
    _tap: SyncWritableTapLike,
    value: null,
  ): void {
    if (!this.validateWrites) {
      this.writeSyncUnchecked(_tap, value);
      return;
    }
    if (value !== null) {
      throwInvalidError([], value, this);
    }
  }

  public override writeSyncUnchecked(
    _tap: SyncWritableTapLike,
    _value: null,
  ): void {}

  /**
   * Compares two null values synchronously in the taps.
   */
  public override matchSync(
    _tap1: SyncReadableTapLike,
    _tap2: SyncReadableTapLike,
  ): number {
    return 0;
  }
}
