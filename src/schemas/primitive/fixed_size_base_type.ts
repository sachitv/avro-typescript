import type { ReadableTapLike } from "../../serialization/tap.ts";
import type { SyncReadableTapLike } from "../../serialization/sync_tap.ts";
import { BaseType } from "../base_type.ts";

/**
 * Base class for fixed-size Avro types.
 * Provides optimized serialization for types with known fixed byte sizes.
 */
/**
 * Base class for fixed-size Avro types.
 * Provides optimized serialization for types with known fixed byte sizes.
 */
export abstract class FixedSizeBaseType<T = unknown> extends BaseType<T> {
  constructor(validate = true) {
    super(validate);
  }

  /**
   * Returns the fixed size in bytes for this type.
   * @returns The exact size in bytes.
   */
  public abstract sizeBytes(): number;

  protected override byteLength(_value: T): number {
    return this.sizeBytes();
  }

  /**
   * Skips a fixed-size value by advancing the tap by the fixed size.
   * @param tap The tap to skip from.
   */
  public async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipFixed(this.sizeBytes());
  }

  /**
   * Skips a fixed-size value synchronously by advancing the tap by the fixed size.
   * @param tap The sync tap to skip from.
   */
  public skipSync(tap: SyncReadableTapLike): void {
    tap.skipFixed(this.sizeBytes());
  }
}
