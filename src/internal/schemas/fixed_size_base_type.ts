import { type ReadableTapLike, WritableTap } from "../serialization/tap.ts";
import { BaseType } from "./base_type.ts";
import { throwInvalidError } from "./error.ts";

/**
 * Base class for fixed-size Avro types.
 * Provides optimized serialization for types with known fixed byte sizes.
 */
export abstract class FixedSizeBaseType<T = unknown> extends BaseType<T> {
  /**
   * Returns the fixed size in bytes for this type.
   * @returns The exact size in bytes.
   */
  public abstract sizeBytes(): number;

  /**
   * Serializes a value into an ArrayBuffer using the exact fixed size.
   * @param value The value to serialize.
   * @returns The serialized ArrayBuffer.
   */
  public override async toBuffer(value: T): Promise<ArrayBuffer> {
    this.check(value, throwInvalidError, []);
    const size = this.sizeBytes();
    const buf = new ArrayBuffer(size);
    const tap = new WritableTap(buf);
    await this.write(tap, value);
    return buf;
  }

  /**
   * Skips a fixed-size value by advancing the tap by the fixed size.
   * @param tap The tap to skip from.
   */
  public async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipFixed(this.sizeBytes());
  }
}
