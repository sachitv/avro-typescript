import { Tap } from '../serialization/tap.ts';
import { BaseType } from './base_type.ts';
import { throwInvalidError } from './error.ts';

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
    public toBuffer(value: T): ArrayBuffer {
        this.check(value, throwInvalidError, []);
        const size = this.sizeBytes();
        const buf = new ArrayBuffer(size);
        const tap = new Tap(buf);
        this.write(tap, value);
        return buf;
    }
}
