import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../serialization/tap.ts";
import {
  type SyncReadableTapLike,
  SyncWritableTap,
  type SyncWritableTapLike,
} from "../serialization/sync_tap.ts";
import { CountingWritableTap } from "../serialization/counting_writable_tap.ts";
import { SyncCountingWritableTap } from "../serialization/sync_counting_writable_tap.ts";
import type { Resolver } from "./resolver.ts";
import { type ErrorHook, throwInvalidError } from "./error.ts";

/**
 * Options for validation, including an optional error hook.
 */
export type IsValidOptions = { errorHook?: ErrorHook };

type InnerJSONType = (string | Record<string, unknown>)[];

/**
 * Represents a JSON value used in Avro schema definitions.
 */
export type JSONType =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JSONType }
  | JSONType[];

/**
 * Pure abstract base class for all Avro schema types.
 * Provides the common interface for serialization, deserialization, validation, and cloning.
 */
export abstract class Type<T = unknown> {
  protected readonly validateWrites: boolean;

  protected constructor(validate = true) {
    this.validateWrites = validate;
  }

  /**
   * Serializes a value into an ArrayBuffer using the schema.
   * @param value The value to serialize.
   * @returns The serialized ArrayBuffer.
   */
  public async toBuffer(value: T): Promise<ArrayBuffer> {
    if (this.validateWrites) {
      this.check(value, throwInvalidError, []);
    }
    const sizeHint = this.byteLength(value);
    if (sizeHint !== undefined) {
      const buffer = new ArrayBuffer(assertValidSize(sizeHint));
      const tap = new WritableTap(buffer);
      await this.writeUnchecked(tap, value);
      return buffer;
    }
    const countingTap = new CountingWritableTap();
    await this.writeUnchecked(countingTap, value);
    const buffer = new ArrayBuffer(countingTap.getPos());
    const tap = new WritableTap(buffer);
    await this.writeUnchecked(tap, value);
    return buffer;
  }

  /**
   * Deserializes an ArrayBuffer into a value using the schema.
   * @param buffer The ArrayBuffer to deserialize.
   * @returns The deserialized value.
   */
  public abstract fromBuffer(buffer: ArrayBuffer): Promise<T>;

  /**
   * Validates if a value conforms to the schema.
   * @param value The value to validate.
   * @param opts Optional validation options.
   * @returns True if valid, false otherwise.
   */
  public abstract isValid(value: unknown, opts?: IsValidOptions): boolean;

  /**
   * Creates a deep clone of the value.
   * @param value The value to clone.
   * @returns The cloned value.
   */
  public abstract cloneFromValue(value: unknown): T;

  /**
   * Creates a resolver for schema evolution from a writer type to this reader type.
   * @param writerType The writer schema type.
   * @returns A resolver for reading the writer type as this type.
   */
  public abstract createResolver(writerType: Type): Resolver;

  /**
   * Writes a value to the tap. Must be implemented by subclasses.
   * @param tap The tap to write to.
   * @param value The value to write.
   */
  public async write(tap: WritableTapLike, value: T): Promise<void> {
    if (this.validateWrites) {
      this.check(value, throwInvalidError, []);
    }
    await this.writeUnchecked(tap, value);
  }

  /**
   * Writes a value to the tap without performing runtime validation.
   *
   * This is used to implement `createType(schema, { validate: false })`
   * efficiently and recursively. The default implementation delegates to
   * {@link write}.
   */
  public abstract writeUnchecked(
    tap: WritableTapLike,
    value: T,
  ): Promise<void>;

  /**
   * Reads a value from the tap. Must be implemented by subclasses.
   * @param tap The tap to read from.
   * @returns The read value.
   */
  public abstract read(tap: ReadableTapLike): Promise<T>;

  /**
   * Skips a value in the tap. Must be implemented by subclasses.
   * @param tap The tap to skip from.
   */
  public abstract skip(tap: ReadableTapLike): Promise<void>;

  /**
   * Checks if a value is valid according to the schema. Must be implemented by subclasses.
   * @param value The value to check.
   * @param errorHook Optional error callback.
   * @param path Current path in the schema for error reporting.
   * @returns True if valid.
   */
  public abstract check(
    value: unknown,
    errorHook?: ErrorHook,
    path?: string[],
  ): boolean;

  /**
   * Compares two values. Must be implemented by subclasses.
   * @param val1 First value.
   * @param val2 Second value.
   * @returns -1 if val1 < val2, 0 if equal, 1 if val1 > val2.
   */
  public abstract compare(val1: T, val2: T): number;

  /**
   * Generates a random value. Must be implemented by subclasses.
   * @returns A random value of this type.
   */
  public abstract random(): T;

  /**
   * Returns the JSON schema representation.
   * @returns The JSON representation as JSONType.
   */
  public abstract toJSON(): JSONType;

  /**
   * Compares two encoded buffers. Must be implemented by subclasses.
   * @param tap1 The first tap.
   * @param tap2 The second tap.
   * @returns -1 if tap1 < tap2, 0 if equal, 1 if tap1 > tap2.
   */
  public abstract match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number>;

  /**
   * Serializes a value into an ArrayBuffer synchronously using the schema.
   * @param value The value to serialize.
   * @returns The serialized ArrayBuffer.
   */
  public toSyncBuffer(value: T): ArrayBuffer {
    if (this.validateWrites) {
      this.check(value, throwInvalidError, []);
    }
    const sizeHint = this.byteLength(value);
    if (sizeHint !== undefined) {
      const buffer = new ArrayBuffer(assertValidSize(sizeHint));
      const tap = new SyncWritableTap(buffer);
      this.writeSyncUnchecked(tap, value);
      return buffer;
    }
    const countingTap = new SyncCountingWritableTap();
    this.writeSyncUnchecked(countingTap, value);
    const buffer = new ArrayBuffer(countingTap.getPos());
    const tap = new SyncWritableTap(buffer);
    this.writeSyncUnchecked(tap, value);
    return buffer;
  }

  /**
   * Deserializes an ArrayBuffer into a value synchronously using the schema.
   * @param buffer The ArrayBuffer to deserialize.
   * @returns The deserialized value.
   */
  public abstract fromSyncBuffer(buffer: ArrayBuffer): T;

  /**
   * Writes a value to the sync tap. Must be implemented by subclasses.
   * @param tap The sync tap to write to.
   * @param value The value to write.
   */
  public writeSync(tap: SyncWritableTapLike, value: T): void {
    if (this.validateWrites) {
      this.check(value, throwInvalidError, []);
    }
    this.writeSyncUnchecked(tap, value);
  }

  /**
   * Writes a value to the sync tap without performing runtime validation.
   *
   * The default implementation delegates to {@link writeSync}.
   */
  public abstract writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: T,
  ): void;

  /**
   * Reads a value from the sync tap. Must be implemented by subclasses.
   * @param tap The sync tap to read from.
   * @returns The read value.
   */
  public abstract readSync(tap: SyncReadableTapLike): T;

  /**
   * Skips a value in the sync tap. Must be implemented by subclasses.
   * @param tap The sync tap to skip from.
   */
  public abstract skipSync(tap: SyncReadableTapLike): void;

  /**
   * Compares two encoded buffers synchronously. Must be implemented by subclasses.
   * @param tap1 The first sync tap.
   * @param tap2 The second sync tap.
   * @returns -1 if tap1 < tap2, 0 if equal, 1 if tap1 > tap2.
   */
  public abstract matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number;

  /**
   * Returns the encoded byte length for the value when it is cheap to compute.
   * Override in types with fixed or easily derived sizes to skip counting taps.
   */
  protected byteLength(_value: T): number | undefined {
    return undefined;
  }
}

function assertValidSize(size: number): number {
  if (!Number.isFinite(size) || !Number.isInteger(size) || size < 0) {
    throw new RangeError(`Invalid byte length: ${size}`);
  }
  return size;
}
