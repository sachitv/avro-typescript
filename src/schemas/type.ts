import type { ReadableTapLike, WritableTapLike } from "../serialization/tap.ts";
import type { Resolver } from "./resolver.ts";

type CloneOptions = Record<string, unknown>;
type ErrorHook = (
  path: string[],
  invalidValue: unknown,
  schemaType: Type,
) => void;
type IsValidOptions = { errorHook?: ErrorHook };

type InnerJSONType = (string | Record<string, unknown>)[];
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
  /**
   * Serializes a value into an ArrayBuffer using the schema.
   * @param value The value to serialize.
   * @returns The serialized ArrayBuffer.
   */
  public abstract toBuffer(value: T): Promise<ArrayBuffer>;

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
   * @param opts Optional cloning options.
   * @returns The cloned value.
   */
  public abstract clone(value: unknown, opts?: CloneOptions): T;

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
  public abstract write(tap: WritableTapLike, value: T): Promise<void>;

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
}
