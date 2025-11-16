import { ReadableTap, type ReadableTapLike } from "../serialization/tap.ts";
import { Type } from "./type.ts";
import { Resolver } from "./resolver.ts";
import { safeStringify } from "./json.ts";

type ErrorHook = (
  path: string[],
  invalidValue: unknown,
  schemaType: Type,
) => void;
type IsValidOptions = { errorHook?: ErrorHook };

/**
 * Base implementation of Type<T> providing common serialization operations.
 * Subclasses must implement the remaining abstract methods.
 */
export abstract class BaseType<T = unknown> extends Type<T> {
  /**
   * Deserializes an ArrayBuffer into a value using the schema.
   * @param buffer The ArrayBuffer to deserialize.
   * @returns The deserialized value.
   */
  public async fromBuffer(buffer: ArrayBuffer): Promise<T> {
    const tap = new ReadableTap(buffer);
    const value = await this.read(tap);
    if (!await tap.isValid() || tap.getPos() !== buffer.byteLength) {
      throw new Error("Insufficient data for type");
    }
    return value;
  }

  /**
   * Validates if a value conforms to the schema.
   * @param value The value to validate.
   * @param opts Optional validation options.
   * @returns True if valid, false otherwise.
   */
  public isValid(value: unknown, opts?: IsValidOptions): boolean {
    return this.check(value, opts?.errorHook, []);
  }

  /**
   * Creates a resolver for schema evolution from a writer type to this reader type.
   * @param writerType The writer schema type.
   * @returns A resolver for reading the writer type as this type.
   */
  public createResolver(writerType: Type): Resolver {
    if (this.constructor === writerType.constructor) {
      return new class extends Resolver {
        async read(tap: ReadableTapLike) {
          return await this.readerType.read(tap);
        }
      }(this);
    } else {
      throw new Error(
        `Schema evolution not supported from writer type: ${
          safeStringify(writerType.toJSON())
        } to reader type: ${safeStringify(this.toJSON())}`,
      );
    }
  }
}
