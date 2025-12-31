import { BaseType } from "../base_type.ts";
import { throwInvalidError } from "../error.ts";

/**
 * Abstract base class for primitive Avro types.
 */
export abstract class PrimitiveType<T = unknown> extends BaseType<T> {
  /** Creates a new primitive type. */
  constructor(validate = true) {
    super(validate);
  }

  /**
   * Clones a primitive value (primitives are immutable).
   */
  /**
   * Clones a primitive value.
   */
  public override cloneFromValue(value: unknown): T {
    this.check(value, throwInvalidError, []);
    return value as T;
  }

  /**
   * Compares two primitive values.
   */
  public override compare(val1: T, val2: T): number {
    if (val1 < val2) return -1;
    if (val1 > val2) return 1;
    return 0;
  }
}
