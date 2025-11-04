import { Tap } from "../serialization/tap.ts";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";
import { type JSONType } from "./type.ts";
import { ErrorHook, throwInvalidError } from "./error.ts";

/**
 * Null type.
 */
export class NullType extends FixedSizeBaseType<null> {
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

  public read(_tap: Tap): null {
    return null;
  }

  public write(_tap: Tap, value: null): void {
    if (value !== null) {
      throwInvalidError([], value, this);
    }
  }

  public override skip(_tap: Tap): void {
    // Null takes no space
  }

  public sizeBytes(): number {
    return 0; // Null takes no space
  }

  public clone(value: null): null {
    this.check(value, throwInvalidError, []);
    return value;
  }

  public compare(_val1: null, _val2: null): number {
    return 0;
  }

  public random(): null {
    return null;
  }

  public override toJSON(): JSONType {
    return "null";
  }
}
