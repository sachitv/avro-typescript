import {
  type ReadableTapLike,
  type WritableTapLike,
} from "../serialization/tap.ts";
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

  // deno-lint-ignore require-await
  public override async read(_tap: ReadableTapLike): Promise<null> {
    return null;
  }

  // deno-lint-ignore require-await
  public override async write(
    _tap: WritableTapLike,
    value: null,
  ): Promise<void> {
    if (value !== null) {
      throwInvalidError([], value, this);
    }
  }

  public override async skip(_tap: ReadableTapLike): Promise<void> {
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

  // deno-lint-ignore require-await
  public override async match(
    _tap1: ReadableTapLike,
    _tap2: ReadableTapLike,
  ): Promise<number> {
    return 0;
  }
}
