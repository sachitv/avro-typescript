import { Tap } from "../serialization/tap.ts";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";
import { ErrorHook, throwInvalidError } from "./error.ts";

/**
 * Boolean type.
 */
export class BooleanType extends FixedSizeBaseType<boolean> {
  public check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    const isValid = typeof value === "boolean";
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  public read(tap: Tap): boolean {
    return tap.readBoolean();
  }

  public write(tap: Tap, value: boolean): void {
    if (typeof value !== "boolean") {
      throwInvalidError([], value, this);
    }
    tap.writeBoolean(value);
  }

  public override skip(tap: Tap): void {
    tap.skipBoolean();
  }

  public sizeBytes(): number {
    return 1; // 1 byte
  }

  public clone(value: boolean): boolean {
    this.check(value, throwInvalidError, []);
    return value;
  }

  public compare(val1: boolean, val2: boolean): number {
    return val1 === val2 ? 0 : val1 ? 1 : -1;
  }

  public random(): boolean {
    return Math.random() < 0.5;
  }

  public toJSON(): string {
    return "boolean";
  }
}
