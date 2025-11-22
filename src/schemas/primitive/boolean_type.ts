import type {
  ReadableTapLike,
  WritableTapLike,
} from "../../serialization/tap.ts";
import { FixedSizeBaseType } from "./fixed_size_base_type.ts";
import type { JSONType } from "../type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";

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

  public async read(tap: ReadableTapLike): Promise<boolean> {
    return await tap.readBoolean();
  }

  public async write(
    tap: WritableTapLike,
    value: boolean,
  ): Promise<void> {
    if (typeof value !== "boolean") {
      throwInvalidError([], value, this);
    }
    await tap.writeBoolean(value);
  }

  public override async skip(tap: ReadableTapLike): Promise<void> {
    await tap.skipBoolean();
  }

  public sizeBytes(): number {
    return 1; // 1 byte
  }

  public override clone(value: unknown): boolean {
    this.check(value, throwInvalidError, []);
    return value as boolean;
  }

  public compare(val1: boolean, val2: boolean): number {
    return val1 === val2 ? 0 : val1 ? 1 : -1;
  }

  public random(): boolean {
    return Math.random() < 0.5;
  }

  public override toJSON(): JSONType {
    return "boolean";
  }

  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    return await tap1.matchBoolean(tap2);
  }
}
