import { Tap } from "../serialization/tap.ts";
import { Type } from "./type.ts";

/**
 * Base resolver for schema evolution, allowing reading data written with one schema
 * using a different but compatible reader schema.
 */
export abstract class Resolver<T = unknown> {
  protected readerType: Type<T>;

  constructor(readerType: Type<T>) {
    this.readerType = readerType;
  }

  public abstract read(tap: Tap): T;
}
