import type { ReadableTapLike } from "../serialization/tap.ts";
import type { SyncReadableTapLike } from "../serialization/tap_sync.ts";
import type { Type } from "./type.ts";

/**
 * Base resolver for schema evolution, allowing reading data written with one schema
 * using a different but compatible reader schema.
 */
/**
 * Abstract base class for schema resolvers.
 * Resolvers handle reading data written by a writer schema into a reader schema,
 * potentially performing conversions or handling evolution.
 */
export abstract class Resolver<T = unknown> {
  /** The type used for reading. */
  protected readerType: Type<T>;

  /**
   * Creates a new Resolver.
   * @param readerType The reader schema type.
   */
  constructor(readerType: Type<T>) {
    this.readerType = readerType;
  }

  /**
   * Reads a value from the tap using the resolution logic.
   * @param tap The tap to read from.
   */
  public abstract read(tap: ReadableTapLike): Promise<T>;

  /**
   * Reads a value from the sync tap using the resolution logic.
   * @param tap The sync tap to read from.
   */
  public abstract readSync(tap: SyncReadableTapLike): T;
}
