/**
 * Interface for readable and writable buffers with random access.
 * All operations are asynchronous to support various backing stores.
 */
export abstract class IBuffer {
  public abstract length(): Promise<number>;
  public abstract read(
    offset: number,
    size: number,
  ): Promise<Uint8Array | undefined>;
  public abstract write(offset: number, data: Uint8Array): Promise<void>;
}
