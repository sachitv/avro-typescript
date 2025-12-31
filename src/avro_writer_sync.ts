import type { ISyncWritable } from "./serialization/buffers/buffer_sync.ts";
import {
  SyncAvroFileWriter,
  type SyncAvroWriterOptions,
} from "./serialization/avro_file_writer_sync.ts";
import {
  SyncStreamWritableBufferAdapter,
} from "./serialization/streams/stream_writable_buffer_adapter_sync.ts";
import type {
  ISyncStreamWritableBuffer,
} from "./serialization/streams/streams_sync.ts";

export type { SyncAvroWriterOptions } from "./serialization/avro_file_writer_sync.ts";

/**
 * Interface exposed by synchronous Avro writer instances.
 */
export interface SyncAvroWriterInstance {
  /** Appends a record to the Avro container. */
  append(record: unknown): void;
  /** Forces the current batch of pending records to flush into a block. */
  flushBlock(): void;
  /** Flushes pending data and closes the writer. */
  close(): void;
}

class SyncAvroWriterInstanceImpl implements SyncAvroWriterInstance {
  #writer: SyncAvroFileWriter;
  #closeHook?: () => void;
  #closed = false;

  constructor(writer: SyncAvroFileWriter, closeHook?: () => void) {
    this.#writer = writer;
    this.#closeHook = closeHook;
  }

  public append(record: unknown): void {
    this.#writer.append(record);
  }

  public flushBlock(): void {
    this.#writer.flushBlock();
  }

  public close(): void {
    if (this.#closed) {
      return;
    }

    this.#writer.close();
    if (this.#closeHook) {
      this.#closeHook();
      this.#closeHook = undefined;
    }
    this.#closed = true;
  }
}

/**
 * Public API for writing Avro object container files synchronously.
 */
export class SyncAvroWriter {
  /**
   * Creates a writer targeting any synchronous writable buffer.
   */
  public static toBuffer(
    buffer: ISyncWritable,
    options: SyncAvroWriterOptions,
  ): SyncAvroWriterInstance {
    const writer = new SyncAvroFileWriter(buffer, options);
    return new SyncAvroWriterInstanceImpl(writer);
  }

  /**
   * Creates a writer that pushes bytes to a synchronous stream sink.
   */
  public static toStream(
    stream: ISyncStreamWritableBuffer,
    options: SyncAvroWriterOptions,
  ): SyncAvroWriterInstance {
    const adapter = new SyncStreamWritableBufferAdapter(stream);
    const writer = new SyncAvroFileWriter(adapter, options);
    return new SyncAvroWriterInstanceImpl(writer, () => {
      adapter.close();
    });
  }
}
