import type { IWritableBuffer } from "./internal/serialization/buffers/buffer.ts";
import {
  AvroFileWriter,
  type AvroWriterOptions,
} from "./internal/serialization/avro_file_writer.ts";
import { StreamWritableBuffer } from "./internal/serialization/streams/stream_writable_buffer.ts";
import { StreamWritableBufferAdapter } from "./internal/serialization/streams/stream_writable_buffer_adapter.ts";

export type { AvroWriterOptions } from "./internal/serialization/avro_file_writer.ts";
export type { EncoderRegistry } from "./internal/serialization/encoders/mod.ts";

/**
 * Interface exposed by Avro writer instances.
 */
export interface AvroWriterInstance {
  /**
   * Appends a single record to the output.
   */
  append(record: unknown): Promise<void>;

  /**
   * Manually flush pending records to a block.
   * This can be called to force writing of accumulated records before the block size threshold is reached.
   */
  flushBlock(): Promise<void>;

  /**
   * Flushes pending data and closes the writer.
   */
  close(): Promise<void>;
}

class AvroWriterInstanceImpl implements AvroWriterInstance {
  #writer: AvroFileWriter;
  #closeHook?: () => Promise<void> | void;
  #closed = false;

  constructor(
    writer: AvroFileWriter,
    closeHook?: () => Promise<void> | void,
  ) {
    this.#writer = writer;
    this.#closeHook = closeHook;
  }

  async append(record: unknown): Promise<void> {
    await this.#writer.append(record);
  }

  async flushBlock(): Promise<void> {
    await this.#writer.flushBlock();
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    await this.#writer.close();
    if (this.#closeHook) {
      await this.#closeHook();
      this.#closeHook = undefined;
    }
    this.#closed = true;
  }
}

/**
 * Public API for writing Avro object container files to various destinations.
 *
 * The writer mirrors the structure of {@link AvroReader}, providing factory
 * helpers that adapt different buffer and stream implementations to the
 * internal {@link AvroFileWriter}.
 *
 * @example
 * ```typescript
 * const schema = {
 *   type: "record",
 *   name: "example.Event",
 *   fields: [
 *     { name: "id", type: "int" },
 *     { name: "message", type: "string" },
 *   ],
 * };
 *
 * // Write to an in-memory buffer
 * const memoryBuffer = new InMemoryWritableBuffer(new ArrayBuffer(1024));
 * const bufferWriter = AvroWriter.toBuffer(memoryBuffer, { schema });
 * await bufferWriter.append({ id: 1, message: "hello" });
 * await bufferWriter.close();
 *
 * // Write to a WritableStream (e.g., network, file)
 * const stream = new WritableStream<Uint8Array>({
 *   async write(chunk) {
 *     await someSink.write(chunk);
 *   },
 * });
 * const streamWriter = AvroWriter.toStream(stream, { schema, codec: "deflate" });
 * for (const record of records) {
 *   await streamWriter.append(record);
 * }
 * await streamWriter.close();
 * ```
 */
export class AvroWriter {
  /**
   * Creates an Avro writer from any {@link IWritableBuffer}.
   *
   * @param buffer Writable buffer that will receive the Avro container output.
   * @param options Writer configuration including schema, codec, metadata, etc.
   */
  public static toBuffer(
    buffer: IWritableBuffer,
    options: AvroWriterOptions,
  ): AvroWriterInstance {
    const writer = new AvroFileWriter(buffer, options);
    return new AvroWriterInstanceImpl(writer);
  }

  /**
   * Creates an Avro writer that emits data to a {@link WritableStream}.
   *
   * @param stream Writable stream that will receive chunked Avro data.
   * @param options Writer configuration including schema, codec, metadata, etc.
   */
  public static toStream(
    stream: WritableStream<Uint8Array>,
    options: AvroWriterOptions,
  ): AvroWriterInstance {
    const streamBuffer = new StreamWritableBuffer(stream);
    const adapter = new StreamWritableBufferAdapter(streamBuffer);
    const writer = new AvroFileWriter(adapter, options);
    return new AvroWriterInstanceImpl(writer, async () => {
      await adapter.close();
    });
  }
}
