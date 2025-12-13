import type { ISyncReadable } from "./serialization/buffers/sync_buffer.ts";
import {
  SyncAvroFileParser,
  type SyncAvroFileParserOptions,
} from "./serialization/sync_avro_file_parser.ts";
import type { ParsedAvroHeader } from "./serialization/avro_file_parser.ts";
import type {
  SyncDecoderRegistry,
} from "./serialization/decoders/sync_decoder.ts";
import {
  SyncStreamReadableBufferAdapter,
} from "./serialization/streams/sync_stream_readable_buffer_adapter.ts";
import type {
  ISyncStreamReadableBuffer,
} from "./serialization/streams/sync_streams.ts";

/**
 * Options shared by all SyncAvroReader factories.
 */
export interface SyncReaderSchemaOptions {
  /** Optional reader schema used to resolve records written with a different schema. */
  readerSchema?: SyncAvroFileParserOptions["readerSchema"];
  /** Custom codec decoders. Cannot include "null" as it is built-in. */
  decoders?: SyncDecoderRegistry;
  /** Optional hook to call when closing the reader. */
  closeHook?: () => void;
}

/**
 * Options accepted by {@link SyncAvroReader.fromBuffer}.
 */
export type SyncFromBufferOptions = SyncReaderSchemaOptions;

/**
 * Options accepted by {@link SyncAvroReader.fromStream}.
 */
export type SyncFromStreamOptions = SyncReaderSchemaOptions;

/**
 * Interface exposed by SyncAvroReader instances.
 */
export interface SyncAvroReaderInstance {
  /** Returns the parsed Avro file header. */
  getHeader(): ParsedAvroHeader;
  /** Closes the reader and releases any resources. */
  close(): void;
  /** Returns an iterator over the Avro records. */
  iterRecords(): IterableIterator<unknown>;
}

class SyncAvroReaderInstanceImpl implements SyncAvroReaderInstance {
  #parser: SyncAvroFileParser;
  #records: IterableIterator<unknown>;
  #closeHook?: () => void;
  #closed = false;

  constructor(parser: SyncAvroFileParser, closeHook?: () => void) {
    this.#parser = parser;
    this.#records = parser.iterRecords();
    this.#closeHook = closeHook;
  }

  public getHeader(): ParsedAvroHeader {
    return this.#parser.getHeader();
  }

  public iterRecords(): IterableIterator<unknown> {
    return this.#records;
  }

  public close(): void {
    if (this.#closed) {
      return;
    }

    if (this.#closeHook) {
      try {
        this.#closeHook();
      } catch {
        // Ignore close hook errors to keep shutdown best-effort.
      }
      this.#closeHook = undefined;
    }
    this.#closed = true;
  }
}

/**
 * Public API for reading Avro object container files synchronously.
 */
export class SyncAvroReader {
  /**
   * Creates a reader from any synchronous readable buffer.
   */
  public static fromBuffer(
    buffer: ISyncReadable,
    options?: SyncFromBufferOptions,
  ): SyncAvroReaderInstance {
    const parser = new SyncAvroFileParser(buffer, {
      readerSchema: options?.readerSchema,
      decoders: options?.decoders,
    });
    return new SyncAvroReaderInstanceImpl(parser, options?.closeHook);
  }

  /**
   * Creates a reader from a synchronous stream buffer by adapting it to ISyncReadable.
   */
  public static fromStream(
    stream: ISyncStreamReadableBuffer,
    options?: SyncFromStreamOptions,
  ): SyncAvroReaderInstance {
    const adapter = new SyncStreamReadableBufferAdapter(stream);
    const combinedCloseHook = options?.closeHook
      ? () => {
        adapter.close();
        options.closeHook?.();
      }
      : () => adapter.close();

    return SyncAvroReader.fromBuffer(adapter, {
      readerSchema: options?.readerSchema,
      decoders: options?.decoders,
      closeHook: combinedCloseHook,
    });
  }
}
