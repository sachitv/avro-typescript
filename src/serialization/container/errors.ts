/**
 * The errors raised when container bytes are not a valid Avro object container
 * file.
 *
 * They live in one module, rather than beside the functions that throw them,
 * because they are a single hierarchy shared by every reader built on the
 * container core: the parsers here, the existing async and sync file parsers,
 * and the random-access and streaming readers. Callers catch
 * {@link AvroContainerError} to handle any malformed file, or a subclass to
 * react to one kind, without knowing which function or reader found it.
 *
 * Which function throws which error:
 *
 * - {@link InvalidMagicError}: `parseHeader`, when the file does not start
 *   with `Obj\x01`.
 * - {@link InvalidHeaderError}: `parseHeader`, for malformed metadata, a
 *   missing or unparsable schema, or a header over `maxHeaderBytes`.
 * - {@link InvalidBlockError}: `parseBlockHead`, for a negative, unsafe, or
 *   overlong record count or byte length.
 * - {@link SyncMismatchError}: readers, when the marker after a block's data
 *   is not the header's sync marker (checked with `matchesSync`).
 * - {@link UnsupportedCodecError}: `selectDecoder`, when no decoder is
 *   registered for the file's codec.
 *
 * Where the file parsers already threw an error for a case (bad magic, missing
 * schema, sync mismatch, unsupported codec), the message is unchanged, so code
 * that matches on message text keeps working when the parsers move onto the
 * core. Cases the parsers did not check before get new messages, and schema
 * JSON that fails to parse is now an {@link InvalidHeaderError} (with the
 * `SyntaxError` as its `cause`) instead of the bare `SyntaxError`. Errors that only a reader can detect, such as a file that
 * ends partway through a block, belong here too as they are added.
 *
 * Every class sets `name` to its own class name, so logs and stack traces
 * show the specific error rather than `Error`.
 *
 * @module
 */

/**
 * Base class for errors raised while parsing an Avro object container file.
 */
export class AvroContainerError extends Error {
  /**
   * Creates a container error whose `name` is the concrete subclass name.
   * @param message Describes what is wrong with the container bytes.
   * @param options Standard error options, such as a `cause`.
   */
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** The file does not start with the Avro magic bytes `Obj\x01`. */
export class InvalidMagicError extends AvroContainerError {
  /** Creates an error for a file whose first four bytes are not `Obj\x01`. */
  constructor() {
    super("Invalid AVRO file: incorrect magic bytes");
  }
}

/** The file header is malformed or lacks required metadata. */
export class InvalidHeaderError extends AvroContainerError {}

/** A block's count or byte-length prefix is malformed. */
export class InvalidBlockError extends AvroContainerError {
  /** Byte offset at which the invalid block starts. */
  readonly blockOffset: number;

  /**
   * Creates an error for the block starting at `blockOffset`.
   * @param blockOffset Byte offset at which the block starts.
   * @param reason Describes what is wrong with the block prefix.
   */
  constructor(blockOffset: number, reason: string) {
    super(`Invalid AVRO file: block at offset ${blockOffset}: ${reason}`);
    this.blockOffset = blockOffset;
  }
}

/** A block's trailing sync marker differs from the header's sync marker. */
export class SyncMismatchError extends AvroContainerError {
  /** Byte offset at which the mismatched block starts. */
  readonly blockOffset: number;

  /**
   * Creates an error for the block starting at `blockOffset`.
   * @param blockOffset Byte offset at which the block starts.
   */
  constructor(blockOffset: number) {
    super(
      `Invalid AVRO file: sync marker mismatch for block at offset ${blockOffset}`,
    );
    this.blockOffset = blockOffset;
  }
}

/** The file's codec has no registered decoder. */
export class UnsupportedCodecError extends AvroContainerError {
  /** The codec name read from the file's `avro.codec` metadata. */
  readonly codec: string;

  /**
   * Creates an error for a codec without a decoder.
   * @param codec The codec name read from the file metadata.
   */
  constructor(codec: string) {
    super(`Unsupported codec: ${codec}. Provide a custom decoder.`);
    this.codec = codec;
  }
}
