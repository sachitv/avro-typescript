import type {
  HandshakeRequestInit,
  HandshakeRequestMessage,
  HandshakeResponseInit,
  HandshakeResponseMessage,
} from "../protocol/wire_format/handshake.ts";
import type {
  MessageTransportOptions,
  ProtocolLike,
} from "../definitions/protocol_definitions.ts";

/**
 * Abstract base class for message endpoints in Avro RPC.
 * Extends EventTarget to dispatch events like errors, handshakes, and end-of-transmission.
 */
export abstract class MessageEndpoint extends EventTarget {
  /**
   * The Avro protocol definition that this endpoint adheres to.
   */
  readonly protocol: ProtocolLike;
  /**
   * The size of the internal buffer used for reading and writing messages.
   */
  protected readonly bufferSize: number;
  /**
   * The maximum size of individual frames in the message transport protocol.
   */
  protected readonly frameSize: number;

  /**
   * Initializes the endpoint with protocol and transport options.
   * @param protocol The Avro protocol definition.
   * @param opts Transport configuration options.
   */
  protected constructor(protocol: ProtocolLike, opts: MessageTransportOptions) {
    super();
    this.protocol = protocol;
    this.bufferSize = opts.bufferSize ?? 2048;
    this.frameSize = opts.frameSize ?? 2048;
  }

  /**
   * Dispatches an error event with the given error.
   * Converts non-Error values to Error instances and uses ErrorEvent if available.
   * @param err The error to dispatch.
   */
  protected dispatchError(err: unknown): void {
    let error: Error;
    if (err instanceof Error) {
      error = err;
    } else {
      error = new Error(String(err));
    }
    if (typeof ErrorEvent === "function") {
      this.dispatchEvent(new ErrorEvent("error", { error }));
    } else {
      this.dispatchEvent(new CustomEvent("error", { detail: error }));
    }
  }

  /**
   * Dispatches a handshake event with request and response details.
   * @param request The handshake request data.
   * @param response The handshake response data.
   */
  protected dispatchHandshake(
    request: HandshakeRequestInit | HandshakeRequestMessage,
    response: HandshakeResponseInit | HandshakeResponseMessage,
  ): void {
    this.dispatchEvent(
      new CustomEvent("handshake", {
        detail: { request, response },
      }),
    );
  }

  /**
   * Dispatches an end-of-transmission event with the number of pending operations.
   * @param pending The number of pending operations.
   */
  protected dispatchEot(pending: number): void {
    this.dispatchEvent(new CustomEvent("eot", { detail: { pending } }));
  }
}
