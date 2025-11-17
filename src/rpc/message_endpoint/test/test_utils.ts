import type { ProtocolLike } from "../../definitions/protocol_definitions.ts";
import { MessageEmitter } from "../emitter.ts";
import type { BinaryDuplexLike } from "../../protocol/transports/transport_helpers.ts";
import type {
  HandshakeRequestInit,
  HandshakeResponseInit,
  HandshakeResponseMessage,
} from "../../protocol/wire_format/handshake.ts";

export class TestEmitter extends MessageEmitter {
  constructor(protocol: ProtocolLike) {
    super(protocol, {});
  }

  send(): Promise<unknown> {
    return Promise.resolve("ok");
  }

  triggerError(err: unknown): void {
    this.dispatchError(err);
  }

  triggerHandshake(
    request: HandshakeRequestInit,
    response: HandshakeResponseInit,
  ): void {
    this.dispatchHandshake(request, response);
  }

  triggerFinalizeHandshake(
    request: HandshakeRequestInit,
    response: HandshakeResponseMessage,
  ): { retry: boolean; serverHash: Uint8Array } {
    return this.finalizeHandshake(request, response);
  }

  getServerHashKey(): string {
    return this.serverHashKey;
  }

  getServerHash(): Uint8Array {
    return this.serverHash;
  }
}

export class MockDuplex implements BinaryDuplexLike {
  readable: { read(): Promise<Uint8Array | null> };
  writable: WritableStream<Uint8Array>;

  sent: Uint8Array[] = [];
  private responseQueue: Uint8Array[] = [];
  throwOnRead = false;

  constructor() {
    this.readable = {
      read: () => {
        if (this.throwOnRead) {
          throw new Error("transport read failed");
        }
        return Promise.resolve(
          this.responseQueue.length > 0 ? this.responseQueue.shift()! : null,
        );
      },
    };
    this.writable = new WritableStream<Uint8Array>({
      write: (chunk) => {
        this.sent.push(chunk);
      },
    });
  }

  enqueue(chunk: Uint8Array): void {
    this.responseQueue.push(chunk);
  }

  setResponse(response: Uint8Array): void {
    this.enqueue(response);
  }

  closeReadable(): void {
    // For the destroy test, make read return null
    this.responseQueue.length = 0;
  }
}
