// Example: adapt a decrypted stream into Avro's tap API using stream adapters.
import { ReadableTap } from "../src/internal/serialization/tap.ts";
import { StreamReadableBuffer } from "../src/internal/serialization/streams/stream_readable_buffer.ts";
import { StreamReadableBufferAdapter } from "../src/internal/serialization/streams/stream_readable_buffer_adapter.ts";

/**
 * This demo shows how to take an encrypted ReadableStream, decrypt it chunk by
 * chunk, and then feed it into Avro's ReadableTap via the stream buffer adapter.
 */
const XOR_KEY = 0x5a;

// Pretend this is Avro binary data that was produced elsewhere.
const plaintext = new Uint8Array([0x06, 0x66, 0x6f, 0x6f]);

function xor(data: Uint8Array): Uint8Array {
  return data.map((byte) => byte ^ XOR_KEY);
}

const encryptedChunks = [xor(plaintext.slice(0, 2)), xor(plaintext.slice(2))];

const encryptedStream = new ReadableStream<Uint8Array>({
  start(controller) {
    for (const chunk of encryptedChunks) {
      controller.enqueue(chunk);
    }
    controller.close();
  },
});

const decryptor = new TransformStream<Uint8Array, Uint8Array>({
  transform(chunk, controller) {
    controller.enqueue(xor(chunk));
  },
});

const decryptedStream = encryptedStream.pipeThrough(decryptor);

const streamBuffer = new StreamReadableBuffer(decryptedStream);
const adapter = new StreamReadableBufferAdapter(streamBuffer);
const tap = new ReadableTap(adapter);

const decodedString = await tap.readString();
console.log(`Decoded string: ${decodedString}`);

await streamBuffer.close();
