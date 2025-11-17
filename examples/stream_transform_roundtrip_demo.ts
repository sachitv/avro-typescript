// Example: decode an encrypted stream and re-encrypt via writable tap roundtrip.
import { ReadableTap, WritableTap } from "../src/serialization/tap.ts";
import { StreamReadableBuffer } from "../src/serialization/streams/stream_readable_buffer.ts";
import { StreamReadableBufferAdapter } from "../src/serialization/streams/stream_readable_buffer_adapter.ts";
import { StreamWritableBuffer } from "../src/serialization/streams/stream_writable_buffer.ts";
import { StreamWritableBufferAdapter } from "../src/serialization/streams/stream_writable_buffer_adapter.ts";

const XOR_KEY = 0x5a;

function xorBytes(data: Uint8Array): Uint8Array {
  return data.map((byte) => byte ^ XOR_KEY);
}

const plaintext = new Uint8Array([0x06, 0x66, 0x6f, 0x6f]);

const encryptedInputChunks = [
  xorBytes(plaintext.slice(0, 2)),
  xorBytes(plaintext.slice(2)),
];

const encryptedInputStream = new ReadableStream<Uint8Array>({
  start(controller) {
    for (const chunk of encryptedInputChunks) {
      controller.enqueue(chunk);
    }
    controller.close();
  },
});

const decryptTransform = new TransformStream<Uint8Array, Uint8Array>({
  transform(chunk, controller) {
    controller.enqueue(xorBytes(chunk));
  },
});

const decryptedStream = encryptedInputStream.pipeThrough(decryptTransform);
const streamReadableBuffer = new StreamReadableBuffer(decryptedStream);
const readableAdapter = new StreamReadableBufferAdapter(streamReadableBuffer);
const readerTap = new ReadableTap(readableAdapter);
const decodedString = await readerTap.readString();
console.log(`Decoded from encrypted input: ${decodedString}`);

const encryptTransform = new TransformStream<Uint8Array, Uint8Array>({
  transform(chunk, controller) {
    controller.enqueue(xorBytes(chunk));
  },
});

const capturedCiphertext: Uint8Array[] = [];
const ciphertextSink = new WritableStream<Uint8Array>({
  write(chunk) {
    capturedCiphertext.push(chunk.slice());
  },
});
const piping = encryptTransform.readable.pipeTo(ciphertextSink);

const streamWritableBuffer = new StreamWritableBuffer(
  encryptTransform.writable,
);
const writableAdapter = new StreamWritableBufferAdapter(streamWritableBuffer);
const writerTap = new WritableTap(writableAdapter);

if (decodedString !== undefined) {
  await writerTap.writeString(decodedString);
}

await streamWritableBuffer.close();
await piping;
console.log("Encrypted output chunks:", capturedCiphertext);
