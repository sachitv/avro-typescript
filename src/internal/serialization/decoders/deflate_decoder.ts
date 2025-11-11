import { type Decoder } from "./decoder.ts";

/**
 * Built-in deflate decoder using modern web standards.
 *
 * Uses the 'deflate-raw' format which corresponds to RFC 1951 as specified
 * in the Avro specification (no zlib header or checksum).
 */
export class DeflateDecoder implements Decoder {
  async decode(compressedData: Uint8Array): Promise<Uint8Array> {
    if (typeof CompressionStream === "undefined") {
      throw new Error(
        "Deflate codec not supported in this environment. CompressionStream API required.",
      );
    }

    try {
      const decompressionStream = new CompressionStream("deflate-raw");
      const writer = decompressionStream.writable.getWriter();
      const reader = decompressionStream.readable.getReader();

      // Write compressed data to the stream
      const arrayBuffer = new ArrayBuffer(compressedData.length);
      new Uint8Array(arrayBuffer).set(compressedData);
      writer.write(arrayBuffer);
      writer.close();

      // Read decompressed data
      const chunks: Uint8Array[] = [];
      let done = false;

      while (!done) {
        const { value, done: readerDone } = await reader.read();
        done = readerDone;
        if (value) {
          chunks.push(value);
        }
      }

      // Combine all chunks into a single Uint8Array
      const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
      const result = new Uint8Array(totalLength);
      let offset = 0;

      for (const chunk of chunks) {
        result.set(chunk, offset);
        offset += chunk.length;
      }

      return result;
    } catch (error) {
      throw new Error(
        `Failed to decompress deflate data: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }
}
