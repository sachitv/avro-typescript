import type { Decoder } from "./decoder.ts";

/**
 * Built-in deflate decoder using modern web standards.
 *
 * Uses the 'deflate-raw' format which corresponds to RFC 1951 as specified
 * in the Avro specification (no zlib header or checksum).
 */
export class DeflateDecoder implements Decoder {
  async decode(compressedData: Uint8Array): Promise<Uint8Array> {
    if (typeof DecompressionStream === "undefined") {
      throw new Error(
        "Deflate codec not supported in this environment. DecompressionStream API required.",
      );
    }

    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(compressedData);
        controller.close();
      },
    });

    const decompressed = await new Response(
      stream.pipeThrough(new DecompressionStream("deflate-raw")),
    ).arrayBuffer();

    return new Uint8Array(decompressed);
  }
}
