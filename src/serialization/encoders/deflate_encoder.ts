import type { Encoder } from "./encoder.ts";

/**
 * Built-in deflate encoder using modern web standards.
 *
 * Uses the 'deflate-raw' format which corresponds to RFC 1951 as specified
 * in the Avro specification (no zlib header or checksum).
 */
export class DeflateEncoder implements Encoder {
  /**
   * Encodes data using the deflate algorithm.
   * @param uncompressedData The data to compress.
   * @returns The compressed data.
   */
  async encode(uncompressedData: Uint8Array): Promise<Uint8Array> {
    if (typeof CompressionStream === "undefined") {
      throw new Error(
        "Deflate codec not supported in this environment. CompressionStream API required.",
      );
    }

    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(uncompressedData);
        controller.close();
      },
    });

    const compressed = await new Response(
      stream.pipeThrough(new CompressionStream("deflate-raw")),
    ).arrayBuffer();

    return new Uint8Array(compressed);
  }
}
