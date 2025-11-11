import type { Decoder } from "./decoder.ts";

/**
 * Built-in null decoder (no compression).
 */
export class NullDecoder implements Decoder {
  decode(compressedData: Uint8Array): Promise<Uint8Array> {
    return Promise.resolve(compressedData);
  }
}
