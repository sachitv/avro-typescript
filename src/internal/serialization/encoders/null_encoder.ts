import type { Encoder } from "./encoder.ts";

/**
 * Built-in null encoder (no compression).
 */
export class NullEncoder implements Encoder {
  encode(uncompressedData: Uint8Array): Promise<Uint8Array> {
    return Promise.resolve(uncompressedData);
  }
}
