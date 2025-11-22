import { concatUint8Arrays } from "../../internal/collections/array_utils.ts";

/**
 * Incrementally collects framed Avro messages from arbitrary binary chunks.
 * Counterpart to the `frameMessage` function in `framing.ts`, which frames messages for sending.
 */
export class FrameAssembler {
  #buffer = new Uint8Array(0);
  #frames: Uint8Array[] = [];

  /** Pushes a chunk and returns complete frames */
  push(chunk: Uint8Array): Uint8Array[] {
    if (!chunk.length) {
      return [];
    }

    if (!this.#buffer.length) {
      this.#buffer = chunk.slice();
    } else {
      const combined = new Uint8Array(this.#buffer.length + chunk.length);
      combined.set(this.#buffer, 0);
      combined.set(chunk, this.#buffer.length);
      this.#buffer = combined;
    }

    const output: Uint8Array[] = [];
    let offset = 0;

    while (this.#buffer.length - offset >= 4) {
      const frameLength = this.#readUint32BE(offset);
      offset += 4;

      if (frameLength === 0) {
        output.push(this.#flushFrames());
        continue;
      }

      if (offset + frameLength > this.#buffer.length) {
        offset -= 4; // rewind header, wait for more data
        break;
      }

      this.#frames.push(this.#buffer.subarray(offset, offset + frameLength));
      offset += frameLength;
    }

    if (offset > 0) {
      this.#buffer = this.#buffer.subarray(offset);
    }

    return output;
  }

  /** Resets the assembler */
  reset(): void {
    this.#buffer = new Uint8Array(0);
    this.#frames = [];
  }

  #flushFrames(): Uint8Array {
    if (!this.#frames.length) {
      return new Uint8Array(0);
    }
    const payload = this.#frames.length === 1
      ? this.#frames[0]
      : concatUint8Arrays(this.#frames);
    this.#frames = [];
    return payload;
  }

  #readUint32BE(offset: number): number {
    const view = new DataView(
      this.#buffer.buffer,
      this.#buffer.byteOffset + offset,
      4,
    );
    return view.getUint32(0, false); // big-endian
  }
}
