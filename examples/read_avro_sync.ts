// Example: synchronously read an Avro file using the SyncAvroReader and a Deno FsFile.
import { SyncAvroReader } from "../src/sync_avro_reader.ts";
import type {
  ISyncStreamReadableBuffer,
} from "../src/serialization/streams/sync_streams.ts";

/**
 * Minimal synchronous stream reader backed by a Deno.FsFile.
 * It implements the ISyncStreamReadableBuffer interface expected by SyncAvroReader.
 */
class FsSyncStreamReader implements ISyncStreamReadableBuffer {
  #file: Deno.FsFile;
  #chunkSize: number;
  #closed = false;

  constructor(path: string, chunkSize = 1024) {
    this.#file = Deno.openSync(path, { read: true });
    this.#chunkSize = chunkSize;
  }

  readNext(): Uint8Array | undefined {
    if (this.#closed) {
      return undefined;
    }
    const buffer = new Uint8Array(this.#chunkSize);
    const read = this.#file.readSync(buffer);
    if (read === null || read === 0) {
      this.close();
      return undefined;
    }
    return buffer.slice(0, read);
  }

  close(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.#file.close();
    }
  }
}

/**
 * Synchronously reads all Avro records from the provided file path.
 */
export function readAvroRecordsSync(
  filePath: string =
    new URL("../test-data/weather.avro", import.meta.url).pathname,
): unknown[] {
  const stream = new FsSyncStreamReader(filePath, 2048);
  const reader = SyncAvroReader.fromStream(stream);

  const header = reader.getHeader();
  const decoder = new TextDecoder();
  for (const [key, value] of header.meta.entries()) {
    console.log(`Meta [${key}]:`, decoder.decode(value));
  }
  console.log("Sync Marker:", header.sync);

  const records = Array.from(reader.iterRecords());
  reader.close();
  // Demonstrate that reads are no-ops after closure to mirror the interface contract.
  stream.readNext();
  return records;
}

const defaultPath = new URL("../test-data/weather.avro", import.meta.url)
  .pathname;
const records = readAvroRecordsSync(defaultPath);
for (const record of records) {
  console.log(record);
}
