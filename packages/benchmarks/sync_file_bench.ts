import {
  SyncAvroReader,
  SyncAvroWriter,
  type ISyncStreamReadableBuffer,
  type ISyncStreamWritableBuffer,
} from "../../src/mod.ts";

const BENCH_SCHEMA = {
  type: "record",
  name: "BenchRecord",
  fields: [
    { name: "id", type: "long" },
    { name: "label", type: "string" },
    { name: "value", type: "double" },
    { name: "active", type: "boolean" },
  ],
} as const;

function generateRecords(count: number) {
  return Array.from({ length: count }, (_, index) => ({
    id: BigInt(index),
    label: `record-${index}`,
    value: index * Math.PI,
    active: index % 2 === 0,
  }));
}

const RECORD_COUNTS = [1000, 10000, 100000];
const SAMPLE_FILES = new Map<number, string>();

class FsSyncStreamReader implements ISyncStreamReadableBuffer {
  #file: Deno.FsFile;
  #chunkSize: number;
  #closed = false;

  constructor(path: string, chunkSize = 4096) {
    this.#file = Deno.openSync(path, { read: true });
    this.#chunkSize = chunkSize;
  }

  readNext(): Uint8Array | undefined {
    if (this.#closed) {
      return undefined;
    }
    const buffer = new Uint8Array(this.#chunkSize);
    const bytesRead = this.#file.readSync(buffer);
    if (bytesRead === null || bytesRead === 0) {
      this.close();
      return undefined;
    }
    return buffer.slice(0, bytesRead);
  }

  close(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.#file.close();
    }
  }
}

class FsSyncStreamWriter implements ISyncStreamWritableBuffer {
  #file: Deno.FsFile;
  #closed = false;

  constructor(path: string) {
    this.#file = Deno.openSync(path, {
      write: true,
      create: true,
      truncate: true,
    });
  }

  writeBytes(data: Uint8Array): void {
    if (this.#closed || data.length === 0) {
      return;
    }
    this.#file.writeSync(data);
  }

  close(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.#file.close();
    }
  }
}

function writeRecordsToFile(path: string, records: any[]): void {
  const writer = SyncAvroWriter.toStream(new FsSyncStreamWriter(path), {
    schema: BENCH_SCHEMA,
  });
  for (const record of records) {
    writer.append(record);
  }
  writer.close();
}

function readRecordsFromFile(path: string): number {
  const reader = SyncAvroReader.fromStream(new FsSyncStreamReader(path));
  let count = 0;
  for (const _record of reader.iterRecords()) {
    count++;
  }
  reader.close();
  return count;
}

const TEMP_DIR = Deno.makeTempDirSync({ prefix: "sync-avro-bench-" });
for (const count of RECORD_COUNTS) {
  const records = generateRecords(count);
  const filePath = Deno.makeTempFileSync({
    dir: TEMP_DIR,
    prefix: `sync-avro-reader-${count}-`,
  });
  writeRecordsToFile(filePath, records);
  SAMPLE_FILES.set(count, filePath);
}

globalThis.addEventListener("unload", () => {
  try {
    Deno.removeSync(TEMP_DIR, { recursive: true });
  } catch {
    // ignore teardown failures
  }
});

for (const count of RECORD_COUNTS) {
  Deno.bench(`sync avro writer (file, ${count} records)`, () => {
    const records = generateRecords(count);
    const filePath = Deno.makeTempFileSync({
      dir: TEMP_DIR,
      prefix: "sync-avro-writer-",
    });
    try {
      writeRecordsToFile(filePath, records);
    } finally {
      Deno.removeSync(filePath);
    }
  });
}

for (const count of RECORD_COUNTS) {
  const sampleFile = SAMPLE_FILES.get(count)!;
  Deno.bench(`sync avro reader (file, ${count} records)`, () => {
    const countRead = readRecordsFromFile(sampleFile);
    if (countRead !== count) {
      throw new Error("Sync reader failed to read all records");
    }
  });
}

for (const count of RECORD_COUNTS) {
  Deno.bench(`sync avro round trip (file, ${count} records)`, () => {
    const records = generateRecords(count);
    const filePath = Deno.makeTempFileSync({
      dir: TEMP_DIR,
      prefix: "sync-avro-roundtrip-",
    });
    try {
      writeRecordsToFile(filePath, records);
      const reader = SyncAvroReader.fromStream(new FsSyncStreamReader(filePath));
      let sum = 0;
      for (const record of reader.iterRecords()) {
        sum += Number((record as { value: number }).value);
      }
      reader.close();
      if (sum <= 0) {
        throw new Error("Invalid round-trip sum");
      }
    } finally {
      Deno.removeSync(filePath);
    }
  });
}
