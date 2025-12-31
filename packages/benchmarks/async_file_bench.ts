import {
  AvroReader,
  AvroWriter,
  SyncAvroWriter,
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

class FsAsyncReadableStream extends ReadableStream<Uint8Array> {
  constructor(path: string, chunkSize = 4096) {
    let file: Deno.FsFile;
    super({
      start() {
        file = Deno.openSync(path, { read: true });
      },
      async pull(controller) {
        const buffer = new Uint8Array(chunkSize);
        const bytesRead = await file.read(buffer);
        if (bytesRead === null || bytesRead === 0) {
          controller.close();
          file.close();
        } else {
          controller.enqueue(buffer.slice(0, bytesRead));
        }
      },
      cancel() {
        file.close();
      },
    });
  }
}

class FsAsyncWritableStream extends WritableStream<Uint8Array> {
  constructor(path: string) {
    let file: Deno.FsFile;
    super({
      start() {
        file = Deno.openSync(path, {
          write: true,
          create: true,
          truncate: true,
        });
      },
      async write(chunk) {
        await file.write(chunk);
      },
      close() {
        file.close();
      },
    });
  }
}

class FsSyncStreamWriterForSetup implements ISyncStreamWritableBuffer {
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

  writeBytesFrom(data: Uint8Array, offset: number, length: number): void {
    if (this.#closed || length === 0) {
      return;
    }
    this.#file.writeSync(data.subarray(offset, offset + length));
  }

  close(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.#file.close();
    }
  }
}

function writeRecordsToFileSync(path: string, records: any[]): void {
  const writer = SyncAvroWriter.toStream(new FsSyncStreamWriterForSetup(path), {
    schema: BENCH_SCHEMA,
  });
  for (const record of records) {
    writer.append(record);
  }
  writer.close();
}

async function writeRecordsToFileAsync(path: string, records: any[]): Promise<void> {
  const stream = new FsAsyncWritableStream(path);
  const writer = AvroWriter.toStream(stream, {
    schema: BENCH_SCHEMA,
  });
  for (const record of records) {
    await writer.append(record);
  }
  await writer.close();
}

async function readRecordsFromFile(path: string): Promise<number> {
  const stream = new FsAsyncReadableStream(path);
  const reader = AvroReader.fromStream(stream);
  let count = 0;
  for await (const _record of reader.iterRecords()) {
    count++;
  }
  await reader.close();
  return count;
}

const TEMP_DIR = Deno.makeTempDirSync({ prefix: "async-avro-bench-" });
for (const count of RECORD_COUNTS) {
  const records = generateRecords(count);
  const filePath = Deno.makeTempFileSync({
    dir: TEMP_DIR,
    prefix: `async-avro-reader-${count}-`,
  });
  writeRecordsToFileSync(filePath, records);
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
  Deno.bench(`async avro writer (file, ${count} records)`, async () => {
    const records = generateRecords(count);
    const filePath = Deno.makeTempFileSync({
      dir: TEMP_DIR,
      prefix: "async-avro-writer-",
    });
    try {
      await writeRecordsToFileAsync(filePath, records);
    } finally {
      Deno.removeSync(filePath);
    }
  });
}

for (const count of RECORD_COUNTS) {
  const sampleFile = SAMPLE_FILES.get(count)!;
  Deno.bench(`async avro reader (file, ${count} records)`, async () => {
    const countRead = await readRecordsFromFile(sampleFile);
    if (countRead !== count) {
      throw new Error("Async reader failed to read all records");
    }
  });
}

for (const count of RECORD_COUNTS) {
  Deno.bench(`async avro round trip (file, ${count} records)`, async () => {
    const records = generateRecords(count);
    const filePath = Deno.makeTempFileSync({
      dir: TEMP_DIR,
      prefix: "async-avro-roundtrip-",
    });
    try {
      await writeRecordsToFileAsync(filePath, records);
      const stream = new FsAsyncReadableStream(filePath);
      const reader = AvroReader.fromStream(stream);
      let sum = 0;
      for await (const record of reader.iterRecords()) {
        sum += Number((record as { value: number }).value);
      }
      await reader.close();
      if (sum <= 0) {
        throw new Error("Invalid round-trip sum");
      }
    } finally {
      Deno.removeSync(filePath);
    }
  });
}