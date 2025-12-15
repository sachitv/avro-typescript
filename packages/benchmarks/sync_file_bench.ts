import {
  SyncAvroReader,
  SyncAvroWriter,
  type ISyncStreamReadableBuffer,
  type ISyncStreamWritableBuffer,
} from "../../src/mod.ts";
import type { SchemaLike } from "../../src/type/create_type.ts";
import avsc from "npm:avsc";
import avrojs from "npm:avro-js";
import { once } from "node:events";
import type { Readable, Writable } from "node:stream";
import type { BenchmarkLibrary } from "./library_targets.ts";
type AvscEncoderSchema = Parameters<typeof avsc.createFileEncoder>[1];

const BENCH_SCHEMA: SchemaLike = {
  type: "record",
  name: "BenchRecord",
  fields: [
    { name: "id", type: "long" },
    { name: "label", type: "string" },
    { name: "value", type: "double" },
    { name: "active", type: "boolean" },
  ],
};

type BenchRecord = {
  id: bigint;
  label: string;
  value: number;
  active: boolean;
};

type BenchRecordLike = {
  value: number;
};

function generateRecords(count: number): BenchRecord[] {
  return Array.from({ length: count }, (_, index) => ({
    id: BigInt(index),
    label: `record-${index}`,
    value: index * Math.PI,
    active: index % 2 === 0,
  }));
}

const RECORD_COUNTS = [1000, 10000, 100000];
const SAMPLE_FILES = new Map<BenchmarkLibrary, Map<number, string>>();

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

const TEMP_DIR = Deno.makeTempDirSync({ prefix: "sync-avro-bench-" });

interface SyncFileTarget {
  id: BenchmarkLibrary;
  label: string;
  writeRecords(path: string, records: BenchRecord[]): Promise<void>;
  iterateRecords(path: string, onRecord: (record: BenchRecordLike) => void): Promise<void>;
}

const syncFileTargets: SyncFileTarget[] = [
  {
    id: "avro-typescript",
    label: "avro-typescript",
    writeRecords: async (path, records) => {
      const writer = SyncAvroWriter.toStream(new FsSyncStreamWriter(path), {
        schema: BENCH_SCHEMA,
      });
      try {
        for (const record of records) {
          writer.append(record);
        }
      } finally {
        writer.close();
      }
    },
    iterateRecords: async (path, onRecord) => {
      const reader = SyncAvroReader.fromStream(new FsSyncStreamReader(path));
      try {
        for (const record of reader.iterRecords()) {
          onRecord(record as BenchRecordLike);
        }
      } finally {
        reader.close();
      }
    },
  },
  {
    id: "avsc",
    label: "avsc",
    writeRecords: (path, records) =>
      writeWithNodeEncoder(
        avsc.createFileEncoder(path, BENCH_SCHEMA as AvscEncoderSchema),
        records,
      ),
    iterateRecords: (path, onRecord) =>
      iterateNodeDecoder(avsc.createFileDecoder(path), onRecord),
  },
  {
    id: "avro-js",
    label: "avro-js",
    writeRecords: (path, records) =>
      writeWithNodeEncoder(avrojs.createFileEncoder(path, BENCH_SCHEMA), records),
    iterateRecords: (path, onRecord) =>
      iterateNodeDecoder(avrojs.createFileDecoder(path), onRecord),
  },
];

await prepareSampleFiles();

globalThis.addEventListener("unload", () => {
  try {
    Deno.removeSync(TEMP_DIR, { recursive: true });
  } catch {
    // ignore teardown failures
  }
});

for (const target of syncFileTargets) {
  for (const count of RECORD_COUNTS) {
    Deno.bench(
      `sync avro writer (file, ${count} records) [${target.label}]`,
      async () => {
        const records = generateRecords(count);
        const filePath = Deno.makeTempFileSync({
          dir: TEMP_DIR,
          prefix: `sync-${target.id}-writer-`,
        });
        try {
          await target.writeRecords(filePath, records);
        } finally {
          Deno.removeSync(filePath);
        }
      },
    );
  }
}

for (const target of syncFileTargets) {
  for (const count of RECORD_COUNTS) {
    const sampleFile = SAMPLE_FILES.get(target.id)?.get(count);
    if (!sampleFile) {
      continue;
    }
    Deno.bench(
      `sync avro reader (file, ${count} records) [${target.label}]`,
      async () => {
        let readCount = 0;
        await target.iterateRecords(sampleFile, () => {
          readCount++;
        });
        if (readCount !== count) {
          throw new Error(
            `${target.label} reader failed to read all ${count} records`,
          );
        }
      },
    );
  }
}

for (const target of syncFileTargets) {
  for (const count of RECORD_COUNTS) {
    Deno.bench(
      `sync avro round trip (file, ${count} records) [${target.label}]`,
      async () => {
        const records = generateRecords(count);
        const filePath = Deno.makeTempFileSync({
          dir: TEMP_DIR,
          prefix: `sync-${target.id}-roundtrip-`,
        });
        try {
          await target.writeRecords(filePath, records);
          let sum = 0;
          await target.iterateRecords(filePath, (record) => {
            sum += Number((record as { value?: number }).value ?? 0);
          });
          if (sum <= 0) {
            throw new Error(`Invalid round-trip sum for ${target.label}`);
          }
        } finally {
          Deno.removeSync(filePath);
        }
      },
    );
  }
}

async function prepareSampleFiles(): Promise<void> {
  for (const target of syncFileTargets) {
    const filesForTarget = new Map<number, string>();
    SAMPLE_FILES.set(target.id, filesForTarget);
    for (const count of RECORD_COUNTS) {
      const records = generateRecords(count);
      const filePath = Deno.makeTempFileSync({
        dir: TEMP_DIR,
        prefix: `sync-${target.id}-sample-${count}-`,
      });
      await target.writeRecords(filePath, records);
      filesForTarget.set(count, filePath);
    }
  }
}

async function writeWithNodeEncoder(
  encoder: Writable,
  records: BenchRecord[],
): Promise<void> {
  try {
    for (const record of records) {
      if (!encoder.write(formatRecordForNode(record))) {
        await once(encoder, "drain");
      }
    }
  } finally {
    encoder.end();
  }
  await waitForFinish(encoder);
}

async function iterateNodeDecoder(
  decoder: Readable & AsyncIterable<unknown>,
  onRecord: (record: BenchRecordLike) => void,
): Promise<void> {
  try {
    for await (const record of decoder as AsyncIterable<BenchRecordLike>) {
      onRecord(record);
    }
  } finally {
    decoder.destroy();
  }
}

function formatRecordForNode(record: BenchRecord) {
  return {
    id: Number(record.id),
    label: record.label,
    value: record.value,
    active: record.active,
  };
}

function waitForFinish(stream: Writable): Promise<void> {
  return new Promise((resolve, reject) => {
    stream.once("finish", resolve);
    stream.once("error", reject);
  });
}
