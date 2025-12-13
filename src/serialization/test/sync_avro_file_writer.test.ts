import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  SyncInMemoryReadableBuffer,
  SyncInMemoryWritableBuffer,
} from "../buffers/sync_in_memory_buffer.ts";
import {
  SyncAvroFileWriter,
  type SyncAvroWriterOptions,
} from "../sync_avro_file_writer.ts";
import { SyncAvroFileParser } from "../sync_avro_file_parser.ts";
import type { SyncAvroFileParserOptions } from "../sync_avro_file_parser.ts";
import { SyncStreamWritableBufferAdapter } from "../streams/sync_stream_writable_buffer_adapter.ts";
import { SyncFixedSizeStreamWriter } from "../streams/sync_fixed_size_stream_writer.ts";

const SIMPLE_SCHEMA = {
  type: "record",
  name: "test.Simple",
  fields: [
    { name: "id", type: "int" },
    { name: "name", type: "string" },
  ],
} as const;

const SAMPLE_RECORDS = [
  { id: 1, name: "alpha" },
  { id: 2, name: "beta" },
  { id: 3, name: "gamma" },
];

function createWritableBuffer(size = 8192): SyncInMemoryWritableBuffer {
  return new SyncInMemoryWritableBuffer(new ArrayBuffer(size));
}

function toReadableBuffer(
  buffer: SyncInMemoryWritableBuffer,
): SyncInMemoryReadableBuffer {
  return new SyncInMemoryReadableBuffer(buffer.getBufferCopy());
}

function collectRecords(
  buffer: SyncInMemoryWritableBuffer,
  parserOptions?: SyncAvroFileParserOptions,
): unknown[] {
  const parser = new SyncAvroFileParser(
    toReadableBuffer(buffer),
    parserOptions,
  );
  return Array.from(parser.iterRecords());
}

describe("SyncAvroFileWriter", () => {
  describe("constructor validation", () => {
    it("requires a schema", () => {
      const buffer = createWritableBuffer();
      assertThrows(
        () =>
          new SyncAvroFileWriter(
            buffer,
            undefined as unknown as SyncAvroWriterOptions,
          ),
        Error,
        "requires a schema",
      );
    });

    it("rejects invalid block sizes", () => {
      const buffer = createWritableBuffer();
      for (const blockSize of [-1, 0, 1.5, Infinity]) {
        assertThrows(
          () =>
            new SyncAvroFileWriter(buffer, {
              schema: SIMPLE_SCHEMA,
              blockSize,
            }),
          RangeError,
        );
      }
    });

    it("rejects invalid sync marker length", () => {
      const buffer = createWritableBuffer();
      assertThrows(
        () =>
          new SyncAvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            syncMarker: new Uint8Array(4),
          }),
        Error,
        "Sync marker must be",
      );
    });

    it("rejects overriding built-in encoders", () => {
      const buffer = createWritableBuffer();
      assertThrows(
        () =>
          new SyncAvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            encoders: { "null": { encode: (data) => data } },
          }),
        Error,
        "Cannot override built-in encoder",
      );
    });

    it("throws when codec has no encoder", () => {
      const buffer = createWritableBuffer();
      assertThrows(
        () =>
          new SyncAvroFileWriter(buffer, {
            schema: SIMPLE_SCHEMA,
            codec: "custom",
          }),
        Error,
        "Unsupported codec: custom",
      );
    });
  });

  describe("writing behavior", () => {
    it("round-trips sample records", () => {
      const buffer = createWritableBuffer();
      const writer = new SyncAvroFileWriter(buffer, { schema: SIMPLE_SCHEMA });
      for (const record of SAMPLE_RECORDS) {
        writer.append(record);
      }
      writer.close();

      const records = collectRecords(buffer);
      assertEquals(records, SAMPLE_RECORDS);
    });

    it("auto flushes when block size is exceeded", () => {
      const buffer = createWritableBuffer();
      const writer = new SyncAvroFileWriter(buffer, {
        schema: SIMPLE_SCHEMA,
        blockSize: 1,
      });
      writer.append(SAMPLE_RECORDS[0]);
      writer.append(SAMPLE_RECORDS[1]);
      writer.close();

      const records = collectRecords(buffer);
      assertEquals(records.length, 2);
    });

    it("flushBlock writes pending data and no-ops when empty", () => {
      const buffer = createWritableBuffer();
      const writer = new SyncAvroFileWriter(buffer, { schema: SIMPLE_SCHEMA });
      writer.flushBlock(); // no pending data
      writer.append(SAMPLE_RECORDS[0]);
      writer.flushBlock();
      writer.close();

      const records = collectRecords(buffer);
      assertEquals(records, [SAMPLE_RECORDS[0]]);
    });

    it("close is idempotent and prevents further writes", () => {
      const buffer = createWritableBuffer();
      const writer = new SyncAvroFileWriter(buffer, { schema: SIMPLE_SCHEMA });
      writer.close();
      writer.close();
      assertThrows(
        () => writer.append(SAMPLE_RECORDS[0]),
        Error,
        "already closed",
      );
    });

    it("rejects invalid records", () => {
      const buffer = createWritableBuffer();
      const writer = new SyncAvroFileWriter(buffer, { schema: SIMPLE_SCHEMA });
      assertThrows(
        () => writer.append({ bad: true }),
        Error,
        "Record does not conform",
      );
    });

    it("writes header when no records appended", () => {
      const buffer = createWritableBuffer();
      const writer = new SyncAvroFileWriter(buffer, { schema: SIMPLE_SCHEMA });
      writer.close();
      const parser = new SyncAvroFileParser(toReadableBuffer(buffer));
      const header = parser.getHeader();
      assertEquals(header.magic.length, 4);
      assertEquals(Array.from(parser.iterRecords()).length, 0);
    });

    it("uses provided sync marker", () => {
      const buffer = createWritableBuffer();
      const marker = new Uint8Array(16);
      marker.fill(0x9a);
      const writer = new SyncAvroFileWriter(buffer, {
        schema: SIMPLE_SCHEMA,
        syncMarker: marker,
      });
      writer.append(SAMPLE_RECORDS[0]);
      writer.close();

      const parser = new SyncAvroFileParser(toReadableBuffer(buffer));
      const header = parser.getHeader();
      assertEquals(header.sync, marker);
    });

    it("accepts metadata map entries and string/object entries", () => {
      const mapMeta = new Map<string, Uint8Array>();
      mapMeta.set("producer", new TextEncoder().encode("sync-tests"));
      mapMeta.set("payload", new Uint8Array([1, 2, 3]));
      const bufferMap = createWritableBuffer();
      const writerMap = new SyncAvroFileWriter(bufferMap, {
        schema: SIMPLE_SCHEMA,
        metadata: mapMeta,
      });
      writerMap.close();

      const headerMap = new SyncAvroFileParser(toReadableBuffer(bufferMap))
        .getHeader();
      assertEquals(
        new TextDecoder().decode(headerMap.meta.get("producer")),
        "sync-tests",
      );
      assertEquals(headerMap.meta.get("payload"), new Uint8Array([1, 2, 3]));

      const bufferObj = createWritableBuffer();
      const writerObj = new SyncAvroFileWriter(bufferObj, {
        schema: SIMPLE_SCHEMA,
        metadata: { note: "object-meta" },
      });
      writerObj.close();
      const headerObj = new SyncAvroFileParser(toReadableBuffer(bufferObj))
        .getHeader();
      assertEquals(
        new TextDecoder().decode(headerObj.meta.get("note")),
        "object-meta",
      );

      assertThrows(
        () =>
          new SyncAvroFileWriter(createWritableBuffer(), {
            schema: SIMPLE_SCHEMA,
            metadata: { "": "oops" },
          }),
        Error,
        "non-empty",
      );
      assertThrows(
        () =>
          new SyncAvroFileWriter(createWritableBuffer(), {
            schema: SIMPLE_SCHEMA,
            metadata: { "avro.schema": "bad" },
          }),
        Error,
        "reserved",
      );
    });

    it("writes to sync stream adapters", () => {
      const sink = new SyncFixedSizeStreamWriter(new Uint8Array(4096));
      const adapter = new SyncStreamWritableBufferAdapter(sink);
      const writer = new SyncAvroFileWriter(adapter, { schema: SIMPLE_SCHEMA });
      writer.append(SAMPLE_RECORDS[0]);
      writer.close();
      adapter.close();
      const bytes = sink.toUint8Array().slice();
      const readable = new SyncInMemoryReadableBuffer(bytes.buffer);
      const parser = new SyncAvroFileParser(readable);
      const records = Array.from(parser.iterRecords());
      assertEquals(records, [SAMPLE_RECORDS[0]]);
    });

    it("supports custom codecs via encoders/decoders", () => {
      const buffer = createWritableBuffer();
      const writer = new SyncAvroFileWriter(buffer, {
        schema: SIMPLE_SCHEMA,
        codec: "custom",
        encoders: { "custom": { encode: (data) => data } },
      });
      writer.append(SAMPLE_RECORDS[0]);
      writer.close();

      const records = collectRecords(buffer, {
        decoders: { "custom": { decode: (data) => data } },
      });
      assertEquals(records, [SAMPLE_RECORDS[0]]);
    });
  });
});
