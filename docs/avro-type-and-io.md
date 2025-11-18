# Working with Avro Types and Streaming IO

This document explains how to construct Avro `Type` objects with `createType`
and how to read and write Avro object container files via the exported reader
and writer helpers, including how to plug in custom readable and writable
buffers.

## Constructing Avro Types with `createType`

The `createType` factory lives in `src/internal/createType/mod.ts` and turns any
valid Avro schema into a `Type` instance that is consumed throughout the reader
and writer layers.

- **Supported inputs**:
  - Primitive names: pass strings like `"int"`, `"string"`, or `"boolean"`.
  - Full schema objects: records, enums, maps, arrays, unions, logical types,
    etc.
  - Recursive namespaces: `createType` handles namespace resolution and registry
    tracking for named types.
  - Nested unions: supply an array of schema-like objects/strings to build
    unions.
  - `Type` instances: it will return the instance unchanged when given one.

- **Options** (`CreateTypeOptions`):
  - `namespace`: override or set the namespace used when resolving named types.
  - `registry`: reuse a shared `Map<string, Type>` to keep named type instances
    in sync (useful for recursive schema graphs or schema evolution).

```typescript
import { createType } from "../src/type/create_type.ts";

const schema = {
  type: "record",
  name: "example.Event",
  fields: [
    { name: "id", type: "long" },
    { name: "payload", type: ["null", "string"], default: null },
  ],
};

const type = createType(schema, { namespace: "example" });
```

The returned `Type` can be passed directly to serializers, parsers, or reused in
custom tooling that needs schema-aware decoding/encoding.

## Reading Avro Files

`AvroReader` (see `src/avro_reader.ts`) exposes four factory helpers that cater
to the most common data sources. All readers expose the same API for querying
the header and iterating records:

```typescript ignore
import { AvroReader } from "../src/avro_reader.ts";

const buffer = new ArrayBuffer(0);
const options = {};

const reader = AvroReader.fromBuffer(buffer, options);
for await (const record of reader.iterRecords()) {
  // ...
}
await reader.close();
```

### Buffer- or Blob-backed readers

- `AvroReader.fromBuffer(buffer, options)` accepts any implementation of
  `IReadableBuffer` (`../src/serialization/buffers/buffer.ts`). The `buffer`
  must implement `read(offset, size)` and is random-access friendly.
- `AvroReader.fromBlob(blob, options)` wraps the blob with `BlobReadableBuffer`
  to provide random access without materializing the entire file.

### Reading over a network

- `AvroReader.fromUrl(url, options)` fetches the URL, cancels the stream on
  close, and fully supports record- vs. schema-resolution via
  `options.readerSchema`.
- `AvroReader.fromStream(stream, options)` accepts any
  `ReadableStream<Uint8Array>` and adapts it to a buffer using the `cacheSize`
  setting:
  - `cacheSize = 0` (default): unlimited buffering via
    `ForwardOnlyStreamReadableBufferAdapter`.
  - `cacheSize > 0`: limited rolling window via
    `FixedSizeStreamReadableBufferAdapter`.

### Reader options

- `readerSchema`: supply a second schema for schema evolution.
- `decoders`: register custom codec decoders (must not include built-in `"null"`
  or `"deflate"`).
- `closeHook`: run cleanup once the reader is closed (e.g., canceling a stream).
- `fetchInit`: pass custom `RequestInit` when fetching URLs.

## Writing Avro Files

`AvroWriter` mirrors the reader helpers (`src/avro_writer.ts`). It exposes:

- `AvroWriter.toBuffer(buffer, options)`: accepts any `IWritableBuffer` that
  implements `appendBytes(data)` and `isValid()` (see
  `../src/serialization/buffers/buffer.ts`).
- `AvroWriter.toStream(stream, options)`: writes to a
  `WritableStream<Uint8Array>` through `StreamWritableBuffer` +
  `StreamWritableBufferAdapter`.

Writer options (`AvroWriterOptions`) live in
`../src/serialization/avro_file_writer.ts` and allow you to provide:

- `schema`: the writer schema.
- `codec`: `"null"` (default), `"deflate"`, or custom codecs via
  `EncoderRegistry`.
- `metadata`: attach arbitrary metadata key/value pairs to the container file.
- `blockSize`, `syncInterval`: tune when blocks flush.

```typescript
import { AvroWriter } from "../src/avro_writer.ts";

const stream = new WritableStream();
const schema = { type: "string" };
const record = "test";

const writer = AvroWriter.toStream(stream, {
  schema,
  codec: "deflate",
  metadata: { "application": "example-service" },
});
await writer.append(record);
await writer.flushBlock();
await writer.close();
```

## Custom Readables and Writables

- `IReadableBuffer`/`IWritableBuffer` are the extension points for integrating
  Avro IO with your own buffer implementations
  (`../src/serialization/buffers/buffer.ts`).
- For stream sources, the project provides `IStreamReadableBuffer` and
  `IStreamWritableBuffer` along with helpers:
  - `StreamReadableBuffer`, `StreamWritableBuffer`: wrap Web Streams.
  - `ForwardOnlyStreamReadableBufferAdapter`: sequential buffer for unlimited
    forward reads.
  - `FixedSizeStreamReadableBufferAdapter`: sliding window buffer to bound
    memory when reading large files.
  - `StreamWritableBufferAdapter`: exposes an `IWritableBuffer` that writes to a
    `WritableStream`.
- Implementing your own bridge typically means:
  1. Implement `read(offset, size)` (and optionally `length()`) for
     `IReadableBuffer`, or `appendBytes`/`isValid` for `IWritableBuffer`.
  2. Pass the implementation to `AvroReader.fromBuffer` / `AvroWriter.toBuffer`.
  3. Combine with manual caching or thread-safe storage if you need to share the
     buffer across readers/writers.

<!--
Deno.File can't be used in here for some reason.
-->

```typescript ignore
import type { IReadableBuffer } from "../src/serialization/buffers/buffer.ts";
import { AvroReader } from "../src/avro_reader.ts";

const file = null as any;

class FileSystemReadableBuffer implements IReadableBuffer {
  constructor(private file: Deno.File) {}

  async read(offset: number, size: number): Promise<Uint8Array | undefined> {
    const buffer = new Uint8Array(size);
    const { bytesRead } = await this.file.read(buffer, { offset });
    if (bytesRead === null || bytesRead === 0) {
      return undefined;
    }
    return buffer.subarray(0, bytesRead);
  }
}

const reader = AvroReader.fromBuffer(new FileSystemReadableBuffer(file));
```

Use the stream adapters if you want to read from or write to the Web Streams
API, or follow the buffer interfaces above to integrate with custom storage
backends (filesystems, in-memory caches, etc.).
