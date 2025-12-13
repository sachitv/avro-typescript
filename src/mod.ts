// Core Avro functionality
export { AvroReader } from "./avro_reader.ts";
export { AvroWriter } from "./avro_writer.ts";
export { createType } from "./type/create_type.ts";

// Schema types
export { Type } from "./schemas/type.ts";
export { BaseType } from "./schemas/base_type.ts";
export { PrimitiveType } from "./schemas/primitive/primitive_type.ts";
export { FixedSizeBaseType } from "./schemas/primitive/fixed_size_base_type.ts";
export { NamedType } from "./schemas/complex/named_type.ts";
export {
  LogicalType,
  NamedLogicalType,
  withLogicalTypeJSON,
} from "./schemas/logical/logical_type.ts";

// Primitive types
export { BooleanType } from "./schemas/primitive/boolean_type.ts";
export { IntType } from "./schemas/primitive/int_type.ts";
export { LongType } from "./schemas/primitive/long_type.ts";
export { FloatType } from "./schemas/primitive/float_type.ts";
export { DoubleType } from "./schemas/primitive/double_type.ts";
export { BytesType } from "./schemas/primitive/bytes_type.ts";
export { StringType } from "./schemas/primitive/string_type.ts";
export { NullType } from "./schemas/primitive/null_type.ts";

// Complex types
export { RecordType } from "./schemas/complex/record_type.ts";
export { ArrayType } from "./schemas/complex/array_type.ts";
export { MapType } from "./schemas/complex/map_type.ts";
export { UnionType } from "./schemas/complex/union_type.ts";
export { EnumType } from "./schemas/complex/enum_type.ts";
export { FixedType } from "./schemas/complex/fixed_type.ts";

// Logical types
export { UuidLogicalType } from "./schemas/logical/uuid_logical_type.ts";
export { DecimalLogicalType } from "./schemas/logical/decimal_logical_type.ts";
export { DurationLogicalType } from "./schemas/logical/duration_logical_type.ts";
export { DateLogicalType } from "./schemas/logical/temporal_logical_types.ts";
export { TimeMillisLogicalType } from "./schemas/logical/temporal_logical_types.ts";
export { TimeMicrosLogicalType } from "./schemas/logical/temporal_logical_types.ts";
export { TimestampMillisLogicalType } from "./schemas/logical/temporal_logical_types.ts";
export { TimestampMicrosLogicalType } from "./schemas/logical/temporal_logical_types.ts";
export { TimestampNanosLogicalType } from "./schemas/logical/temporal_logical_types.ts";
export { LocalTimestampMillisLogicalType } from "./schemas/logical/temporal_logical_types.ts";
export { LocalTimestampMicrosLogicalType } from "./schemas/logical/temporal_logical_types.ts";
export { LocalTimestampNanosLogicalType } from "./schemas/logical/temporal_logical_types.ts";

// Schema utilities
export { Resolver } from "./schemas/resolver.ts";
export { ValidationError } from "./schemas/error.ts";
export { safeStringify } from "./schemas/json.ts";

// Serialization
export type {
  AvroFileParserOptions,
  AvroHeader,
  ParsedAvroHeader,
} from "./serialization/avro_file_parser.ts";
export type { AvroWriterOptions } from "./serialization/avro_file_writer.ts";
export { AvroFileParser } from "./serialization/avro_file_parser.ts";
export { AvroFileWriter } from "./serialization/avro_file_writer.ts";
export {
  BLOCK_TYPE,
  HEADER_TYPE,
  MAGIC_BYTES,
} from "./serialization/avro_constants.ts";

// Buffers
export type {
  IReadableAndWritableBuffer,
  IReadableBuffer,
  IWritableBuffer,
} from "./serialization/buffers/buffer.ts";
export { BlobReadableBuffer } from "./serialization/buffers/blob_readable_buffer.ts";
export {
  InMemoryReadableBuffer,
  InMemoryWritableBuffer,
} from "./serialization/buffers/in_memory_buffer.ts";

// Streams
export type {
  IStreamReadableBuffer,
  IStreamWritableBuffer,
} from "./serialization/streams/streams.ts";
export { StreamReadableBuffer } from "./serialization/streams/stream_readable_buffer.ts";
export { StreamWritableBuffer } from "./serialization/streams/stream_writable_buffer.ts";
export { StreamReadableBufferAdapter } from "./serialization/streams/stream_readable_buffer_adapter.ts";
export { StreamWritableBufferAdapter } from "./serialization/streams/stream_writable_buffer_adapter.ts";
export { FixedSizeStreamReadableBufferAdapter } from "./serialization/streams/fixed_size_stream_readable_buffer_adapter.ts";
export { ForwardOnlyStreamReadableBufferAdapter } from "./serialization/streams/forward_only_stream_readable_buffer_adapter.ts";

// Encoders/Decoders
export type { Encoder } from "./serialization/encoders/encoder.ts";
export type {
  SyncEncoder,
  SyncEncoderRegistry,
} from "./serialization/encoders/sync_encoder.ts";
export type { Decoder } from "./serialization/decoders/decoder.ts";
export type {
  SyncDecoder,
  SyncDecoderRegistry,
} from "./serialization/decoders/sync_decoder.ts";
export { NullEncoder } from "./serialization/encoders/null_encoder.ts";
export { DeflateEncoder } from "./serialization/encoders/deflate_encoder.ts";
export { NullSyncEncoder } from "./serialization/encoders/null_sync_encoder.ts";
export { NullDecoder } from "./serialization/decoders/null_decoder.ts";
export { DeflateDecoder } from "./serialization/decoders/deflate_decoder.ts";
export { NullSyncDecoder } from "./serialization/decoders/null_sync_decoder.ts";

// Taps
export type { ReadableTapLike, WritableTapLike } from "./serialization/tap.ts";
export { ReadableTap, WritableTap } from "./serialization/tap.ts";

// Utilities
export { decode, encode } from "./serialization/text_encoding.ts";

// RPC - Protocols
export { Protocol } from "./rpc/protocol_core.ts";

// RPC - Message endpoints
export { MessageEndpoint } from "./rpc/message_endpoint/base.ts";
export {
  MessageEmitter,
  StatelessEmitter,
} from "./rpc/message_endpoint/emitter.ts";
export {
  MessageListener,
  StatelessListener,
} from "./rpc/message_endpoint/listener.ts";

// RPC - Definitions
export type {
  CallRequestEnvelopeOptions,
  MessageDefinition,
  MessageTransportOptions,
  ProtocolDefinition,
  ProtocolHandlerContext,
  ProtocolInfo,
  ProtocolLike,
  ProtocolOptions,
  ResolverEntry,
} from "./rpc/definitions/protocol_definitions.ts";
export { Message } from "./rpc/definitions/message_definition.ts";

// RPC - Wire Format
export type {
  HandshakeRequestInit,
  HandshakeRequestMessage,
  HandshakeResponseInit,
  HandshakeResponseMessage,
} from "./rpc/protocol/wire_format/handshake.ts";
export type {
  CallRequestEnvelope,
  CallRequestInit,
  CallRequestMessage,
  CallResponseEnvelope,
  CallResponseInit,
  CallResponseMessage,
  DecodeCallRequestOptions,
  DecodeCallResponseOptions,
} from "./rpc/protocol/wire_format/messages.ts";
export type {
  DecodeFramedMessageOptions,
  DecodeFramedMessageResult,
  FrameMessageOptions,
} from "./rpc/protocol/wire_format/framing.ts";

// RPC - Transports
export type {
  BinaryDuplex,
  BinaryDuplexLike,
  BinaryReadable,
  BinaryWritable,
  FetchTransportOptions,
  WebSocketTransportOptions,
} from "./rpc/protocol/transports/transport_helpers.ts";
export type { InMemoryTransportPair } from "./rpc/protocol/transports/in_memory.ts";
export { createFetchTransport } from "./rpc/protocol/transports/fetch.ts";
export { createWebSocketTransport } from "./rpc/protocol/transports/websocket.ts";
export {
  createInMemoryTransport,
  createInMemoryTransportPair,
} from "./rpc/protocol/transports/in_memory.ts";
