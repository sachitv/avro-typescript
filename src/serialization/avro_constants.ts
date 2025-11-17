import { createType } from "../type/create_type.ts";
import type { Type } from "../schemas/type.ts";

// Type of Avro header.
export const HEADER_TYPE: Type = createType({
  type: "record",
  name: "org.apache.avro.file.Header",
  fields: [
    { name: "magic", type: { type: "fixed", name: "Magic", size: 4 } },
    { name: "meta", type: { type: "map", values: "bytes" } },
    { name: "sync", type: { type: "fixed", name: "Sync", size: 16 } },
  ],
});

// Type of each block.
export const BLOCK_TYPE: Type = createType({
  type: "record",
  name: "org.apache.avro.file.Block",
  fields: [
    { name: "count", type: "long" },
    { name: "data", type: "bytes" },
    { name: "sync", type: { type: "fixed", name: "Sync", size: 16 } },
  ],
});

// First 4 bytes of an Avro object container file.
export const MAGIC_BYTES: Uint8Array = new Uint8Array([0x4F, 0x62, 0x6A, 0x01]); // 'Obj\x01'
