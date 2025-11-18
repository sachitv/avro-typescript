import { createType } from "../type/create_type.ts";
import type { Type } from "../schemas/type.ts";

/**
 * Avro schema definition for the object container file header section.
 */
export const HEADER_TYPE: Type = createType({
  type: "record",
  name: "org.apache.avro.file.Header",
  fields: [
    { name: "magic", type: { type: "fixed", name: "Magic", size: 4 } },
    { name: "meta", type: { type: "map", values: "bytes" } },
    { name: "sync", type: { type: "fixed", name: "Sync", size: 16 } },
  ],
});

/**
 * Avro schema definition for individual data blocks within a container file.
 */
export const BLOCK_TYPE: Type = createType({
  type: "record",
  name: "org.apache.avro.file.Block",
  fields: [
    { name: "count", type: "long" },
    { name: "data", type: "bytes" },
    { name: "sync", type: { type: "fixed", name: "Sync", size: 16 } },
  ],
});

/**
 * Magic bytes that prefix every Avro object container file (`Obj\x01`).
 */
export const MAGIC_BYTES: Uint8Array = new Uint8Array([0x4F, 0x62, 0x6A, 0x01]); // 'Obj\x01'
