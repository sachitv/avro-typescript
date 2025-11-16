import { readAvroRecords } from "./read_avro_async.ts";
import { readAvroRecordsFromBlob } from "./read_avro_blob.ts";

const SAMPLE_PERSON_AVRO = new URL(
  "../../js/test/dat/person-10.avro",
  import.meta.url,
);

function sampleAvroPath(): string {
  return SAMPLE_PERSON_AVRO.pathname;
}
// These example scripts are exercised during CI `deno test` runs to help
// ensure they stay correct for downstream users.

Deno.test("examples/read_avro_async reads the sample file", async () => {
  const records: unknown[] = [];
  for await (const record of readAvroRecords(sampleAvroPath())) {
    records.push(record);
  }

  if (records.length === 0) {
    throw new Error(
      "Expected to read at least one record from the sample AVRO file.",
    );
  }
});

Deno.test("examples/read_avro_blob reads the sample file", async () => {
  const records: unknown[] = [];
  for await (const record of readAvroRecordsFromBlob(sampleAvroPath())) {
    records.push(record);
  }

  if (records.length === 0) {
    throw new Error(
      "Expected to read at least one record from the sample AVRO file via blob.",
    );
  }
});

Deno.test("examples/stream_adapter_demo runs on import", async () => {
  await import("./stream_adapter_demo.ts");
});

Deno.test("examples/stream_transform_roundtrip_demo runs on import", async () => {
  await import("./stream_transform_roundtrip_demo.ts");
});
