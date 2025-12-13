// These example scripts are exercised during CI `deno test` runs to help
// ensure they stay correct for downstream users.

Deno.test("examples/read_avro_async reads the sample file", async () => {
  await import("../read_avro_async.ts");
});

Deno.test("examples/read_avro_blob reads the sample file", async () => {
  await import("../read_avro_blob.ts");
});

Deno.test("examples/stream_adapter_demo runs on import", async () => {
  await import("../stream_adapter_demo.ts");
});

Deno.test("examples/stream_transform_roundtrip_demo runs on import", async () => {
  await import("../stream_transform_roundtrip_demo.ts");
});

Deno.test("examples/read_avro_sync runs on import", async () => {
  await import("../read_avro_sync.ts");
});
