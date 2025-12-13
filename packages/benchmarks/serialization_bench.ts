import { createType, AvroReader, AvroWriter } from "../../src/mod.ts";

/**
 * Benchmark serialization performance
 */

const schema = {
  type: "record",
  name: "TestRecord",
  fields: [
    { name: "id", type: "int" },
    { name: "name", type: "string" },
    { name: "value", type: "double" },
    { name: "active", type: "boolean" },
    { name: "data", type: "bytes" },
  ],
};

const testType = createType(schema);
const testData = {
  id: 12345,
  name: "benchmark test record",
  value: 3.14159,
  active: true,
  data: new Uint8Array([1, 2, 3, 4, 5]),
};

Deno.bench("serialize single record", async () => {
  await testType.toBuffer(testData);
});

Deno.bench("deserialize single record", async () => {
  const serialized = await testType.toBuffer(testData);
  await testType.fromBuffer(serialized);
});

Deno.bench("round-trip serialization", async () => {
  const serialized = await testType.toBuffer(testData);
  const result = await testType.fromBuffer(serialized);

  // Verify correctness
  if ((result as any).id !== testData.id) {
    throw new Error("Round-trip failed");
  }
});