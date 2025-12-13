import { createType } from "../../src/mod.ts";

/**
 * Benchmark schema creation and type operations
 */

const simpleSchema = {
  type: "record",
  name: "SimpleRecord",
  fields: [
    { name: "id", type: "int" },
    { name: "name", type: "string" },
  ],
};

const complexSchema = {
  type: "record",
  name: "ComplexRecord",
  fields: [
    { name: "id", type: "long" },
    { name: "name", type: "string" },
    { name: "email", type: "string" },
    { name: "age", type: "int" },
    { name: "tags", type: { type: "array", items: "string" } },
  ],
};

Deno.bench("create simple type", () => {
  createType(simpleSchema);
});

Deno.bench("create complex type", () => {
  createType(complexSchema);
});

Deno.bench("type serialization/deserialization", async () => {
  const type = createType(complexSchema);
  const testData = {
    id: 123456789n,
    name: "John Doe",
    email: "john@example.com",
    age: 30,
    tags: ["developer", "typescript", "avro"],
  };

  // This will exercise the type's serialization logic
  const serialized = await type.toBuffer(testData);
  const deserialized = await type.fromBuffer(serialized);

  if ((deserialized as any).id !== testData.id) {
    throw new Error("Type round-trip failed");
  }
});