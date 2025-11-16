// Example: demonstrate loading an AVRO blob and iterating records.
import { AvroReader } from "../src/avro_reader.ts";

/**
 * Asynchronously reads an AVRO file as a blob and yields each record.
 */
export async function* readAvroRecordsFromBlob(
  filePath: string =
    new URL("../test-data/weather.avro", import.meta.url).pathname,
): AsyncIterableIterator<unknown> {
  const file = await Deno.open(filePath, { read: true });
  const response = new Response(file.readable);
  const blob = await response.blob();
  const reader = AvroReader.fromBlob(blob);

  const header = await reader.getHeader();

  // read all the data in the map as Text Encoded key and Uint8Array value
  for (const [key, value] of header.meta) {
    console.log(`Meta [${key}]:`, new TextDecoder().decode(value));
  }

  console.log("Schema:", header.meta.get("avro.schema"));

  console.log("Sync Marker:", header.sync);

  // Yield each record asynchronously
  for await (const record of reader.iterRecords()) {
    yield record;
  }
}

const filePath = new URL("../test-data/weather.avro", import.meta.url).pathname;

for await (const record of readAvroRecordsFromBlob(filePath)) {
  console.log(record);
}
