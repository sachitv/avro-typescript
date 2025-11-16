import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { toBinaryDuplex } from "./transport_helpers.ts";
import {
  createInMemoryTransport,
  createInMemoryTransportPair,
} from "./in_memory.ts";

async function readAll(
  readable: ReturnType<typeof toBinaryDuplex>["readable"],
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  while (true) {
    const chunk = await readable.read();
    if (chunk === null) break;
    chunks.push(chunk);
  }
  const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }
  return merged;
}

describe("createInMemoryTransport", () => {
  it(
    "delivers queued server connections immediately",
    { sanitizeOps: false, sanitizeResources: false, sanitizeExit: false },
    async () => {
      const transport = createInMemoryTransportPair();
      const client = await transport.clientFactory();
      const server = await transport.accept();

      const clientDuplex = toBinaryDuplex(client);
      const serverDuplex = toBinaryDuplex(server);

      const payload = new TextEncoder().encode("ping");
      await clientDuplex.writable.write(payload);
      await clientDuplex.writable.close();

      const received = await serverDuplex.readable.read();
      assertEquals(received, payload);
      const end = await serverDuplex.readable.read();
      assertEquals(end, null);
      await serverDuplex.writable.close();
    },
  );

  it(
    "awaits server connections when none are queued",
    { sanitizeOps: false, sanitizeResources: false, sanitizeExit: false },
    async () => {
      const transport = createInMemoryTransportPair();
      const acceptPromise = transport.accept();

      const client = await transport.clientFactory();
      const server = await acceptPromise;

      const clientDuplex = toBinaryDuplex(client);
      const serverDuplex = toBinaryDuplex(server);

      await serverDuplex.writable.write(new TextEncoder().encode("pong"));
      await serverDuplex.writable.close();

      const result = await readAll(clientDuplex.readable);
      assertEquals(new TextDecoder().decode(result), "pong");
    },
  );

  it(
    "resolves pending readers before writes and handles closing",
    { sanitizeOps: false, sanitizeResources: false, sanitizeExit: false },
    async () => {
      const transport = createInMemoryTransportPair();
      const client = await transport.clientFactory();
      const server = await transport.accept();

      const clientDuplex = toBinaryDuplex(client);
      const serverDuplex = toBinaryDuplex(server);

      const pendingRead = serverDuplex.readable.read();
      const payload = new TextEncoder().encode("pending");
      await clientDuplex.writable.write(payload);
      assertEquals(await pendingRead, payload);

      const pendingNull = serverDuplex.readable.read();
      await clientDuplex.writable.close();
      assertEquals(await pendingNull, null);

      assertThrows(
        () => {
          clientDuplex.writable.write(new Uint8Array([1]));
        },
        Error,
        "Cannot write to closed channel.",
      );
      await clientDuplex.writable.close();
      await serverDuplex.writable.close();
      await serverDuplex.writable.close();
    },
  );

  it("exposes a factory-only transport", async () => {
    const factory = createInMemoryTransport();
    const client = await factory();
    await client.writable.close();
  });
});
