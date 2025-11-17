import { assertEquals } from "@std/assert";
import { beforeAll, describe, it } from "@std/testing/bdd";
import { Protocol } from "../../../protocol_core.ts";
import type { ProtocolDefinition } from "../../../definitions/protocol_definitions.ts";
import { createInMemoryTransportPair } from "../in_memory.ts";
import type { BinaryDuplexLike } from "../transport_helpers.ts";
import type { StatelessListener } from "../../../message_endpoint/listener.ts";
import { AvroFileParser } from "../../../../serialization/avro_file_parser.ts";
import { InMemoryReadableBuffer } from "../../../../serialization/buffers/in_memory_buffer.ts";

let protocolDefinition: ProtocolDefinition;

async function loadProtocolDefinition() {
  const url = new URL(
    "../../../../../test-data/schemas/simple.avpr",
    import.meta.url,
  );
  const text = await Deno.readTextFile(url);
  protocolDefinition = JSON.parse(text) as ProtocolDefinition;
}

async function readInteropDatum(path: string): Promise<unknown> {
  const url = new URL(path, import.meta.url);
  const bytes = await Deno.readFile(url);
  const arrayBuffer = bytes.buffer.slice(
    bytes.byteOffset,
    bytes.byteOffset + bytes.byteLength,
  );
  const parser = new AvroFileParser(new InMemoryReadableBuffer(arrayBuffer));
  for await (const record of parser.iterRecords()) {
    return record;
  }
  throw new Error(`No records in ${url.pathname}`);
}

async function serveOnce(
  protocol: Protocol,
  accept: () => Promise<BinaryDuplexLike>,
) {
  const transport = await accept();
  const listener = protocol.createListener(transport) as StatelessListener;
  await listener.waitForClose();
}

describe("in-memory transport end-to-end", () => {
  beforeAll(async () => {
    await loadProtocolDefinition();
  });

  it("handles the add/onePlusOne scenario", async () => {
    const definition = protocolDefinition;
    const clientProtocol = Protocol.create(definition);
    const serverProtocol = Protocol.create(definition);

    const addRequest = await readInteropDatum(
      "../../../../../test-data/interop/rpc/add/onePlusOne/request.avro",
    ) as { arg1: number; arg2: number };
    const expectedResponse = await readInteropDatum(
      "../../../../../test-data/interop/rpc/add/onePlusOne/response.avro",
    ) as number;

    serverProtocol.on("add", (request) => {
      const handlerRequest = request as { arg1: number; arg2: number };
      assertEquals(handlerRequest, addRequest);
      return handlerRequest.arg1 + handlerRequest.arg2;
    });

    const transport = createInMemoryTransportPair();
    const emitter = clientProtocol.createEmitter(transport.clientFactory);
    const serverPromise = serveOnce(serverProtocol, transport.accept);

    const result = await clientProtocol.emit("add", addRequest, emitter);
    await serverPromise;
    emitter.destroy();

    assertEquals(result, expectedResponse);
  });

  it("handles the hello/world scenario", async () => {
    const definition = protocolDefinition;
    const clientProtocol = Protocol.create(definition);
    const serverProtocol = Protocol.create(definition);

    const helloRequest = await readInteropDatum(
      "../../../../../test-data/interop/rpc/hello/world/request.avro",
    ) as { greeting: string };
    const expectedResponse = await readInteropDatum(
      "../../../../../test-data/interop/rpc/hello/world/response.avro",
    ) as string;

    serverProtocol.on("hello", (request) => {
      const handlerRequest = request as { greeting: string };
      assertEquals(handlerRequest, helloRequest);
      return expectedResponse;
    });

    const transport = createInMemoryTransportPair();
    const emitter = clientProtocol.createEmitter(transport.clientFactory);
    const serverPromise = serveOnce(serverProtocol, transport.accept);

    const result = await clientProtocol.emit("hello", helloRequest, emitter);
    await serverPromise;
    emitter.destroy();

    assertEquals(result, expectedResponse);
  });

  it("handles the echo/foo scenario", async () => {
    const definition = protocolDefinition;
    const clientProtocol = Protocol.create(definition);
    const serverProtocol = Protocol.create(definition);

    const echoRequest = await readInteropDatum(
      "../../../../../test-data/interop/rpc/echo/foo/request.avro",
    ) as {
      record: { name: string; kind: "FOO" | "BAR" | "BAZ"; hash: Uint8Array };
    };
    const expectedResponse = await readInteropDatum(
      "../../../../../test-data/interop/rpc/echo/foo/response.avro",
    ) as { name: string; kind: "FOO" | "BAR" | "BAZ"; hash: Uint8Array };

    serverProtocol.on("echo", (request) => {
      const handlerRequest = request as { record: typeof expectedResponse };
      assertEquals(handlerRequest, echoRequest);
      return handlerRequest.record;
    });

    const transport = createInMemoryTransportPair();
    const emitter = clientProtocol.createEmitter(transport.clientFactory);
    const serverPromise = serveOnce(serverProtocol, transport.accept);

    const result = await clientProtocol.emit("echo", echoRequest, emitter);
    await serverPromise;
    emitter.destroy();

    assertEquals(result, expectedResponse);
  });
});
