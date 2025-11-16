import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import type {
  ProtocolDefinition,
  ProtocolFactory,
} from "./protocol_definitions.ts";
import { Protocol } from "../mod.ts";

describe("Protocol definitions typings", () => {
  it("supports ProtocolFactory", () => {
    const factory: ProtocolFactory = (definition) =>
      Protocol.create(definition);
    const definition: ProtocolDefinition = { protocol: "Ping" };
    const protocol = factory(definition);
    assertEquals(protocol.getName(), "Ping");
  });
});
