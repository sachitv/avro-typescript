import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { Message } from "./message_definition.ts";

describe("Message definition", () => {
  it("exposes JSON with documentation and one-way flag", () => {
    const opts = {
      namespace: "org.example",
      registry: new Map(),
    };
    const message = new Message("notify", {
      request: [],
      response: "null",
      doc: "test event",
      "one-way": true,
    }, opts);

    const json = message.toJSON();
    assertEquals(json.doc, "test event");
    assertEquals(json["one-way"], true);
    assertEquals(json.response, "null");
  });

  it("throws when one-way message declares a response other than null", () => {
    const opts = {
      namespace: "org.example",
      registry: new Map(),
    };
    assertThrows(() => {
      new Message("bad", {
        request: [],
        response: "int",
        "one-way": true,
      }, opts);
    });
  });

  it("throws when one-way message declares custom errors", () => {
    const opts = {
      namespace: "org.example",
      registry: new Map(),
    };
    assertThrows(() => {
      new Message("bad", {
        request: [],
        response: "null",
        errors: ["int"],
        "one-way": true,
      }, opts);
    });
  });

  it("throws when response is missing", () => {
    const opts = {
      namespace: "org.example",
      registry: new Map(),
    };
    assertThrows(() => {
      new Message("bad", {
        request: [],
        // deno-lint-ignore no-explicit-any
      } as any, opts);
    });
  });

  it("serializes custom errors in toJSON", () => {
    const opts = {
      namespace: "org.example",
      registry: new Map(),
    };
    const message = new Message("test", {
      request: [],
      response: "int",
      errors: ["int", "long"],
    }, opts);

    const json = message.toJSON();
    assertEquals(json.errors, ["int", "long"]);
  });

  it("exposes JSON without optional fields", () => {
    const opts = {
      namespace: "org.example",
      registry: new Map(),
    };
    const message = new Message("basic", {
      request: [],
      response: "string",
    }, opts);

    const json = message.toJSON();
    assertEquals(json.request, []);
    assertEquals(json.response, "string");
    assertEquals(json.doc, undefined);
    assertEquals(json["one-way"], undefined);
    assertEquals(json.errors, undefined);
  });
});
