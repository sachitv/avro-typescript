import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { isValidName, resolveNames } from "../resolve_names.ts";

describe("resolveNames", () => {
  it("qualifies a simple name using the provided namespace", () => {
    const result = resolveNames({
      name: "Person",
      namespace: "com.example",
    });

    assertEquals(result.fullName, "com.example.Person");
    assertEquals(result.namespace, "com.example");
    assertEquals(result.aliases, []);
  });

  it("returns the same fully qualified name when namespace is absent", () => {
    const result = resolveNames({ name: "com.example.Person" });
    assertEquals(result.fullName, "com.example.Person");
    assertEquals(result.namespace, "com.example");
  });

  it("treats empty namespace as unspecified", () => {
    const result = resolveNames({ name: "Person", namespace: "" });
    assertEquals(result.fullName, "Person");
    assertEquals(result.namespace, "");
  });

  it("qualifies aliases within the effective namespace", () => {
    const result = resolveNames({
      name: "Person",
      namespace: "com.example",
      aliases: ["LegacyPerson", "other.Alias"],
    });

    assertEquals(result.aliases, [
      "com.example.LegacyPerson",
      "other.Alias",
    ]);
  });

  it("inherits namespace for aliases when the name is already qualified", () => {
    const result = resolveNames({
      name: "com.example.Person",
      aliases: ["LegacyPerson"],
    });

    assertEquals(result.aliases, ["com.example.LegacyPerson"]);
  });

  it("throws when the name is invalid", () => {
    assertThrows(
      () => resolveNames({ name: "1invalid" }),
      Error,
      "Invalid Avro name: 1invalid",
    );
  });

  it("throws when an alias is invalid", () => {
    assertThrows(
      () =>
        resolveNames({
          name: "ValidName",
          namespace: "com.example",
          aliases: ["also.valid", "1invalid"],
        }),
      Error,
      "Invalid Avro alias: com.example.1invalid",
    );
  });

  it("deduplicates aliases when they resolve to the same name", () => {
    const result = resolveNames({
      name: "com.example.Person",
      aliases: ["Alias", "Alias", "com.example.Person"],
    });
    assertEquals(result.aliases, ["com.example.Alias"]);
  });

  it("throws when name is missing", () => {
    assertThrows(
      () =>
        resolveNames({
          // deno-lint-ignore no-explicit-any
          name: "" as any,
        }),
      Error,
      "Avro name is required.",
    );
  });

  it("throws when alias name is missing", () => {
    assertThrows(
      () =>
        resolveNames({
          name: "ValidName",
          // deno-lint-ignore no-explicit-any
          aliases: ["" as any],
        }),
      Error,
      "Avro alias is required.",
    );
  });

  it("throws when attempting to use a primitive name", () => {
    assertThrows(
      () => resolveNames({ name: "int" }),
      Error,
      "Cannot rename primitive Avro type: int",
    );
  });

  it("throws when an alias attempts to use a primitive name", () => {
    assertThrows(
      () =>
        resolveNames({
          name: "ValidName",
          aliases: ["int"],
        }),
      Error,
      "Cannot rename primitive Avro alias: int",
    );
  });
});

describe("isValidName", () => {
  it("returns true for valid names", () => {
    assertEquals(isValidName("ValidName"), true);
    assertEquals(isValidName("_underscore"), true);
    assertEquals(isValidName("name123"), true);
    assertEquals(isValidName("A"), true);
  });

  it("returns false for invalid names", () => {
    assertEquals(isValidName(""), false);
    assertEquals(isValidName("123invalid"), false);
    assertEquals(isValidName("invalid-name"), false);
    assertEquals(isValidName("invalid.name"), false);
    assertEquals(isValidName("invalid name"), false);
  });
});
