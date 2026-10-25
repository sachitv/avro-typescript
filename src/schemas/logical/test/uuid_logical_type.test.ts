import {
  assert,
  assertEquals,
  assertInstanceOf,
  assertMatch,
  assertRejects,
  assertThrows,
} from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { createType } from "../../../type/create_type.ts";
import { SchemaJSONScope } from "../../schema_json_scope.ts";
import { UuidLogicalType } from "../uuid_logical_type.ts";
import { StringType } from "../../primitive/string_type.ts";
import { FixedType } from "../../complex/fixed_type.ts";
import { resolveNames } from "../../complex/resolve_names.ts";
import { ValidationError } from "../../error.ts";

function createFixed(name: string, size: number): FixedType {
  const names = resolveNames({ name });
  return new FixedType({ ...names, size });
}

describe("UuidLogicalType", () => {
  const canonical = "123e4567-e89b-12d3-a456-426655440000";

  it("validates uuid strings", () => {
    const type = new UuidLogicalType(new StringType());
    assert(type.isValid(canonical));
    assert(!type.isValid("not-a-uuid"));
  });

  it("round-trips using string underlying type", async () => {
    const type = new UuidLogicalType(new StringType());
    const buffer = await type.toBuffer(canonical);
    const decoded = await type.fromBuffer(buffer);
    assertEquals(decoded, canonical);
  });

  it("round-trips using fixed underlying type", async () => {
    const fixed = createFixed("UuidFixed", 16);
    const type = new UuidLogicalType(fixed);
    const buffer = await type.toBuffer(canonical);
    const decoded = await type.fromBuffer(buffer);
    assertEquals(decoded, canonical);
  });

  it("throws when fixed size is not 16", () => {
    const fixed = createFixed("BadUuid", 8);
    assertThrows(() => new UuidLogicalType(fixed));
  });

  it("throws on invalid value", async () => {
    const type = new UuidLogicalType(new StringType());
    await assertRejects(
      async () => {
        await type.toBuffer("invalid");
      },
      ValidationError,
    );
  });

  it("produces uuid json schema for string", () => {
    const type = new UuidLogicalType(new StringType());
    assertEquals(type.toJSON(), { type: "string", logicalType: "uuid" });
  });

  it("produces uuid json schema for fixed", () => {
    const fixed = createFixed("UuidFixed", 16);
    const type = new UuidLogicalType(fixed);
    assertEquals(type.toJSON(), {
      name: fixed.getFullName(),
      type: "fixed",
      size: 16,
      logicalType: "uuid",
    });
  });

  describe("random value generation", () => {
    it("generates valid random uuids", () => {
      const type = new UuidLogicalType(new StringType());
      const value = type.random();
      assertMatch(
        value,
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      );
    });

    it("generates valid random uuids with fallback", () => {
      const type = new UuidLogicalType(new StringType());

      // Temporarily mock crypto to test fallback path
      const originalCrypto = globalThis.crypto;
      const descriptor = Object.getOwnPropertyDescriptor(globalThis, "crypto");
      try {
        // Remove crypto property to test fallback
        // deno-lint-ignore no-explicit-any
        delete (globalThis as any).crypto;

        const value = type.random();

        // Verify it's a valid UUID v4 format (fallback generates v4)
        assertMatch(
          value,
          /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
        );

        // Verify multiple calls generate different UUIDs
        const value2 = type.random();
        assert(value !== value2);
      } finally {
        // Restore crypto
        if (descriptor) {
          Object.defineProperty(globalThis, "crypto", descriptor);
        } else {
          // deno-lint-ignore no-explicit-any
          (globalThis as any).crypto = originalCrypto;
        }
      }
    });
  });

  it("throws on invalid bytes length in fromUnderlying", () => {
    const type = new UuidLogicalType(new StringType());
    assertThrows(
      // deno-lint-ignore no-explicit-any
      () => (type as any).fromUnderlying(new Uint8Array(15)),
      Error,
      "UUID bytes must be 16 bytes long.",
    );
  });

  it("can read from compatible logical types", () => {
    const type = new UuidLogicalType(new StringType());
    const otherUuidType = new UuidLogicalType(new StringType());

    // Test canReadFromLogical through type compatibility
    // deno-lint-ignore no-explicit-any
    assert((type as any).canReadFromLogical(otherUuidType));

    // Test with incompatible type - create a mock logical type
    const mockLogicalType = {
      constructor: { name: "MockLogicalType" },
    };
    // deno-lint-ignore no-explicit-any
    assert(!(type as any).canReadFromLogical(mockLogicalType));
  });

  it("throws named type methods when backed by string", () => {
    const type = new UuidLogicalType(new StringType());

    assertThrows(
      () => type.getFullName(),
      Error,
      "UUID logical type backed by string has no name.",
    );

    assertThrows(
      () => type.getNamespace(),
      Error,
      "UUID logical type backed by string has no namespace.",
    );

    assertThrows(
      () => type.getAliases(),
      Error,
      "UUID logical type backed by string has no aliases.",
    );
  });

  it("returns named type methods when backed by fixed", () => {
    const fixed = createFixed("test.uuid", 16);
    const type = new UuidLogicalType(fixed);

    // Check that the methods don't throw and return expected types
    const fullName = type.getFullName();
    const namespace = type.getNamespace();
    const aliases = type.getAliases();

    assertEquals(typeof fullName, "string");
    assertEquals(typeof namespace, "string");
    assertEquals(Array.isArray(aliases), true);
    assertEquals(aliases.length, 0);
  });
});

describe("UuidLogicalType.schemaJSON", () => {
  const overFixed = {
    type: "fixed",
    name: "N",
    size: 16,
    logicalType: "uuid",
  } as const;

  it("passes the scope to the underlying fixed", () => {
    const type = createType(overFixed);
    assertInstanceOf(type, UuidLogicalType);
    const scope = new SchemaJSONScope();

    assertEquals(type.schemaJSON(scope), {
      name: "N",
      type: "fixed",
      size: 16,
      logicalType: "uuid",
    });
    assertEquals([...scope.defined.keys()], ["N"]);
  });

  it("writes the fixed's name once the scope defines it", () => {
    const type = createType(overFixed);
    const scope = new SchemaJSONScope();
    type.schemaJSON(scope);

    assertEquals(type.schemaJSON(scope), "N");
  });

  it("annotates an underlying string whatever the scope", () => {
    const type = createType({ type: "string", logicalType: "uuid" });
    assertEquals(type.schemaJSON(new SchemaJSONScope()), {
      type: "string",
      logicalType: "uuid",
    });
  });

  it("starts a new scope on each toJSON call", () => {
    const type = createType(overFixed);
    assertEquals(type.toJSON(), {
      name: "N",
      type: "fixed",
      size: 16,
      logicalType: "uuid",
    });
    assertEquals(type.toJSON(), {
      name: "N",
      type: "fixed",
      size: 16,
      logicalType: "uuid",
    });
  });
});

describe("UuidLogicalType defaults", () => {
  const uuid = "00ff0000-0000-4000-8000-000000000080";

  it("writes and reads a default over string as the UUID string", () => {
    const type = createType({ type: "string", logicalType: "uuid" });
    assertEquals(type.defaultToJSON(uuid), uuid);
    assertEquals(type.defaultFromJSON(uuid), uuid);
  });

  it("writes and reads a default over fixed as its 16 bytes", () => {
    const type = createType({
      type: "fixed",
      name: "Id",
      size: 16,
      logicalType: "uuid",
    });
    const json = "\u0000\u00ff" + "\u0000".repeat(4) + "\u0040\u0000" +
      "\u0080" + "\u0000".repeat(6) + "\u0080";
    assertEquals(type.defaultToJSON(uuid), json);
    assertEquals(type.defaultFromJSON(json), uuid);
    assertEquals(type.defaultFromJSON(uuid), uuid);
  });
});
