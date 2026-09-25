import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { createType } from "../../type/create_type.ts";
import { FixedType } from "../complex/fixed_type.ts";
import { resolveNames } from "../complex/resolve_names.ts";
import {
  namedTypeJSON,
  sameSchema,
  SchemaJSONScope,
  TypePairs,
} from "../schema_json_scope.ts";

/** Parses `schema`, writes it back with `toJSON`, and parses that again. */
function roundTrip(schema: unknown): { json: unknown; reparsed: unknown } {
  const json = JSON.parse(JSON.stringify(createType(schema as never)));
  // toJSON on the reparsed type builds every field, so a duplicate or
  // dangling name would throw here.
  const reparsed = JSON.parse(JSON.stringify(createType(json)));
  return { json, reparsed };
}

describe("namedTypeJSON", () => {
  const fixed = (name: string, namespace?: string) =>
    new FixedType({ ...resolveNames({ name, namespace }), size: 1 });

  it("defines a name the first time and refers to it after", () => {
    const scope = new SchemaJSONScope();
    const type = fixed("F");

    const first = namedTypeJSON(type, "fixed", scope, () => ({ size: 1 }));
    const second = namedTypeJSON(type, "fixed", scope, () => ({ size: 1 }));

    assertEquals(first, { name: "F", type: "fixed", size: 1 });
    assertEquals(second, "F");
    assertEquals([...scope.defined.keys()], ["F"]);
  });

  it("records the name before building the definition", () => {
    const scope = new SchemaJSONScope();
    const type = fixed("F", "a");
    let inner: unknown;

    namedTypeJSON(type, "fixed", scope, () => {
      inner = namedTypeJSON(type, "fixed", scope, () => ({}));
      return {};
    });

    assertEquals(inner, "a.F");
  });

  it("writes an empty namespace only under a namespaced scope", () => {
    const scope = new SchemaJSONScope();
    scope.namespace = "a.b";

    assertEquals(
      namedTypeJSON(fixed("F"), "fixed", scope, () => ({ size: 1 })),
      { name: "F", namespace: "", type: "fixed", size: 1 },
    );
    assertEquals(
      namedTypeJSON(fixed("G"), "fixed", new SchemaJSONScope(), () => ({})),
      { name: "G", type: "fixed" },
    );
  });

  it("refuses a different type with the same name", () => {
    const scope = new SchemaJSONScope();
    namedTypeJSON(fixed("F"), "fixed", scope, () => ({ size: 1 }));
    const other = new FixedType({ ...resolveNames({ name: "F" }), size: 2 });

    assertThrows(
      () => namedTypeJSON(other, "fixed", scope, () => ({ size: 2 })),
      Error,
      "Duplicate Avro type name: F",
    );
  });

  it("lets an identical type with the same name refer to it", () => {
    const scope = new SchemaJSONScope();
    namedTypeJSON(fixed("F"), "fixed", scope, () => ({ size: 1 }));

    assertEquals(
      namedTypeJSON(fixed("F"), "fixed", scope, () => ({ size: 1 })),
      "F",
    );
  });

  it("refuses to refer to a null-namespace type from inside a namespace", () => {
    const scope = new SchemaJSONScope();
    const type = fixed("F");
    namedTypeJSON(type, "fixed", scope, () => ({ size: 1 }));
    scope.namespace = "a";

    assertThrows(
      () => namedTypeJSON(type, "fixed", scope, () => ({ size: 1 })),
      Error,
      "Cannot refer to F in the null namespace from namespace a",
    );
  });

  it("refuses a reference this library parses but the specification cannot express", () => {
    // `{ type: "E", namespace: "" }` refers to the null-namespace E from inside
    // namespace a in this library's parser, but it is not portable Avro, so
    // it is not written.
    const type = createType({
      type: "record",
      name: "R",
      fields: [
        { name: "e", type: { type: "enum", name: "E", symbols: ["X"] } },
        {
          name: "inner",
          type: {
            type: "record",
            name: "a.Inner",
            fields: [{ name: "y", type: { type: "E", namespace: "" } }],
          },
        },
      ],
    });

    assertThrows(
      () => type.toJSON(),
      Error,
      "Cannot refer to E in the null namespace from namespace a: the Avro specification has no portable syntax",
    );
  });

  it("refuses a plain reference to a name defined with a logical type", () => {
    const scope = new SchemaJSONScope();
    const type = fixed("F");
    namedTypeJSON(type, "fixed", scope, () => ({ size: 1 }));
    scope.defined.get("F")!.logical = createType({
      type: "fixed",
      name: "F",
      size: 1,
      logicalType: "decimal",
      precision: 2,
    });

    assertThrows(
      () => namedTypeJSON(type, "fixed", scope, () => ({ size: 1 })),
      Error,
      "Cannot refer to F without its logical type",
    );
  });

  it("writes aliases as full names after the type", () => {
    const type = new FixedType({
      ...resolveNames({
        name: "F",
        namespace: "a",
        aliases: ["Old", "b.Older"],
      }),
      size: 1,
    });

    assertEquals(
      namedTypeJSON(type, "fixed", new SchemaJSONScope(), () => ({ size: 1 })),
      {
        name: "a.F",
        type: "fixed",
        aliases: ["a.Old", "b.Older"],
        size: 1,
      },
    );
  });

  it("refuses a null-namespace alias on a namespaced type", () => {
    // Only reachable for types built in code: the parser qualifies bare
    // aliases with the type's namespace.
    const type = new FixedType({
      fullName: "a.F",
      namespace: "a",
      aliases: ["Old"],
      size: 1,
    });

    assertThrows(
      () => namedTypeJSON(type, "fixed", new SchemaJSONScope(), () => ({})),
      Error,
      "Cannot write alias Old of a.F: it is in the null namespace",
    );
  });

  it("writes a bare alias on a null-namespace type", () => {
    const type = new FixedType({
      ...resolveNames({ name: "F", aliases: ["Old"] }),
      size: 1,
    });
    const scope = new SchemaJSONScope();
    scope.namespace = "outer";

    assertEquals(
      namedTypeJSON(type, "fixed", scope, () => ({ size: 1 })),
      {
        name: "F",
        namespace: "",
        type: "fixed",
        aliases: ["Old"],
        size: 1,
      },
    );
  });

  it("treats types that differ only in aliases as different types", () => {
    const scope = new SchemaJSONScope();
    namedTypeJSON(fixed("F"), "fixed", scope, () => ({ size: 1 }));
    const aliased = new FixedType({
      ...resolveNames({ name: "F", aliases: ["Old"] }),
      size: 1,
    });

    assertThrows(
      () => namedTypeJSON(aliased, "fixed", scope, () => ({ size: 1 })),
      Error,
      "Duplicate Avro type name: F",
    );
  });

  it("does not share state between scopes", () => {
    const type = fixed("F");
    namedTypeJSON(type, "fixed", new SchemaJSONScope(), () => ({}));

    assertEquals(
      namedTypeJSON(type, "fixed", new SchemaJSONScope(), () => ({})),
      { name: "F", type: "fixed" },
    );
  });
});

describe("Type.schemaJSON", () => {
  it("defaults to toJSON for types without children", () => {
    const type = createType("long");
    assertEquals(type.schemaJSON(new SchemaJSONScope()), "long");
  });

  it("defines a named type again in a new top-level toJSON", () => {
    const type = createType({ type: "fixed", name: "F", size: 1 });
    assertEquals(type.toJSON(), { name: "F", type: "fixed", size: 1 });
    assertEquals(type.toJSON(), { name: "F", type: "fixed", size: 1 });
  });
});

describe("named types in toJSON", () => {
  it("writes a reused enum once, then by name", () => {
    const { json, reparsed } = roundTrip({
      type: "record",
      name: "R",
      fields: [
        { name: "a", type: { type: "enum", name: "E", symbols: ["X"] } },
        { name: "b", type: "E" },
      ],
    });
    assertEquals(json, {
      name: "R",
      type: "record",
      fields: [
        { name: "a", type: { name: "E", type: "enum", symbols: ["X"] } },
        { name: "b", type: "E" },
      ],
    });
    assertEquals(reparsed, json);
  });

  it("writes a reused fixed once, then by name", () => {
    const { json, reparsed } = roundTrip({
      type: "record",
      name: "R",
      fields: [
        { name: "a", type: { type: "fixed", name: "F", size: 2 } },
        { name: "b", type: { type: "map", values: "F" } },
      ],
    });
    assertEquals(json, {
      name: "R",
      type: "record",
      fields: [
        { name: "a", type: { name: "F", type: "fixed", size: 2 } },
        { name: "b", type: { type: "map", values: "F" } },
      ],
    });
    assertEquals(reparsed, json);
  });

  it("writes a reused record once, then by name", () => {
    const { json, reparsed } = roundTrip({
      type: "record",
      name: "R",
      fields: [
        {
          name: "a",
          type: {
            type: "record",
            name: "P",
            fields: [{ name: "x", type: "int" }],
          },
        },
        { name: "b", type: { type: "array", items: "P" } },
      ],
    });
    assertEquals(
      (json as { fields: { type: unknown }[] }).fields[1].type,
      { type: "array", items: "P" },
    );
    assertEquals(reparsed, json);
  });

  it("writes a recursive record by referring back to itself", () => {
    const { json, reparsed } = roundTrip({
      type: "record",
      name: "Node",
      fields: [
        { name: "value", type: "int" },
        { name: "next", type: ["null", "Node"] },
      ],
    });
    assertEquals(json, {
      name: "Node",
      type: "record",
      fields: [
        { name: "value", type: "int" },
        { name: "next", type: ["null", "Node"] },
      ],
    });
    assertEquals(reparsed, json);
  });

  it("writes mutually recursive records", () => {
    const { json, reparsed } = roundTrip({
      type: "record",
      name: "A",
      fields: [{
        name: "b",
        type: ["null", {
          type: "record",
          name: "B",
          fields: [{ name: "a", type: ["null", "A"] }],
        }],
      }],
    });
    assertEquals(json, {
      name: "A",
      type: "record",
      fields: [{
        name: "b",
        type: ["null", {
          name: "B",
          type: "record",
          fields: [{ name: "a", type: ["null", "A"] }],
        }],
      }],
    });
    assertEquals(reparsed, json);
  });

  it("gives the same JSON on every call", () => {
    const type = createType({
      type: "record",
      name: "Node",
      fields: [{ name: "next", type: ["null", "Node"] }],
    });
    assertEquals(JSON.stringify(type), JSON.stringify(type));
  });

  it("keeps an empty namespace under a namespaced record", () => {
    const { json, reparsed } = roundTrip({
      type: "record",
      name: "R",
      namespace: "a.b",
      fields: [{
        name: "e",
        type: { type: "enum", name: "E", namespace: "", symbols: ["X"] },
      }],
    });
    assertEquals(
      (json as { fields: { type: unknown }[] }).fields[0].type,
      { name: "E", namespace: "", type: "enum", symbols: ["X"] },
    );
    assertEquals(reparsed, json);
  });

  it("does not write an empty namespace where it is already empty", () => {
    const { json } = roundTrip({
      type: "record",
      name: "R",
      fields: [{
        name: "e",
        type: { type: "enum", name: "E", namespace: "", symbols: ["X"] },
      }],
    });
    assertEquals(
      (json as { fields: { type: unknown }[] }).fields[0].type,
      { name: "E", type: "enum", symbols: ["X"] },
    );
  });

  it("restores the enclosing namespace after a nested record", () => {
    const { json, reparsed } = roundTrip({
      type: "record",
      name: "Outer",
      fields: [
        {
          name: "inner",
          type: {
            type: "record",
            name: "Inner",
            namespace: "x.y",
            fields: [{ name: "v", type: "int" }],
          },
        },
        {
          name: "e",
          type: { type: "enum", name: "E", namespace: "", symbols: ["X"] },
        },
      ],
    });
    // Back in Outer's null namespace, E needs no explicit namespace.
    assertEquals(
      (json as { fields: { type: unknown }[] }).fields[1].type,
      { name: "E", type: "enum", symbols: ["X"] },
    );
    assertEquals(reparsed, json);
  });

  it("refers to a reused type from another namespace by full name", () => {
    const { json, reparsed } = roundTrip({
      type: "record",
      name: "R",
      namespace: "a.b",
      fields: [
        {
          name: "f",
          type: { type: "fixed", name: "F", namespace: "c", size: 1 },
        },
        { name: "g", type: "c.F" },
      ],
    });
    assertEquals(
      (json as { fields: { type: unknown }[] }).fields[1].type,
      "c.F",
    );
    assertEquals(reparsed, json);
  });

  it("writes a reused decimal fixed once, then by name", () => {
    const { json, reparsed } = roundTrip({
      type: "record",
      name: "R",
      fields: [
        {
          name: "a",
          type: {
            type: "fixed",
            name: "Money",
            size: 8,
            logicalType: "decimal",
            precision: 10,
            scale: 2,
          },
        },
        { name: "b", type: "Money" },
      ],
    });
    assertEquals(json, {
      name: "R",
      type: "record",
      fields: [
        {
          name: "a",
          type: {
            name: "Money",
            type: "fixed",
            size: 8,
            logicalType: "decimal",
            precision: 10,
            scale: 2,
          },
        },
        { name: "b", type: "Money" },
      ],
    });
    assertEquals(reparsed, json);
  });
});

describe("TypePairs", () => {
  const a = createType("int");
  const b = createType("long");

  it("holds ordered pairs", () => {
    const pairs = new TypePairs();
    pairs.add(a, b);

    assertEquals(pairs.has(a, b), true);
    assertEquals(pairs.has(b, a), false);
    assertEquals(pairs.size, 1);
  });

  it("drops a type once its last pair is deleted", () => {
    const pairs = new TypePairs();
    pairs.add(a, b);
    pairs.add(a, a);

    pairs.delete(a, b);
    assertEquals(pairs.size, 1);
    pairs.delete(a, a);
    assertEquals(pairs.size, 0);
    // Deleting a pair that is not there is a no-op.
    pairs.delete(b, a);
    assertEquals(pairs.size, 0);
  });
});

describe("sameSchema", () => {
  it("is true for the same instance without writing it", () => {
    const type = createType({ type: "fixed", name: "F", size: 1 });
    const scope = new SchemaJSONScope();
    assertEquals(sameSchema(type, type, scope), true);
    assertEquals(scope.comparison.equal.size, 0);
  });

  it("compares separately built types by their schemas", () => {
    const scope = new SchemaJSONScope();
    const a = createType({ type: "fixed", name: "F", size: 1 });
    const same = createType({ type: "fixed", name: "F", size: 1 });
    const other = createType({ type: "fixed", name: "F", size: 2 });

    assertEquals(sameSchema(a, same, scope), true);
    assertEquals(sameSchema(a, other, scope), false);
    // Pairs are only held while they are being compared; equal ones are kept.
    assertEquals(scope.comparison.comparing.size, 0);
    assertEquals(scope.comparison.equal.has(a, same), true);
    assertEquals(scope.comparison.equal.has(a, other), false);
  });

  it("takes a pair met again during its own comparison to be equal", () => {
    const scope = new SchemaJSONScope();
    const a = createType({ type: "fixed", name: "F", size: 1 });
    const b = createType({ type: "fixed", name: "F", size: 2 });
    scope.comparison.comparing.add(a, b);

    assertEquals(sameSchema(a, b, scope), true);
  });

  it("compares a pair once per write", () => {
    const scope = new SchemaJSONScope();
    const a = createType({ type: "fixed", name: "F", size: 1 });
    const b = createType({ type: "fixed", name: "F", size: 1 });
    let writes = 0;
    const write = b.schemaJSON.bind(b);
    b.schemaJSON = (inner) => {
      writes++;
      return write(inner);
    };

    sameSchema(a, b, scope);
    sameSchema(a, b, scope);

    assertEquals(writes, 1);
  });

  it("compares types with long defaults", () => {
    const schema = {
      type: "record",
      name: "L",
      fields: [{ name: "n", type: "long", default: 5 }],
    } as const;
    assertEquals(
      sameSchema(
        createType(schema),
        createType(schema),
        new SchemaJSONScope(),
      ),
      true,
    );
  });
});

describe("a type containing another instance with its own name", () => {
  it("refuses a different record instead of recursing", () => {
    const inner = createType({
      type: "record",
      name: "N",
      fields: [{ name: "v", type: "int" }],
    });
    const outer = createType({
      type: "record",
      name: "N",
      fields: [{ name: "inner", type: inner }],
    });

    assertThrows(() => outer.toJSON(), Error, "Duplicate Avro type name: N");
  });

  it("refers to an identical recursive record by name", () => {
    const schema = {
      type: "record",
      name: "Node",
      fields: [{ name: "next", type: ["null", "Node"] }],
    };
    const first = createType(schema);
    const second = createType(schema);
    const pair = createType({
      type: "record",
      name: "Pair",
      fields: [{ name: "a", type: first }, { name: "b", type: second }],
    });

    assertEquals(pair.toJSON(), {
      name: "Pair",
      type: "record",
      fields: [
        {
          name: "a",
          type: {
            name: "Node",
            type: "record",
            fields: [{ name: "next", type: ["null", "Node"] }],
          },
        },
        { name: "b", type: "Node" },
      ],
    });
  });

  // A contains B, and B refers back to A: comparing two copies of A reaches
  // the B pair, whose comparison reaches the A pair again.
  const mutual = (bField = "int") => ({
    type: "record",
    name: "A",
    fields: [{
      name: "b",
      type: ["null", {
        type: "record",
        name: "B",
        fields: [
          { name: "a", type: ["null", "A"] },
          { name: "v", type: bField },
        ],
      }],
    }],
  });

  it("refers to identical mutually recursive records by name", () => {
    const pair = createType({
      type: "record",
      name: "Pair",
      fields: [
        { name: "first", type: createType(mutual()) },
        { name: "second", type: createType(mutual()) },
      ],
    });

    assertEquals(pair.toJSON(), {
      name: "Pair",
      type: "record",
      fields: [
        {
          name: "first",
          type: {
            name: "A",
            type: "record",
            fields: [{
              name: "b",
              type: ["null", {
                name: "B",
                type: "record",
                fields: [
                  { name: "a", type: ["null", "A"] },
                  { name: "v", type: "int" },
                ],
              }],
            }],
          },
        },
        { name: "second", type: "A" },
      ],
    });
  });

  it("refuses mutually recursive records that differ in the inner one", () => {
    const pair = createType({
      type: "record",
      name: "Pair",
      fields: [
        { name: "first", type: createType(mutual("int")) },
        { name: "second", type: createType(mutual("string")) },
      ],
    });

    // The two Bs are only ever written inside their own A, so the difference
    // shows up when the As are compared, and the error names A.
    assertThrows(() => pair.toJSON(), Error, "Duplicate Avro type name: A");
  });

  it("compares separately built copies in a cycle a linear number of times", () => {
    // Each copy refers to the next two, around a ring. Without remembering
    // pairs found equal, each pair is compared again on every path that
    // reaches it, and the writes grow exponentially (about 290,000 for 8
    // copies); with it they grow linearly (99 for 8 copies).
    const count = 8;
    // Record fields are built lazily from the schema, so the copies can be
    // created first and their union branches pointed at each other after.
    const schemas = Array.from({ length: count }, () => ({
      type: "record",
      name: "N",
      fields: [
        { name: "a", type: ["null", "int"] as unknown[] },
        { name: "b", type: ["null", "int"] as unknown[] },
      ],
    }));
    const types = schemas.map((schema) => createType(schema));
    schemas.forEach((schema, i) => {
      schema.fields[0].type[1] = types[(i + 1) % count];
      schema.fields[1].type[1] = types[(i + 2) % count];
    });
    let writes = 0;
    for (const type of types) {
      const write = type.schemaJSON.bind(type);
      type.schemaJSON = (scope) => {
        writes++;
        return write(scope);
      };
    }

    assertEquals(types[0].toJSON(), {
      name: "N",
      type: "record",
      fields: [
        { name: "a", type: ["null", "N"] },
        { name: "b", type: ["null", "N"] },
      ],
    });
    assert(writes <= 16 * count, `${writes} writes for ${count} copies`);
  });
});
