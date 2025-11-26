import { assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { createType } from "../create_type.ts";
import { TestTap as Tap } from "../../serialization/test/test_tap.ts";

const WRITER_BRANCH_NAME = "example.compat.BranchReferencingFoo";

const WRITER_SCHEMA = {
  type: "record",
  name: "Envelope",
  namespace: "example.compat",
  fields: [
    {
      name: "unionField",
      type: [
        "null",
        {
          type: "record",
          name: "BranchReferencingFoo",
          fields: [
            { name: "foo", type: "Foo" },
            { name: "marker", type: "string" },
          ],
        },
        {
          type: "record",
          name: "BranchWithFooDefinition",
          fields: [
            {
              name: "foo",
              type: {
                type: "record",
                name: "Foo",
                fields: [
                  { name: "id", type: "string" },
                  { name: "count", type: "int" },
                ],
              },
            },
            { name: "note", type: "string" },
          ],
        },
      ],
      default: null,
    },
  ],
} as const;

const COMPATIBLE_READER_SCHEMA = {
  type: "record",
  name: "Envelope",
  namespace: "example.compat",
  fields: [
    {
      name: "unionField",
      type: [
        "null",
        {
          type: "record",
          name: "BranchReferencingFoo",
          fields: [
            { name: "foo", type: "Foo" },
            { name: "marker", type: "string" },
            { name: "tag", type: "string", default: "reader-default" },
          ],
        },
        {
          type: "record",
          name: "BranchWithFooDefinition",
          fields: [
            {
              name: "foo",
              type: {
                type: "record",
                name: "Foo",
                fields: [
                  { name: "id", type: "string" },
                  { name: "count", type: "int" },
                  { name: "source", type: "string", default: "legacy" },
                ],
              },
            },
            { name: "note", type: "string", default: "" },
          ],
        },
      ],
      default: null,
    },
  ],
} as const;

const SAMPLE_VALUE = {
  unionField: {
    [WRITER_BRANCH_NAME]: {
      foo: { id: "abc-123", count: 7 },
      marker: "writer-branch",
    },
  },
};

describe("createType nested named type materialization", () => {
  it("serializes and deserializes a branch that references a nested type defined in another branch", async () => {
    const type = createType(WRITER_SCHEMA);
    const buffer = await type.toBuffer(SAMPLE_VALUE);
    const decoded = await type.fromBuffer(buffer);
    assertEquals(decoded, SAMPLE_VALUE);
  });

  it("builds a resolver when writer and reader share the same schema", async () => {
    const writerType = createType(WRITER_SCHEMA);
    const readerType = createType(WRITER_SCHEMA);
    const buffer = new ArrayBuffer(256);
    const writeTap = new Tap(buffer);
    await writerType.write(writeTap, SAMPLE_VALUE);

    const resolver = readerType.createResolver(writerType);
    const readTap = new Tap(buffer);
    const resolved = await resolver.read(readTap);
    assertEquals(resolved, SAMPLE_VALUE);
  });

  it("reads writer data with a compatible reader schema that extends the nested record", async () => {
    const writerType = createType(WRITER_SCHEMA);
    const readerType = createType(COMPATIBLE_READER_SCHEMA);

    const resolver = readerType.createResolver(writerType);
    const buffer = new ArrayBuffer(256);
    const writeTap = new Tap(buffer);
    await writerType.write(writeTap, SAMPLE_VALUE);

    const readTap = new Tap(buffer);
    const resolved = await resolver.read(readTap);
    assertEquals(resolved, {
      unionField: {
        [WRITER_BRANCH_NAME]: {
          foo: { id: "abc-123", count: 7, source: "legacy" },
          marker: "writer-branch",
          tag: "reader-default",
        },
      },
    });
  });

  it("throws when a referenced named type is never defined", () => {
    const schemaMissingFoo = {
      type: "record",
      name: "Envelope",
      namespace: "example.compat",
      fields: [
        {
          name: "unionField",
          type: [
            "null",
            {
              type: "record",
              name: "BranchReferencingMissingFoo",
              fields: [
                { name: "foo", type: "Foo" },
              ],
            },
          ],
        },
      ],
    } as const;

    const type = createType(schemaMissingFoo);
    assertRejects(
      async () =>
        await type.toBuffer({
          unionField: {
            BranchReferencingMissingFoo: { foo: { id: "x" } },
          },
        }),
      Error,
    );
  });

  it("materializes deeply nested named types referenced across branches", async () => {
    const DEEP_NAMESPACE = "example.skywalker";
    const deepSchema = {
      type: "record",
      name: "SkywalkerFamily",
      namespace: DEEP_NAMESPACE,
      fields: [
        {
          name: "lineage",
          type: [
            "null",
            {
              type: "record",
              name: "AnakinBranch",
              fields: [
                {
                  name: "lukeSubtree",
                  type: {
                    type: "record",
                    name: "LukeSubtree",
                    fields: [
                      {
                        name: "message",
                        type: {
                          type: "record",
                          name: "KenobiMessage",
                          fields: [
                            { name: "leaf", type: "SkywalkerLeaf" },
                          ],
                        },
                      },
                    ],
                  },
                },
              ],
            },
            {
              type: "record",
              name: "LeiaBranch",
              fields: [
                {
                  name: "organaSubtree",
                  type: {
                    type: "record",
                    name: "OrganaSubtree",
                    fields: [
                      {
                        name: "leafWrapper",
                        type: {
                          type: "record",
                          name: "SkywalkerLeaf",
                          fields: [
                            { name: "id", type: "string" },
                            { name: "note", type: "string" },
                          ],
                        },
                      },
                    ],
                  },
                },
              ],
            },
          ],
          default: null,
        },
      ],
    } as const;

    const deepType = createType(deepSchema);
    const value = {
      lineage: {
        [`${DEEP_NAMESPACE}.AnakinBranch`]: {
          lukeSubtree: {
            message: { leaf: { id: "leaf-1", note: "a-new-hope" } },
          },
        },
      },
    };

    const buffer = await deepType.toBuffer(value);
    const decoded = await deepType.fromBuffer(buffer);
    assertEquals(decoded, value);
  });

  it("resolves named types defined in another namespace branch", async () => {
    const schema = {
      type: "record",
      name: "Envelope",
      namespace: "example.crossns",
      fields: [
        {
          name: "payload",
          type: [
            {
              type: "record",
              name: "RefBranch",
              fields: [
                { name: "foo", type: "otherpkg.Foo" },
                { name: "label", type: "string" },
              ],
            },
            {
              type: "record",
              name: "DefBranch",
              namespace: "otherpkg",
              fields: [
                {
                  name: "foo",
                  type: {
                    type: "record",
                    name: "Foo",
                    namespace: "otherpkg",
                    fields: [
                      { name: "id", type: "string" },
                    ],
                  },
                },
                { name: "label", type: "string" },
              ],
            },
          ],
        },
      ],
    } as const;

    const type = createType(schema); // Writer uses original layout.
    const value = {
      payload: {
        "example.crossns.RefBranch": {
          foo: { id: "ns-1" },
          label: "ref",
        },
      },
    };

    const buffer = await type.toBuffer(value); // Writer can serialize.
    const decoded = await type.fromBuffer(buffer); // Writer can read back.
    assertEquals(decoded, value);

    // Compatible reader adds Foo.note defaulted to "".
    const readerSchema = {
      type: "record",
      name: "Envelope",
      namespace: "example.crossns",
      fields: [
        {
          name: "payload",
          type: [
            {
              type: "record",
              name: "RefBranch",
              fields: [
                { name: "foo", type: "otherpkg.Foo" },
                { name: "label", type: "string" },
              ],
            },
            {
              type: "record",
              name: "DefBranch",
              namespace: "otherpkg",
              fields: [
                {
                  name: "foo",
                  type: {
                    type: "record",
                    name: "Foo",
                    namespace: "otherpkg",
                    fields: [
                      { name: "id", type: "string" },
                      { name: "note", type: "string", default: "" },
                    ],
                  },
                },
                { name: "label", type: "string" },
              ],
            },
          ],
        },
      ],
    } as const;

    const readerType = createType(readerSchema);
    const resolver = readerType.createResolver(type);
    const tap = new Tap(new ArrayBuffer(128));
    await type.write(tap, value);
    tap._testOnlyResetPos();
    const resolved = await resolver.read(tap);
    assertEquals(resolved, {
      payload: {
        "example.crossns.RefBranch": {
          foo: { id: "ns-1", note: "" },
          label: "ref",
        },
      },
    });
  });

  it("reuses forward-referenced named types across multiple branches", async () => {
    const schema = {
      type: "record",
      name: "Envelope",
      namespace: "example.multiple",
      fields: [
        {
          name: "unionField",
          type: [
            {
              type: "record",
              name: "AlphaBranch",
              fields: [{ name: "foo", type: "Foo" }],
            },
            {
              type: "record",
              name: "BetaBranch",
              fields: [{ name: "foo", type: "Foo" }],
            },
            {
              type: "record",
              name: "DefinitionBranch",
              fields: [
                {
                  name: "foo",
                  type: {
                    type: "record",
                    name: "Foo",
                    fields: [{ name: "id", type: "string" }],
                  },
                },
              ],
            },
          ],
        },
      ],
    } as const;

    const type = createType(schema); // Writer with shared forward ref.
    const value = {
      unionField: {
        "example.multiple.BetaBranch": { foo: { id: "beta" } },
      },
    };

    const buffer = await type.toBuffer(value); // Writer round-trips.
    const decoded = await type.fromBuffer(buffer);
    assertEquals(decoded, value);

    // Compatible reader evolves Foo.extra defaulted to "reader".
    const readerSchema = {
      type: "record",
      name: "Envelope",
      namespace: "example.multiple",
      fields: [
        {
          name: "unionField",
          type: [
            {
              type: "record",
              name: "AlphaBranch",
              fields: [{ name: "foo", type: "Foo" }],
            },
            {
              type: "record",
              name: "BetaBranch",
              fields: [{ name: "foo", type: "Foo" }],
            },
            {
              type: "record",
              name: "DefinitionBranch",
              fields: [
                {
                  name: "foo",
                  type: {
                    type: "record",
                    name: "Foo",
                    fields: [
                      { name: "id", type: "string" },
                      { name: "extra", type: "string", default: "reader" },
                    ],
                  },
                },
              ],
            },
          ],
        },
      ],
    } as const;

    const readerType = createType(readerSchema);
    const resolver = readerType.createResolver(type);
    const tap = new Tap(new ArrayBuffer(128));
    await type.write(tap, value);
    tap._testOnlyResetPos();
    const resolved = await resolver.read(tap);
    assertEquals(resolved, {
      unionField: {
        "example.multiple.BetaBranch": { foo: { id: "beta", extra: "reader" } },
      },
    });

    // Compatible reader migrates Foo.id from string to bytes.
    const bytesReader = createType({
      type: "record",
      name: "Envelope",
      namespace: "example.multiple",
      fields: [
        {
          name: "unionField",
          type: [
            {
              type: "record",
              name: "AlphaBranch",
              fields: [{ name: "foo", type: "Foo" }],
            },
            {
              type: "record",
              name: "BetaBranch",
              fields: [{ name: "foo", type: "Foo" }],
            },
            {
              type: "record",
              name: "DefinitionBranch",
              fields: [
                {
                  name: "foo",
                  type: {
                    type: "record",
                    name: "Foo",
                    fields: [{ name: "id", type: "bytes" }],
                  },
                },
              ],
            },
          ],
        },
      ],
    } as const);
    const bytesResolver = bytesReader.createResolver(type);
    const bytesTap = new Tap(new ArrayBuffer(128));
    await type.write(bytesTap, value);
    bytesTap._testOnlyResetPos();
    const bytesResolved = await bytesResolver.read(bytesTap);
    const bytesId =
      (bytesResolved as any).unionField["example.multiple.BetaBranch"].foo.id;
    assertEquals(bytesId, new Uint8Array([98, 101, 116, 97]));
  });

  it("supports forward references to enums defined later in a union", async () => {
    const schema = {
      type: "record",
      name: "Envelope",
      namespace: "example.enum",
      fields: [
        {
          name: "unionField",
          type: [
            {
              type: "record",
              name: "EnumUser",
              fields: [{ name: "color", type: "Foo" }],
            },
            {
              type: "enum",
              name: "Foo",
              symbols: ["RED", "GREEN"],
            },
          ],
        },
      ],
    } as const;

    const type = createType(schema); // Writer with forward-ref enum.
    const value = {
      unionField: {
        "example.enum.EnumUser": { color: "RED" },
      },
    };

    const buffer = await type.toBuffer(value); // Writer round-trips.
    const decoded = await type.fromBuffer(buffer);
    assertEquals(decoded, value);

    // Reader with identical schema remains compatible via resolver.
    const readerType = createType(schema);
    const resolver = readerType.createResolver(type);
    const tap = new Tap(new ArrayBuffer(64));
    await type.write(tap, value);
    tap._testOnlyResetPos();
    const resolved = await resolver.read(tap);
    assertEquals(resolved, value);
  });

  it("fails schema evolution when reader adds required nested fields without defaults", () => {
    const readerSchema = {
      type: "record",
      name: "Envelope",
      namespace: "example.compat",
      fields: [
        {
          name: "unionField",
          type: [
            "null",
            {
              type: "record",
              name: "BranchReferencingFoo",
              fields: [
                { name: "foo", type: "Foo" },
                { name: "marker", type: "string" },
              ],
            },
            {
              type: "record",
              name: "BranchWithFooDefinition",
              fields: [
                {
                  name: "foo",
                  type: {
                    type: "record",
                    name: "Foo",
                    fields: [
                      { name: "id", type: "string" },
                      { name: "count", type: "int" },
                      { name: "extra", type: "string" },
                    ],
                  },
                },
                { name: "note", type: "string", default: "" },
              ],
            },
          ],
          default: null,
        },
      ],
    } as const;

    const writerType = createType(WRITER_SCHEMA);
    const readerType = createType(readerSchema);
    assertThrows(() => readerType.createResolver(writerType)); // Incompatible: reader adds Foo.extra without a default.
  });

  it("surfaces errors for missing deeply nested named types", () => {
    const schema = {
      type: "record",
      name: "Envelope",
      namespace: "example.missingdeep",
      fields: [
        {
          name: "payload",
          type: {
            type: "record",
            name: "Top",
            fields: [
              {
                name: "middle",
                type: {
                  type: "record",
                  name: "Middle",
                  fields: [
                    { name: "leaf", type: "UndefinedLeaf" },
                  ],
                },
              },
            ],
          },
        },
      ],
    } as const;

    const type = createType(schema);
    assertRejects( // Should fail because UndefinedLeaf is never defined.
      () => type.toBuffer({ payload: { middle: { leaf: { id: "x" } } } }),
      Error,
    );
  });

  it("treats similarly spelled named types as distinct", () => {
    const schema = {
      type: "record",
      name: "Envelope",
      namespace: "example.casesense",
      fields: [
        {
          name: "payload",
          type: [
            {
              type: "record",
              name: "BranchWithFoo",
              fields: [{ name: "foo", type: "Foo" }],
            },
            {
              type: "record",
              name: "BranchWithfooDefinition",
              fields: [
                {
                  name: "foo",
                  type: {
                    type: "record",
                    name: "foo",
                    fields: [{ name: "id", type: "string" }],
                  },
                },
              ],
            },
          ],
        },
      ],
    } as const;

    const type = createType(schema);
    assertRejects( // Should fail because Foo !== foo by case.
      () =>
        type.toBuffer({
          payload: {
            "example.casesense.BranchWithFoo": { foo: { id: "upper" } },
          },
        }),
      Error,
    );
  });
});
