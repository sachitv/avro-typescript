import { assertEquals, assertRejects } from "@std/assert";
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
});
