import { assert, assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { TestTap as Tap } from "../../../serialization/test/test_tap.ts";
import { NamedType } from "../named_type.ts";
import { type ResolvedNames, resolveNames } from "../resolve_names.ts";
import type { JSONType } from "../../type.ts";

class DummyNamedType extends NamedType<string> {
  constructor(
    params: ResolvedNames,
  ) {
    super(params);
  }

  // deno-lint-ignore require-await
  public override async toBuffer(_value: string): Promise<ArrayBuffer> {
    return new ArrayBuffer(0);
  }

  public override cloneFromValue(value: string): string {
    return value;
  }

  public override async write(_tap: Tap, _value: string): Promise<void> {}

  // deno-lint-ignore require-await
  public override async read(_tap: Tap): Promise<string> {
    return "";
  }

  public override async skip(_tap: Tap): Promise<void> {}

  public override check(
    _value: unknown,
    _errorHook?: unknown,
    _path?: string[],
  ): boolean {
    return true;
  }

  public override compare(_a: string, _b: string): number {
    return 0;
  }

  public override random(): string {
    return "";
  }

  public override toJSON(): JSONType {
    return "dummy";
  }

  // deno-lint-ignore require-await
  public override async match(_tap1: Tap, _tap2: Tap): Promise<number> {
    return 0;
  }
}

describe("NamedType", () => {
  it("resolves fully qualified name and namespace", () => {
    const params = {
      name: "Person",
      namespace: "com.example",
      aliases: ["LegacyPerson", "other.Alias"],
    };
    const resolved = resolveNames(params);
    const type = new DummyNamedType(resolved);

    assertEquals(type.getFullName(), resolved.fullName);
    assertEquals(type.getNamespace(), resolved.namespace);
    assertEquals(type.getAliases(), resolved.aliases);
    assert(type.matchesName("com.example.Person"));
    assert(type.matchesName("com.example.LegacyPerson"));
    assert(type.matchesName("other.Alias"));
  });

  it("match should return 0", async () => {
    const params = {
      name: "Test",
      namespace: "test",
    };
    const resolved = resolveNames(params);
    const type = new DummyNamedType(resolved);

    const buf1 = await type.toBuffer("a");
    const buf2 = await type.toBuffer("b");

    assertEquals(await type.match(new Tap(buf1), new Tap(buf2)), 0);
  });
});
