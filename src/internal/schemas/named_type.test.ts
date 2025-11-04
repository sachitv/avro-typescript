import { assert, assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { Tap } from "../serialization/tap.ts";
import { NamedType } from "./named_type.ts";
import { ResolvedNames, resolveNames } from "./resolve_names.ts";
import { JSONType } from "./type.ts";

class DummyNamedType extends NamedType<string> {
  constructor(
    params: ResolvedNames,
  ) {
    super(params);
  }

  public override toBuffer(_value: string): ArrayBuffer {
    return new ArrayBuffer(0);
  }

  public override clone(value: string): string {
    return value;
  }

  public override write(_tap: Tap, _value: string): void {}

  public override read(_tap: Tap): string {
    return "";
  }

  public override skip(_tap: Tap): void {}

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

  public override match(_tap1: Tap, _tap2: Tap): number {
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

  it("match should return 0", () => {
    const params = {
      name: "Test",
      namespace: "test",
    };
    const resolved = resolveNames(params);
    const type = new DummyNamedType(resolved);

    const buf1 = type.toBuffer("a");
    const buf2 = type.toBuffer("b");

    assertEquals(type.match(new Tap(buf1), new Tap(buf2)), 0);
  });
});
