/**
 * The state shared while a type is turned into its JSON schema.
 *
 * Avro defines a record, enum, or fixed once and then refers to it by name: a
 * schema that defines the same name twice is invalid, and a recursive record
 * can only be written by referring back to itself. A single type's JSON does
 * not know what the rest of the schema has defined, so the types pass a
 * {@link SchemaJSONScope} down as they write their children:
 *
 * - A top-level `toJSON()` creates a new scope and calls
 *   `schemaJSON(scope)` on itself.
 * - Types with children (records, arrays, maps, unions, and logical types
 *   over named types) call `child.schemaJSON(scope)` with the same scope.
 * - Named types use {@link namedTypeJSON}: the first time a name is written
 *   it is defined in full, and after that it is written as its full name,
 *   which also ends the recursion for recursive records.
 *
 * A name can only stand for one definition, so writing it again is refused
 * when it would mean something else: a different type with the same name, a
 * type in the null namespace referenced from inside a namespace (the Avro
 * specification has no portable syntax for that reference), or a named type
 * used both with and without a logical type. These fail loudly rather than
 * write a file whose schema does not describe its data.
 *
 * The scope also tracks the namespace that a bare name resolves in at the
 * current point: the namespace of the nearest enclosing record, or "" at the
 * top. A type in the null namespace nested under a namespaced record is
 * written with `"namespace": ""`, or the parser would move it into the
 * record's namespace.
 *
 * @module
 */

import type { NamedType } from "./complex/named_type.ts";
import { safeStringify } from "./json.ts";
import type { JSONType, Type } from "./type.ts";

/** What a name written in the schema stands for. */
export interface DefinedName {
  /** The named type whose definition was written. */
  readonly type: NamedType;
  /**
   * The logical type that wrote the definition around `type` (such as a
   * decimal over a fixed), if any. The parser maps the name to it.
   */
  logical?: Type;
}

/**
 * What the schema being written has defined so far, and the namespace that
 * names resolve in at the current point. Create one per top-level `toJSON()`.
 */
export class SchemaJSONScope {
  /** Names written as definitions so far, by full name. */
  public readonly defined: Map<string, DefinedName> = new Map();
  /** Namespace bare names resolve in here; "" for the null namespace. */
  public namespace = "";
  /**
   * What {@link sameSchema} knows about pairs of types during this write,
   * shared with the scopes it writes them in.
   */
  public readonly comparison: SchemaComparison;

  /**
   * Creates an empty scope.
   *
   * @param comparison The comparison state to share, when this scope writes
   * a type for {@link sameSchema}; a top-level `toJSON()` starts a new one.
   */
  constructor(comparison: SchemaComparison = new SchemaComparison()) {
    this.comparison = comparison;
  }
}

/** A set of ordered pairs of types. */
export class TypePairs {
  readonly #pairs: Map<Type, Set<Type>> = new Map();

  /** Whether the pair (a, b) is in the set. */
  public has(a: Type, b: Type): boolean {
    return this.#pairs.get(a)?.has(b) ?? false;
  }

  /** Adds the pair (a, b). */
  public add(a: Type, b: Type): void {
    let partners = this.#pairs.get(a);
    if (partners === undefined) {
      partners = new Set();
      this.#pairs.set(a, partners);
    }
    partners.add(b);
  }

  /** Removes the pair (a, b), if present. */
  public delete(a: Type, b: Type): void {
    const partners = this.#pairs.get(a);
    if (partners === undefined) {
      return;
    }
    partners.delete(b);
    if (partners.size === 0) {
      this.#pairs.delete(a);
    }
  }

  /** The number of types with at least one partner. */
  public get size(): number {
    return this.#pairs.size;
  }
}

/**
 * What {@link sameSchema} knows about pairs of types during one top-level
 * `toJSON()`.
 */
export class SchemaComparison {
  /**
   * Pairs whose comparison is in progress. A pair met again while it is still
   * being compared is taken to be equal, which ends the recursion when a type
   * contains another instance with its own name.
   */
  public readonly comparing: TypePairs = new TypePairs();
  /**
   * Pairs found equal, so each pair is compared once per write however many
   * paths reach it. A pair found equal while an enclosing pair was assumed
   * equal is only as good as that assumption; but a pair found different
   * makes the write throw (the callers of {@link sameSchema} refuse it), so a
   * failed assumption never leaves an entry here that a finished write used.
   */
  public readonly equal: TypePairs = new TypePairs();
}

/**
 * Returns whether two types write the same schema on their own, so one can
 * stand in for the other by name.
 *
 * Types parsed from one schema share instances, so this usually returns on the
 * identity check; the schema comparison only runs when separately built types
 * with the same name meet in one schema. Each type is written in a fresh scope
 * sharing `scope.comparison`: a pair reached again during its own comparison
 * (a type containing another instance with its own name) is assumed equal,
 * and the outer comparison decides. Pairs found equal are remembered for the
 * rest of the write.
 *
 * @param a The type that defined the name.
 * @param b The type being written under the same name.
 * @param scope The scope of the schema being written.
 * @returns Whether `b` can be written as a reference to `a`.
 */
export function sameSchema(a: Type, b: Type, scope: SchemaJSONScope): boolean {
  if (a === b) {
    return true;
  }
  const { comparing, equal } = scope.comparison;
  if (equal.has(a, b) || comparing.has(a, b)) {
    return true;
  }
  comparing.add(a, b);
  let same: boolean;
  try {
    const schemaA = a.schemaJSON(new SchemaJSONScope(scope.comparison));
    const schemaB = b.schemaJSON(new SchemaJSONScope(scope.comparison));
    same = safeStringify(schemaA) === safeStringify(schemaB);
  } finally {
    comparing.delete(a, b);
  }
  if (same) {
    equal.add(a, b);
  }
  return same;
}

/**
 * Checks that `type` may be written as a reference to its name, which the
 * scope already defines.
 *
 * @param type The named type to refer to.
 * @param defined What the scope already has under the type's full name.
 * @param scope The scope of the schema being written.
 * @throws Error when the name was defined by a different type, or when the
 * type is in the null namespace and the reference would be read in another
 * namespace.
 */
export function assertReferable(
  type: NamedType,
  defined: DefinedName,
  scope: SchemaJSONScope,
): void {
  const fullName = type.getFullName();
  if (!sameSchema(defined.type, type, scope)) {
    throw new Error(`Duplicate Avro type name: ${fullName}`);
  }
  if (type.getNamespace() === "" && scope.namespace !== "") {
    throw new Error(
      `Cannot refer to ${fullName} in the null namespace from namespace ${scope.namespace}: the Avro specification has no portable syntax for that reference.`,
    );
  }
}

/**
 * Writes a named type within `scope`: its full name when the schema already
 * defines it, otherwise its definition, which includes its aliases (as full
 * names) when it has any. `definition` builds the type-specific attributes; it
 * runs after the name is recorded, so a recursive record that reaches itself
 * again gets its name.
 *
 * Aliases are treated as part of the definition, so two types that differ only
 * in their aliases are different types, and using both under one name is
 * refused. That is this library's choice, not a rule of the Avro
 * specification (whose Parsing Canonical Form leaves aliases out); it keeps
 * one name from silently dropping another type's aliases.
 *
 * @param type The named type to write.
 * @param typeName The Avro type name, e.g. `"enum"`.
 * @param scope The scope of the schema being written.
 * @param definition Builds the attributes that follow `name` and `type`.
 * @returns The definition, or the full name when already defined.
 * @throws Error when the name cannot be written as a reference to the
 * existing definition (see {@link assertReferable}), when it was defined
 * with a logical type, which a plain reference would silently pick up, or
 * when a namespaced type has an alias in the null namespace.
 */
export function namedTypeJSON(
  type: NamedType,
  typeName: string,
  scope: SchemaJSONScope,
  definition: () => { [key: string]: JSONType },
): JSONType {
  const fullName = type.getFullName();
  const defined = scope.defined.get(fullName);
  if (defined !== undefined) {
    assertReferable(type, defined, scope);
    if (defined.logical !== undefined) {
      throw new Error(
        `Cannot refer to ${fullName} without its logical type: the schema defines it as ${
          safeStringify(defined.logical.toJSON()).trim()
        }.`,
      );
    }
    return fullName;
  }
  scope.defined.set(fullName, { type });

  const json: { [key: string]: JSONType } = { name: fullName };
  // A bare name would otherwise take the enclosing record's namespace.
  if (type.getNamespace() === "" && scope.namespace !== "") {
    json.namespace = "";
  }
  json.type = typeName;
  // Aliases are stored as full names, so they read back the same whatever
  // namespace the definition ends up in.
  const aliases = type.getAliases();
  if (aliases.length > 0) {
    assertAliasesWritable(type, aliases);
    json.aliases = aliases;
  }
  return Object.assign(json, definition());
}

/**
 * Checks that every alias of `type` reads back as itself. A bare alias is
 * qualified with the type's namespace when parsed, so a namespaced type cannot
 * carry an alias in the null namespace: written as its full name (which has no
 * dot), it would read back in the type's namespace.
 *
 * @throws Error when a namespaced type has an alias in the null namespace.
 */
function assertAliasesWritable(type: NamedType, aliases: string[]): void {
  if (type.getNamespace() === "") {
    return;
  }
  for (const alias of aliases) {
    if (!alias.includes(".")) {
      throw new Error(
        `Cannot write alias ${alias} of ${type.getFullName()}: it is in the null namespace, and the Avro specification has no portable syntax for a null-namespace alias on a namespaced type.`,
      );
    }
  }
}
