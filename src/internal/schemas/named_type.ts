import { BaseType } from "./base_type.ts";
import { ResolvedNames } from "./resolve_names.ts";

/**
 * Base class for Avro types that carry a schema name (`record`, `enum`, `fixed`).
 * Resolves the primary name, namespace, and aliases to fully qualified names.
 */
export abstract class NamedType<T = unknown> extends BaseType<T> {
  readonly #fullName: string;
  readonly #namespace: string;
  readonly #aliases: string[];

  protected constructor(resolvedNames: ResolvedNames) {
    super();
    this.#fullName = resolvedNames.fullName;
    this.#namespace = resolvedNames.namespace;
    this.#aliases = resolvedNames.aliases;
  }

  /**
   * Fully qualified schema name.
   */
  public getFullName(): string {
    return this.#fullName;
  }

  /**
   * Namespace associated with this type (empty string when unspecified).
   */
  public getNamespace(): string {
    return this.#namespace;
  }

  /**
   * Fully qualified aliases for this type.
   */
  public getAliases(): string[] {
    return this.#aliases.slice();
  }

  /**
   * Checks whether the provided name matches either the full name or any alias.
   */
  public matchesName(name: string): boolean {
    return name === this.#fullName || this.#aliases.includes(name);
  }
}
