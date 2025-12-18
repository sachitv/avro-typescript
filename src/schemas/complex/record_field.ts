import { isValidName } from "./resolve_names.ts";
import { Type } from "../type.ts";

/**
 * Specifies the sort order for record fields.
 */
export type RecordFieldOrder = "ascending" | "descending" | "ignore";

/**
 * Parameters for defining a record field.
 */
export interface RecordFieldParams {
  /** The name of the field. */
  name: string;
  /** The type of the field. */
  type: Type;
  /** Optional aliases for the field. */
  aliases?: string[];
  /** Optional ordering for the field. */
  order?: RecordFieldOrder;
  /** Optional default value for the field. */
  default?: unknown;
}

/**
 * Represents a field in an Avro record type.
 */
export class RecordField {
  #name: string;
  #type: Type;
  #aliases: string[];
  #order: RecordFieldOrder;
  #hasDefault: boolean;
  #defaultValue?: unknown;

  /**
   * Constructs a new RecordField instance.
   */
  constructor(params: RecordFieldParams) {
    const { name, type, aliases = [], order = "ascending" } = params;

    if (typeof name !== "string" || !isValidName(name)) {
      throw new Error(`Invalid record field name: ${name}`);
    }
    if (!(type instanceof Type)) {
      throw new Error(`Invalid field type for ${name}`);
    }

    if (!RecordField.#isValidOrder(order)) {
      throw new Error(`Invalid record field order: ${order}`);
    }

    this.#name = name;
    this.#type = type;
    this.#order = order;

    const aliasSet = new Set<string>();
    const resolvedAliases: string[] = [];
    for (const alias of aliases) {
      if (typeof alias !== "string" || !isValidName(alias)) {
        throw new Error(`Invalid record field alias: ${alias}`);
      }
      if (!aliasSet.has(alias)) {
        aliasSet.add(alias);
        resolvedAliases.push(alias);
      }
    }
    this.#aliases = resolvedAliases;

    this.#hasDefault = Object.prototype.hasOwnProperty.call(params, "default");
    if (this.#hasDefault) {
      this.#defaultValue = this.#type.cloneFromValue(params.default as unknown);
    }
  }

  /**
   * Gets the name of the field.
   */
  public getName(): string {
    return this.#name;
  }

  /**
   * Gets the type of the field.
   */
  public getType(): Type {
    return this.#type;
  }

  /**
   * Gets the aliases of the field.
   */
  public getAliases(): string[] {
    return this.#aliases.slice();
  }

  /**
   * Gets the order of the field.
   */
  public getOrder(): RecordFieldOrder {
    return this.#order;
  }

  /**
   * Returns true if the field has a default value.
   */
  public hasDefault(): boolean {
    return this.#hasDefault;
  }

  /**
   * Gets the default value for the field.
   * @returns The default value.
   * @throws Error if the field has no default.
   */
  public getDefault(): unknown {
    if (!this.#hasDefault) {
      throw new Error(`Field '${this.#name}' has no default.`);
    }
    return this.#type.cloneFromValue(this.#defaultValue as unknown);
  }

  /**
   * Checks if the given name matches the field name or any of its aliases.
   * @param name The name to check.
   * @returns True if the name matches, false otherwise.
   */
  public nameMatches(name: string): boolean {
    return name === this.#name || this.#aliases.includes(name);
  }

  static #isValidOrder(order: string): order is RecordFieldOrder {
    return order === "ascending" || order === "descending" ||
      order === "ignore";
  }
}
