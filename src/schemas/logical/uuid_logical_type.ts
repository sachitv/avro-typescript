import { FixedType } from "../complex/fixed_type.ts";
import { StringType } from "../primitive/string_type.ts";
import type { NamedType } from "../complex/named_type.ts";
import { withLogicalTypeJSON } from "./logical_type.ts";
import { LogicalType } from "./logical_type.ts";
import type { JSONType, Type } from "../type.ts";

const UUID_REGEX =
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;

/**
 * Logical type for UUID.
 * Represents a universally unique identifier.
 */
export class UuidLogicalType extends LogicalType<string, string | Uint8Array> {
  readonly #underlying: StringType | FixedType;
  readonly #named?: NamedType<Uint8Array>;

  /**
   * Creates a UUID logical type backed by the given underlying type.
   * @param underlying The underlying StringType or FixedType.
   */
  constructor(underlying: StringType | FixedType, validate = true) {
    super(underlying as unknown as Type<string | Uint8Array>, validate);
    this.#underlying = underlying;

    if (underlying instanceof FixedType) {
      if (underlying.getSize() !== 16) {
        throw new Error("UUID logical type requires fixed size of 16 bytes.");
      }
      this.#named = underlying;
    }
  }

  /**
   * Checks if this UUID logical type can read from the given writer logical type.
   * @param writer The writer logical type.
   * @returns True if compatible, false otherwise.
   */
  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof UuidLogicalType;
  }

  /**
   * Determines if the given value is a valid UUID string.
   * @param value The value to check.
   * @returns True if the value is a valid UUID string.
   */
  protected override isInstance(value: unknown): value is string {
    return typeof value === "string" && UUID_REGEX.test(value);
  }

  /**
   * Converts a UUID string to the underlying type's representation.
   * @param value The UUID string.
   * @returns The underlying representation.
   */
  protected override toUnderlying(value: string): string | Uint8Array {
    if (this.#underlying instanceof StringType) {
      return value;
    }
    return uuidToBytes(value);
  }

  /**
   * Converts from the underlying type's representation to a UUID string.
   * @param value The underlying value.
   * @returns The UUID string.
   */
  protected override fromUnderlying(value: string | Uint8Array): string {
    if (typeof value === "string") {
      return value;
    }
    return bytesToUuid(value);
  }

  /**
   * Generates a random UUID string.
   */
  public override random(): string {
    if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
      return crypto.randomUUID();
    }
    const bytes = new Uint8Array(16);
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = Math.floor(Math.random() * 256);
    }
    // Set version (4) and variant bits for RFC-4122 compliance.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    return bytesToUuid(bytes);
  }

  /**
   * Returns the JSON schema representation of this logical type.
   */
  public override toJSON(): JSONType {
    return withLogicalTypeJSON(this.#underlying.toJSON(), "uuid");
  }

  /**
   * Returns the full name of the named type backing this UUID.
   * Throws if backed by a primitive string type.
   */
  public getFullName(): string {
    if (!this.#named) {
      throw new Error("UUID logical type backed by string has no name.");
    }
    return this.#named.getFullName();
  }

  /**
   * Returns the namespace of the named type backing this UUID.
   * Throws if backed by a primitive string type.
   */
  public getNamespace(): string {
    if (!this.#named) {
      throw new Error("UUID logical type backed by string has no namespace.");
    }
    return this.#named.getNamespace();
  }

  /**
   * Returns the aliases of the named type backing this UUID.
   * Throws if backed by a primitive string type.
   */
  public getAliases(): string[] {
    if (!this.#named) {
      throw new Error("UUID logical type backed by string has no aliases.");
    }
    return this.#named.getAliases();
  }
}

function uuidToBytes(value: string): Uint8Array {
  const hex = value.replace(/-/g, "");
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function bytesToUuid(bytes: Uint8Array): string {
  if (bytes.length !== 16) {
    throw new Error("UUID bytes must be 16 bytes long.");
  }
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"))
    .join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${
    hex.slice(16, 20)
  }-${hex.slice(20)}`;
}
