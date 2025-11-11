import { FixedType } from "../fixed_type.ts";
import { StringType } from "../string_type.ts";
import type { NamedType } from "../named_type.ts";
import { withLogicalTypeJSON } from "./logical_type.ts";
import { LogicalType } from "./logical_type.ts";
import type { Type } from "../type.ts";

const UUID_REGEX =
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;

export class UuidLogicalType extends LogicalType<string, string | Uint8Array> {
  readonly #underlying: StringType | FixedType;
  readonly #named?: NamedType<Uint8Array>;

  constructor(underlying: StringType | FixedType) {
    super(underlying as unknown as Type<string | Uint8Array>);
    this.#underlying = underlying;

    if (underlying instanceof FixedType) {
      if (underlying.getSize() !== 16) {
        throw new Error("UUID logical type requires fixed size of 16 bytes.");
      }
      this.#named = underlying;
    }
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof UuidLogicalType;
  }

  protected override isInstance(value: unknown): value is string {
    return typeof value === "string" && UUID_REGEX.test(value);
  }

  protected override toUnderlying(value: string): string | Uint8Array {
    if (this.#underlying instanceof StringType) {
      return value;
    }
    return uuidToBytes(value);
  }

  protected override fromUnderlying(value: string | Uint8Array): string {
    if (typeof value === "string") {
      return value;
    }
    return bytesToUuid(value);
  }

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

  public override toJSON() {
    return withLogicalTypeJSON(this.#underlying.toJSON(), "uuid");
  }

  public getFullName(): string {
    if (!this.#named) {
      throw new Error("UUID logical type backed by string has no name.");
    }
    return this.#named.getFullName();
  }

  public getNamespace(): string {
    if (!this.#named) {
      throw new Error("UUID logical type backed by string has no namespace.");
    }
    return this.#named.getNamespace();
  }

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
