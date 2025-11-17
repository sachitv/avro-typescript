import { BytesType } from "../primitive/bytes_type.ts";
import { FixedType } from "../complex/fixed_type.ts";
import { withLogicalTypeJSON } from "./logical_type.ts";
import { LogicalType } from "./logical_type.ts";

interface DecimalParams {
  precision: number;
  scale?: number;
}

export class DecimalLogicalType extends LogicalType<bigint, Uint8Array> {
  readonly #precision: number;
  readonly #scale: number;
  readonly #fixedSize?: number;

  constructor(underlying: BytesType | FixedType, params: DecimalParams) {
    super(underlying);

    if (!(underlying instanceof BytesType || underlying instanceof FixedType)) {
      throw new Error(
        "Decimal logical type requires bytes or fixed underlying type.",
      );
    }

    const precision = params.precision;
    if (!Number.isInteger(precision) || precision <= 0) {
      throw new Error("Decimal logical type requires a positive precision.");
    }

    const scale = params.scale ?? 0;
    if (!Number.isInteger(scale) || scale < 0 || scale > precision) {
      throw new Error(
        "Decimal logical type requires a valid scale (0 <= scale <= precision).",
      );
    }

    if (underlying instanceof FixedType) {
      const size = underlying.getSize();
      const maxPrecision = Math.floor(Math.log10(2) * (8 * size - 1));
      if (precision > maxPrecision) {
        throw new Error("Decimal precision exceeds maximum for fixed size.");
      }
      this.#fixedSize = size;
    }

    this.#precision = precision;
    this.#scale = scale;
  }

  public getPrecision(): number {
    return this.#precision;
  }

  public getScale(): number {
    return this.#scale;
  }

  protected override canReadFromLogical(
    writer: LogicalType<unknown, unknown>,
  ): boolean {
    return writer instanceof DecimalLogicalType &&
      writer.#precision === this.#precision &&
      writer.#scale === this.#scale;
  }

  protected override isInstance(value: unknown): value is bigint {
    return typeof value === "bigint";
  }

  protected override toUnderlying(value: bigint): Uint8Array {
    const digits = value < 0n
      ? (-value).toString().length
      : value.toString().length;
    if (digits > this.#precision) {
      throw new Error(
        `Decimal value: ${value} exceeds declared precision: ${this.#precision}`,
      );
    }

    const size = this.#fixedSize;
    // Constructor already enforced precision <= max for the fixed size, so every
    // value that passes the precision check fits within `size` bytes.
    const byteLength = size ?? minimalBytesForValue(value);

    return encodeBigInt(value, byteLength);
  }

  protected override fromUnderlying(value: Uint8Array): bigint {
    return decodeBigInt(value);
  }

  public override toJSON() {
    const extras: Record<string, unknown> = {
      precision: this.#precision,
    };
    if (this.#scale !== 0) {
      extras.scale = this.#scale;
    }
    return withLogicalTypeJSON(
      this.getUnderlyingType().toJSON(),
      "decimal",
      extras,
    );
  }
}

function minimalBytesForValue(value: bigint): number {
  let bytes = 1;
  while (
    value < -(1n << BigInt(bytes * 8 - 1)) ||
    value >= (1n << BigInt(bytes * 8 - 1))
  ) {
    bytes++;
  }
  return bytes;
}

function encodeBigInt(value: bigint, size: number): Uint8Array {
  let twos = BigInt.asUintN(size * 8, value);
  const result = new Uint8Array(size);
  for (let i = size - 1; i >= 0; i--) {
    result[i] = Number(twos & 0xffn);
    twos >>= 8n;
  }
  return result;
}

function decodeBigInt(bytes: Uint8Array): bigint {
  if (bytes.length === 0) {
    return 0n;
  }
  let value = 0n;
  for (const byte of bytes) {
    value = (value << 8n) | BigInt(byte);
  }
  if (bytes[0] & 0x80) {
    const max = 1n << BigInt(bytes.length * 8);
    value -= max;
  }
  return value;
}
