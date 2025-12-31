import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import { SyncWritableTap } from "../../../serialization/tap_sync.ts";
import { SyncInMemoryWritableBuffer } from "../../../serialization/buffers/in_memory_buffer_sync.ts";
import { WritableTap } from "../../../serialization/tap.ts";
import { BooleanType } from "../boolean_type.ts";
import { BytesType } from "../bytes_type.ts";
import { DoubleType } from "../double_type.ts";
import { FloatType } from "../float_type.ts";
import { IntType } from "../int_type.ts";
import { LongType } from "../long_type.ts";
import { NullType } from "../null_type.ts";
import { StringType } from "../string_type.ts";

function encodeWithTap<T>(write: (tap: SyncWritableTap) => void): Uint8Array {
  const buffer = new ArrayBuffer(64);
  const writable = new SyncInMemoryWritableBuffer(buffer);
  const tap = new SyncWritableTap(writable);
  write(tap);
  return new Uint8Array(buffer, 0, tap.getPos());
}

type SyncWriteHarness = {
  writeSync(tap: SyncWritableTap, value: unknown): void;
  writeSyncUnchecked(tap: SyncWritableTap, value: unknown): void;
};

type AsyncWriteHarness = {
  write(tap: WritableTap, value: unknown): Promise<void>;
  writeUnchecked(tap: WritableTap, value: unknown): Promise<void>;
};

describe("Primitive types with validate=false", () => {
  it("writeSync routes through unchecked methods and stays byte-identical for valid inputs", () => {
    const cases: Array<[unknown, unknown, unknown]> = [
      [new BooleanType(true), new BooleanType(false), true],
      [new IntType(true), new IntType(false), 123],
      [new LongType(true), new LongType(false), 123n],
      [new FloatType(true), new FloatType(false), 1.5],
      [new DoubleType(true), new DoubleType(false), 1.5],
      [new BytesType(true), new BytesType(false), new Uint8Array([1, 2, 3])],
      [new StringType(true), new StringType(false), "hello"],
      [new NullType(true), new NullType(false), null],
    ];

    for (const [strict, unchecked, value] of cases) {
      const strictBytes = encodeWithTap((tap) =>
        (strict as unknown as SyncWriteHarness).writeSync(tap, value)
      );
      const uncheckedBytes = encodeWithTap((tap) =>
        (unchecked as unknown as SyncWriteHarness).writeSync(tap, value)
      );
      const uncheckedDirect = encodeWithTap((tap) =>
        (unchecked as unknown as SyncWriteHarness).writeSyncUnchecked(
          tap,
          value,
        )
      );

      expect(uncheckedBytes).toEqual(strictBytes);
      expect(uncheckedDirect).toEqual(strictBytes);
    }
  });

  it("write and writeUnchecked stay byte-identical for valid inputs (async)", async () => {
    const cases: Array<[unknown, unknown, unknown]> = [
      [new BooleanType(true), new BooleanType(false), true],
      [new IntType(true), new IntType(false), 123],
      [new LongType(true), new LongType(false), 123n],
      [new FloatType(true), new FloatType(false), 1.5],
      [new DoubleType(true), new DoubleType(false), 1.5],
      [new BytesType(true), new BytesType(false), new Uint8Array([1, 2, 3])],
      [new StringType(true), new StringType(false), "hello"],
      [new NullType(true), new NullType(false), null],
    ];

    for (const [strict, unchecked, value] of cases) {
      const strictBuffer = new ArrayBuffer(128);
      const strictTap = new WritableTap(strictBuffer);
      await (strict as unknown as AsyncWriteHarness).write(strictTap, value);

      const uncheckedBuffer = new ArrayBuffer(128);
      const uncheckedTap = new WritableTap(uncheckedBuffer);
      await (unchecked as unknown as AsyncWriteHarness).write(
        uncheckedTap,
        value,
      );

      const uncheckedDirectBuffer = new ArrayBuffer(128);
      const uncheckedDirectTap = new WritableTap(uncheckedDirectBuffer);
      await (unchecked as unknown as AsyncWriteHarness).writeUnchecked(
        uncheckedDirectTap,
        value,
      );

      expect(new Uint8Array(uncheckedBuffer, 0, uncheckedTap.getPos()))
        .toEqual(new Uint8Array(strictBuffer, 0, strictTap.getPos()));
      expect(
        new Uint8Array(uncheckedDirectBuffer, 0, uncheckedDirectTap.getPos()),
      ).toEqual(new Uint8Array(strictBuffer, 0, strictTap.getPos()));
    }
  });

  it("toBuffer/toSyncBuffer skip validation when validate=false", async () => {
    const strict = new StringType(true);
    const unchecked = new StringType(false);

    const strictAsync = new Uint8Array(await strict.toBuffer("hello"));
    const uncheckedAsync = new Uint8Array(await unchecked.toBuffer("hello"));
    expect(uncheckedAsync).toEqual(strictAsync);

    const strictSync = new Uint8Array(strict.toSyncBuffer("hello"));
    const uncheckedSync = new Uint8Array(unchecked.toSyncBuffer("hello"));
    expect(uncheckedSync).toEqual(strictSync);
  });

  it("writeUnchecked routes through unchecked methods (async)", async () => {
    const type = new StringType(false);
    const buffer = new ArrayBuffer(64);
    const tap = new WritableTap(buffer);
    await type.writeUnchecked(tap, "hello");
    expect(tap.getPos()).toBeGreaterThan(0);
  });
});
