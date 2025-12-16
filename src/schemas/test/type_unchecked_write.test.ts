import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import type { WritableTapLike } from "../../serialization/tap.ts";
import type { SyncWritableTapLike } from "../../serialization/sync_tap.ts";
import { Type } from "../type.ts";

class TestType extends Type<number> {
  public override toBuffer(_value: number): Promise<ArrayBuffer> {
    return Promise.resolve(new ArrayBuffer(0));
  }
  public override fromBuffer(_buffer: ArrayBuffer): Promise<number> {
    return Promise.resolve(0);
  }
  public override isValid(_value: unknown): boolean {
    return true;
  }
  public override cloneFromValue(_value: unknown): number {
    return 0;
  }
  public override createResolver(_writerType: Type): never {
    throw new Error("not needed");
  }
  public override write(
    _tap: WritableTapLike,
    _value: number,
  ): Promise<void> {
    return Promise.resolve();
  }
  public override read(): Promise<number> {
    return Promise.resolve(0);
  }
  public override skip(): Promise<void> {
    return Promise.resolve();
  }
  public override check(): boolean {
    return true;
  }
  public override compare(): number {
    return 0;
  }
  public override random(): number {
    return 0;
  }
  public override toJSON(): string {
    return "int";
  }
  public override match(): Promise<number> {
    return Promise.resolve(0);
  }
  public override toSyncBuffer(_value: number): ArrayBuffer {
    return new ArrayBuffer(0);
  }
  public override fromSyncBuffer(_buffer: ArrayBuffer): number {
    return 0;
  }
  public override writeSync(_tap: SyncWritableTapLike, _value: number): void {}
  public override readSync(): number {
    return 0;
  }
  public override skipSync(): void {}
  public override matchSync(): number {
    return 0;
  }
}

describe("Type.writeUnchecked / writeSyncUnchecked", () => {
  it("defaults to delegating to write / writeSync", async () => {
    const type = new TestType();
    let asyncCalls = 0;
    let syncCalls = 0;

    type.write = () => {
      asyncCalls++;
      return Promise.resolve();
    };
    type.writeSync = () => {
      syncCalls++;
    };

    await type.writeUnchecked({} as WritableTapLike, 1);
    type.writeSyncUnchecked({} as SyncWritableTapLike, 1);

    expect(asyncCalls).toBe(1);
    expect(syncCalls).toBe(1);
  });
});
