import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";
import type { ReadableTapLike, WritableTapLike } from "../../serialization/tap.ts";
import type {
  SyncReadableTapLike,
  SyncWritableTapLike,
} from "../../serialization/sync_tap.ts";
import { Type } from "../type.ts";

class TestType extends Type<number> {
  constructor() {
    super();
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
  public override writeUnchecked(
    _tap: WritableTapLike,
    _value: number,
  ): Promise<void> {
    return Promise.resolve();
  }
  public override read(_tap: ReadableTapLike): Promise<number> {
    return Promise.resolve(0);
  }
  public override skip(_tap: ReadableTapLike): Promise<void> {
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
  public override match(
    _tap1: ReadableTapLike,
    _tap2: ReadableTapLike,
  ): Promise<number> {
    return Promise.resolve(0);
  }
  public override fromSyncBuffer(_buffer: ArrayBuffer): number {
    return 0;
  }
  public override writeSyncUnchecked(
    _tap: SyncWritableTapLike,
    _value: number,
  ): void {}
  public override readSync(_tap: SyncReadableTapLike): number {
    return 0;
  }
  public override skipSync(_tap: SyncReadableTapLike): void {}
  public override matchSync(
    _tap1: SyncReadableTapLike,
    _tap2: SyncReadableTapLike,
  ): number {
    return 0;
  }
}

describe("Type.write / writeSync", () => {
  it("delegates to writeUnchecked / writeSyncUnchecked", async () => {
    const type = new TestType();
    let asyncCalls = 0;
    let syncCalls = 0;

    type.writeUnchecked = () => {
      asyncCalls++;
      return Promise.resolve();
    };
    type.writeSyncUnchecked = () => {
      syncCalls++;
    };

    await type.write({} as WritableTapLike, 1);
    type.writeSync({} as SyncWritableTapLike, 1);

    expect(asyncCalls).toBe(1);
    expect(syncCalls).toBe(1);
  });
});

describe("Type.toBuffer / toSyncBuffer size hints", () => {
  class BadSizeType extends Type<number> {
    constructor() {
      super();
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

    public override writeUnchecked(
      _tap: WritableTapLike,
      _value: number,
    ): Promise<void> {
      return Promise.resolve();
    }

    public override read(_tap: ReadableTapLike): Promise<number> {
      return Promise.resolve(0);
    }

    public override skip(_tap: ReadableTapLike): Promise<void> {
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

    public override match(
      _tap1: ReadableTapLike,
      _tap2: ReadableTapLike,
    ): Promise<number> {
      return Promise.resolve(0);
    }

    public override fromSyncBuffer(_buffer: ArrayBuffer): number {
      return 0;
    }

    public override writeSyncUnchecked(
      _tap: SyncWritableTapLike,
      _value: number,
    ): void {}

    public override readSync(_tap: SyncReadableTapLike): number {
      return 0;
    }

    public override skipSync(_tap: SyncReadableTapLike): void {}

    public override matchSync(
      _tap1: SyncReadableTapLike,
      _tap2: SyncReadableTapLike,
    ): number {
      return 0;
    }

    protected override byteLength(): number | undefined {
      return NaN;
    }
  }

  it("throws when byteLength is invalid", async () => {
    const type = new BadSizeType();
    await expect(type.toBuffer(1)).rejects.toThrow(RangeError);
    expect(() => type.toSyncBuffer(1)).toThrow(RangeError);
  });
});
