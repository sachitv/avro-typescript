import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  LogicalType,
  NamedLogicalType,
  withLogicalTypeJSON,
} from "../logical_type.ts";
import { StringType } from "../../primitive/string_type.ts";
import { FixedType } from "../../complex/fixed_type.ts";
import { resolveNames } from "../../complex/resolve_names.ts";
import { ReadableTap, WritableTap } from "../../../serialization/tap.ts";
import {
  SyncReadableTap,
  SyncWritableTap,
} from "../../../serialization/sync_tap.ts";
import { ValidationError } from "../../error.ts";

// A simple test logical type that wraps strings
class TestLogicalType extends LogicalType<string, string> {
  constructor() {
    super(new StringType());
  }

  protected isInstance(value: unknown): value is string {
    return typeof value === "string";
  }

  protected toUnderlying(value: string): string {
    if (value === "throw") {
      throw new Error("Test throw");
    }
    return value;
  }

  protected fromUnderlying(value: string): string {
    return value;
  }

  public toJSON() {
    return withLogicalTypeJSON("string", "test");
  }
}

// Another test logical type for incompatibility
class IncompatibleLogicalType extends LogicalType<string, string> {
  constructor() {
    super(new StringType());
  }

  protected isInstance(value: unknown): value is string {
    return typeof value === "string";
  }

  protected toUnderlying(value: string): string {
    return value;
  }

  protected fromUnderlying(value: string): string {
    return value;
  }

  public toJSON() {
    return withLogicalTypeJSON("string", "incompatible");
  }
}

// A test named logical type for testing NamedLogicalType
class TestNamedLogicalType extends NamedLogicalType<string, Uint8Array> {
  constructor() {
    const names = resolveNames({
      name: "TestNamed",
      namespace: "test.namespace",
    });
    super(new FixedType({ ...names, size: 4 }));
  }

  protected isInstance(value: unknown): value is string {
    return typeof value === "string";
  }

  protected toUnderlying(value: string): Uint8Array {
    return new TextEncoder().encode(value.padEnd(4, "\0").slice(0, 4));
  }

  protected fromUnderlying(value: Uint8Array): string {
    return new TextDecoder().decode(value).replace(/\0+$/, "");
  }

  public toJSON() {
    return withLogicalTypeJSON(
      { type: "fixed", name: "TestNamed", size: 4 },
      "test_named",
    );
  }
}

describe("LogicalType", () => {
  it("isValid calls check with errorHook on invalid value", () => {
    const type = new TestLogicalType();
    let called = false;
    const errorHook = () => {
      called = true;
    };
    type.isValid(123, { errorHook }); // invalid value
    assert(called);
  });

  it("check calls errorHook when toUnderlying throws", () => {
    const type = new TestLogicalType();
    let called = false;
    const errorHook = () => {
      called = true;
    };
    type.check("throw", errorHook);
    assert(called);
  });

  it("check returns false when isInstance fails without errorHook", () => {
    const type = new TestLogicalType();
    const result = type.check(123); // invalid value, no errorHook
    assertEquals(result, false);
  });

  it("check returns false when toUnderlying throws without errorHook", () => {
    const type = new TestLogicalType();
    const result = type.check("throw"); // toUnderlying throws, no errorHook
    assertEquals(result, false);
  });

  it("createResolver throws for incompatible logical types", () => {
    const reader = new TestLogicalType();
    const writer = new IncompatibleLogicalType();
    assertThrows(
      () => {
        reader.createResolver(writer);
      },
      Error,
      "Schema evolution not supported between incompatible logical types.",
    );
  });

  it("canReadFromLogical returns false for different constructors", () => {
    const reader = new TestLogicalType();
    const writer = new IncompatibleLogicalType();
    // This tests the branch where canReadFromLogical returns false
    assertThrows(() => {
      reader.createResolver(writer);
    });
  });

  it("createResolver creates resolver for compatible logical types", () => {
    const reader = new TestLogicalType();
    const writer = new TestLogicalType();
    const resolver = reader.createResolver(writer);
    assert(resolver !== undefined);
  });

  it("createResolver creates resolver for non-logical writer", () => {
    const reader = new TestLogicalType();
    const writer = new StringType();
    const resolver = reader.createResolver(writer);
    assert(resolver !== undefined);
  });

  it("toBuffer throws when ensureValid fails", async () => {
    const type = new TestLogicalType();
    await assertRejects(async () => {
      await type.toBuffer("throw");
    });
  });

  it("withLogicalTypeJSON handles object underlying", () => {
    const result = withLogicalTypeJSON({ type: "string", size: 10 }, "test", {
      extra: "value",
    });
    assertEquals(result, {
      type: "string",
      size: 10,
      logicalType: "test",
      extra: "value",
    });
  });

  // Tests for missing coverage lines 25-27: convertFromUnderlying() method
  it("convertFromUnderlying converts underlying value to logical value", () => {
    const type = new TestLogicalType();
    const result = type.convertFromUnderlying("test");
    assertEquals(result, "test");
  });

  // Tests for missing coverage lines 73-77: cloneFromValue() method
  it("cloneFromValue creates a copy of the logical value", () => {
    const type = new TestLogicalType();
    const original = "test";
    const cloned = type.cloneFromValue(original);
    assertEquals(cloned, original);
  });

  it("cloneFromValue throws when ensureValid fails", () => {
    const type = new TestLogicalType();
    assertThrows(() => {
      type.cloneFromValue("throw");
    });
  });

  // Tests for missing coverage lines 85-88: random() method
  it("random generates a valid logical value", () => {
    const type = new TestLogicalType();
    const value = type.random();
    assertEquals(typeof value, "string");
    assert(type.isValid(value));
  });

  // Tests for missing coverage lines 90-96: write() method
  it("write writes logical value to tap", async () => {
    const type = new TestLogicalType();
    const buffer = new ArrayBuffer(100);
    const tap = new WritableTap(buffer);
    await type.write(tap, "test");
    assertEquals(tap.getPos() > 0, true);
  });

  it("write throws when ensureValid fails", async () => {
    const type = new TestLogicalType();
    const buffer = new ArrayBuffer(100);
    const tap = new WritableTap(buffer);
    await assertRejects(async () => {
      await type.write(tap, "throw");
    });
  });

  // Tests for missing coverage lines 98-101: read() method
  it("read reads logical value from tap", async () => {
    const type = new TestLogicalType();
    const buffer = new ArrayBuffer(100);
    const writeTap = new WritableTap(buffer);
    await writeTap.writeString("test");

    const readBuffer = new ArrayBuffer(writeTap.getPos());
    new Uint8Array(readBuffer).set(
      new Uint8Array(buffer, 0, writeTap.getPos()),
    );
    const readTap = new ReadableTap(readBuffer);

    const value = await type.read(readTap);
    assertEquals(value, "test");
  });

  // Tests for missing coverage lines 103-105: skip() method
  it("skip skips logical value in tap", async () => {
    const type = new TestLogicalType();
    const buffer = new ArrayBuffer(100);
    const writeTap = new WritableTap(buffer);
    await writeTap.writeString("test");
    await writeTap.writeString("skip");

    const readBuffer = new ArrayBuffer(writeTap.getPos());
    new Uint8Array(readBuffer).set(
      new Uint8Array(buffer, 0, writeTap.getPos()),
    );
    const readTap = new ReadableTap(readBuffer);

    await type.read(readTap); // read first value
    const initialPos = readTap.getPos();
    await type.skip(readTap); // skip second value
    assert(readTap.getPos() > initialPos);
  });

  // Tests for missing coverage lines 107-112: match() method
  it("match compares logical values in two taps", async () => {
    const type = new TestLogicalType();
    const buffer1 = new ArrayBuffer(100);
    const buffer2 = new ArrayBuffer(100);

    const tap1 = new WritableTap(buffer1);
    const tap2 = new WritableTap(buffer2);

    await tap1.writeString("test");
    await tap2.writeString("test");

    const readBuffer1 = new ArrayBuffer(tap1.getPos());
    const readBuffer2 = new ArrayBuffer(tap2.getPos());
    new Uint8Array(readBuffer1).set(
      new Uint8Array(buffer1, 0, tap1.getPos()),
    );
    new Uint8Array(readBuffer2).set(
      new Uint8Array(buffer2, 0, tap2.getPos()),
    );

    const readTap1 = new ReadableTap(readBuffer1);
    const readTap2 = new ReadableTap(readBuffer2);

    const matchResult = await type.match(readTap1, readTap2);
    assertEquals(typeof matchResult, "number");
  });

  // Tests for missing coverage lines 134-136: ensureValid error path
  it("toBuffer throws ValidationError when ensureValid fails", async () => {
    const type = new TestLogicalType();
    await assertRejects(async () => {
      // deno-lint-ignore no-explicit-any
      await type.toBuffer(123 as any); // invalid value, not a string
    }, ValidationError);
  });

  it("ensureValid does not throw when check succeeds", () => {
    const type = new TestLogicalType();
    // Access private method for testing success path
    const method = (type as unknown as {
      ensureValid: (value: unknown, path: string[]) => void;
    }).ensureValid;
    method.call(type, "valid string", []); // valid value, should not throw
  });

  // Tests for missing coverage lines 155-158: LogicalResolver read() method
  it("compare compares logical values correctly", () => {
    const type = new TestLogicalType();
    const result = type.compare("a", "b");
    assertEquals(typeof result, "number");
  });

  it("fromBuffer deserializes logical value from buffer", async () => {
    const type = new TestLogicalType();
    const buffer = await type.toBuffer("test");
    const value = await type.fromBuffer(buffer);
    assertEquals(value, "test");
  });

  it("getUnderlyingType returns the underlying type", () => {
    const type = new TestLogicalType();
    const underlying = type.getUnderlyingType();
    assertEquals(underlying.constructor.name, "StringType");
  });

  it("isValid calls check with errorHook option", () => {
    const type = new TestLogicalType();
    let called = false;
    const errorHook = () => {
      called = true;
    };
    type.isValid(123, { errorHook }); // invalid value
    assert(called);
  });

  it("LogicalResolver read() uses convertFromUnderlying", async () => {
    const reader = new TestLogicalType();
    const writer = new TestLogicalType();
    const resolver = reader.createResolver(writer);

    const buffer = new ArrayBuffer(100);
    const writeTap = new WritableTap(buffer);
    await writeTap.writeString("test");

    const readBuffer = new ArrayBuffer(writeTap.getPos());
    new Uint8Array(readBuffer).set(
      new Uint8Array(buffer, 0, writeTap.getPos()),
    );
    const readTap = new ReadableTap(readBuffer);

    const value = await resolver.read(readTap);
    assertEquals(value, "test");
  });
});

describe("NamedLogicalType", () => {
  // Tests for missing coverage lines 171-181: NamedLogicalType methods
  it("getFullName returns the full name", () => {
    const type = new TestNamedLogicalType();
    assertEquals(type.getFullName(), "test.namespace.TestNamed");
  });

  it("getNamespace returns the namespace", () => {
    const type = new TestNamedLogicalType();
    assertEquals(type.getNamespace(), "test.namespace");
  });

  it("getAliases returns the aliases", () => {
    const type = new TestNamedLogicalType();
    assertEquals(type.getAliases(), []); // No aliases in our test
  });
});

describe("withLogicalTypeJSON", () => {
  it("handles string underlying", () => {
    const result = withLogicalTypeJSON("string", "test");
    assertEquals(result, { type: "string", logicalType: "test" });
  });

  it("handles object underlying without extras", () => {
    const result = withLogicalTypeJSON({ type: "string" }, "test");
    assertEquals(result, { type: "string", logicalType: "test" });
  });

  it("handles object underlying", () => {
    const result = withLogicalTypeJSON({ type: "string", size: 10 }, "test", {
      extra: "value",
    });
    assertEquals(result, {
      type: "string",
      size: 10,
      logicalType: "test",
      extra: "value",
    });
  });

  // Tests for missing coverage line 195: withLogicalTypeJSON error path
  it("throws for unsupported underlying schema", () => {
    assertThrows(
      () => {
        withLogicalTypeJSON(null as never, "test");
      },
      Error,
      "Unsupported underlying schema for logical type serialization.",
    );

    assertThrows(
      () => {
        withLogicalTypeJSON(123 as never, "test");
      },
      Error,
      "Unsupported underlying schema for logical type serialization.",
    );

    assertThrows(
      () => {
        withLogicalTypeJSON(["array"] as never, "test");
      },
      Error,
      "Unsupported underlying schema for logical type serialization.",
    );
  });

  describe("sync helpers", () => {
    const type = new TestLogicalType();

    it("round-trips via sync buffer", () => {
      const buffer = type.toSyncBuffer("sync");
      assertEquals(type.fromSyncBuffer(buffer), "sync");
    });

    it("writes, reads, and skips via sync taps", () => {
      const buffer = new ArrayBuffer(64);
      const writeTap = new SyncWritableTap(buffer);
      type.writeSync(writeTap, "sync");
      const readTap = new SyncReadableTap(buffer);
      assertEquals(type.readSync(readTap), "sync");
      assertEquals(readTap.getPos(), writeTap.getPos());

      const skipTap = new SyncReadableTap(buffer);
      type.skipSync(skipTap);
      assertEquals(skipTap.getPos(), writeTap.getPos());
    });

    it("matches buffers via matchSync", () => {
      const bufA = type.toSyncBuffer("a");
      const bufB = type.toSyncBuffer("b");
      assertEquals(
        type.matchSync(new SyncReadableTap(bufA), new SyncReadableTap(bufB)),
        -1,
      );
      assertEquals(
        type.matchSync(new SyncReadableTap(bufA), new SyncReadableTap(bufA)),
        0,
      );
    });

    it("resolver.readSync returns logical values", () => {
      const resolver = type.createResolver(type);
      const buffer = type.toSyncBuffer("resolved");
      assertEquals(resolver.readSync(new SyncReadableTap(buffer)), "resolved");
    });
  });
});
