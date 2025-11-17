import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import {
  DateLogicalType,
  LocalTimestampMicrosLogicalType,
  LocalTimestampMillisLogicalType,
  LocalTimestampNanosLogicalType,
  TimeMicrosLogicalType,
  TimeMillisLogicalType,
  TimestampMicrosLogicalType,
  TimestampMillisLogicalType,
  TimestampNanosLogicalType,
} from "../temporal_logical_types.ts";
import { IntType } from "../../primitive/int_type.ts";
import { LongType } from "../../primitive/long_type.ts";
import { ValidationError } from "../../error.ts";

describe("Temporal logical types", () => {
  describe("DateLogicalType", () => {
    const type = new DateLogicalType(new IntType());

    it("validates integer days", () => {
      assert(type.isValid(0));
      assert(!type.isValid(1.2));
    });

    it("round-trips values", async () => {
      const buffer = await type.toBuffer(1234);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, 1234);
    });

    it("can read from same logical type", () => {
      const sameType = new DateLogicalType(new IntType());
      // deno-lint-ignore no-explicit-any
      assert((type as any).canReadFromLogical(sameType));
    });

    it("cannot read from different logical type", () => {
      const timeType = new TimeMillisLogicalType(new IntType());
      // deno-lint-ignore no-explicit-any
      assert(!(type as any).canReadFromLogical(timeType));
    });

    it("generates random date values", () => {
      const randomValue = type.random();
      assert(typeof randomValue === "number");
      assert(Number.isInteger(randomValue));
    });

    it("returns correct JSON representation", () => {
      assertEquals(type.toJSON(), { type: "int", logicalType: "date" });
    });
  });

  describe("TimeMillisLogicalType", () => {
    const type = new TimeMillisLogicalType(new IntType());

    it("enforces millisecond range", () => {
      assert(type.isValid(0));
      assert(!type.isValid(86_400_000));
    });

    it("round-trips values", async () => {
      const buffer = await type.toBuffer(123_456);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, 123_456);
    });

    it("can read from same logical type", () => {
      const sameType = new TimeMillisLogicalType(new IntType());
      // deno-lint-ignore no-explicit-any
      assert((type as any).canReadFromLogical(sameType));
    });

    it("cannot read from different logical type", () => {
      const dateType = new DateLogicalType(new IntType());
      // deno-lint-ignore no-explicit-any
      assert(!(type as any).canReadFromLogical(dateType));
    });

    it("generates random time values", () => {
      const randomValue = type.random();
      assert(typeof randomValue === "number");
      assert(Number.isInteger(randomValue));
      assert(randomValue >= 0);
      assert(randomValue < 86_400_000);
    });

    it("returns correct JSON representation", () => {
      assertEquals(type.toJSON(), { type: "int", logicalType: "time-millis" });
    });
  });

  describe("TimeMicrosLogicalType", () => {
    const type = new TimeMicrosLogicalType(new LongType());

    it("requires bigint inputs", () => {
      assert(type.isValid(1000n));
      assert(!type.isValid(10));
    });

    it("round-trips bigint values", async () => {
      const buffer = await type.toBuffer(123_456n);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, 123_456n);
    });

    it("can read from same logical type", () => {
      const sameType = new TimeMicrosLogicalType(new LongType());
      // deno-lint-ignore no-explicit-any
      assert((type as any).canReadFromLogical(sameType));
    });

    it("cannot read from different logical type", () => {
      const timeMillisType = new TimeMillisLogicalType(new IntType());
      // deno-lint-ignore no-explicit-any
      assert(!(type as any).canReadFromLogical(timeMillisType));
    });

    it("generates random time values", () => {
      const randomValue = type.random();
      assert(typeof randomValue === "bigint");
      assert(randomValue >= 0n);
      assert(randomValue < 86_400_000_000n);
    });

    it("returns correct JSON representation", () => {
      assertEquals(type.toJSON(), { type: "long", logicalType: "time-micros" });
    });
  });

  describe("TimestampMillisLogicalType", () => {
    const type = new TimestampMillisLogicalType(new LongType());

    it("round-trips Date instances", async () => {
      const date = new Date("2024-01-01T12:00:00.000Z");
      const buffer = await type.toBuffer(date);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded.getTime(), date.getTime());
    });

    it("rejects invalid dates", async () => {
      await assertRejects(
        async () => {
          // deno-lint-ignore no-explicit-any
          await type.toBuffer(new Date(NaN) as any);
        },
        ValidationError,
      );
    });

    it("throws error for non-finite date time in toUnderlying", () => {
      const fakeDate = { getTime: () => Infinity } as Date;
      assertThrows(
        // deno-lint-ignore no-explicit-any
        () => (type as any).toUnderlying(fakeDate),
        Error,
        "Invalid Date value for timestamp-millis logical type.",
      );
    });

    it("can read from same logical type", () => {
      const sameType = new TimestampMillisLogicalType(new LongType());
      // deno-lint-ignore no-explicit-any
      assert((type as any).canReadFromLogical(sameType));
    });

    it("cannot read from different logical type", () => {
      const timeType = new TimeMillisLogicalType(new IntType());
      // deno-lint-ignore no-explicit-any
      assert(!(type as any).canReadFromLogical(timeType));
    });

    it("generates random timestamp values", () => {
      const randomValue = type.random();
      assert(randomValue instanceof Date);
      assert(!Number.isNaN(randomValue.getTime()));
    });

    it("compares timestamp values correctly", () => {
      const date1 = new Date("2024-01-01T12:00:00.000Z");
      const date2 = new Date("2024-01-01T12:00:01.000Z");
      const date3 = new Date("2024-01-01T12:00:00.000Z");

      assertEquals(type.compare(date1, date2), -1);
      assertEquals(type.compare(date2, date1), 1);
      assertEquals(type.compare(date1, date3), 0);
    });

    it("returns correct JSON representation", () => {
      assertEquals(type.toJSON(), {
        type: "long",
        logicalType: "timestamp-millis",
      });
    });
  });

  describe("TimestampMicrosLogicalType", () => {
    const type = new TimestampMicrosLogicalType(new LongType());

    it("round-trips bigint microseconds", async () => {
      const value = 1_700_000_000_123_456n;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("can read from same logical type", () => {
      const sameType = new TimestampMicrosLogicalType(new LongType());
      // deno-lint-ignore no-explicit-any
      assert((type as any).canReadFromLogical(sameType));
    });

    it("cannot read from different logical type", () => {
      const timestampMillisType = new TimestampMillisLogicalType(
        new LongType(),
      );
      // deno-lint-ignore no-explicit-any
      assert(!(type as any).canReadFromLogical(timestampMillisType));
    });

    it("generates random timestamp values", () => {
      const randomValue = type.random();
      assert(typeof randomValue === "bigint");
    });

    it("returns correct JSON representation", () => {
      assertEquals(type.toJSON(), {
        type: "long",
        logicalType: "timestamp-micros",
      });
    });
  });

  describe("TimestampNanosLogicalType", () => {
    const type = new TimestampNanosLogicalType(new LongType());

    it("round-trips bigint nanoseconds", async () => {
      const value = 1_700_000_000_123_456_789n;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("can read from same logical type", () => {
      const sameType = new TimestampNanosLogicalType(new LongType());
      // deno-lint-ignore no-explicit-any
      assert((type as any).canReadFromLogical(sameType));
    });

    it("cannot read from different logical type", () => {
      const timestampMicrosType = new TimestampMicrosLogicalType(
        new LongType(),
      );
      // deno-lint-ignore no-explicit-any
      assert(!(type as any).canReadFromLogical(timestampMicrosType));
    });

    it("generates random timestamp values", () => {
      const randomValue = type.random();
      assert(typeof randomValue === "bigint");
    });

    it("returns correct JSON representation", () => {
      assertEquals(type.toJSON(), {
        type: "long",
        logicalType: "timestamp-nanos",
      });
    });
  });

  describe("LocalTimestampMillisLogicalType", () => {
    const type = new LocalTimestampMillisLogicalType(new LongType());

    it("round-trips millisecond counts", async () => {
      const value = 1_700_000_000_000;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("can read from same logical type", () => {
      const sameType = new LocalTimestampMillisLogicalType(new LongType());
      // deno-lint-ignore no-explicit-any
      assert((type as any).canReadFromLogical(sameType));
    });

    it("cannot read from different logical type", () => {
      const timestampMillisType = new TimestampMillisLogicalType(
        new LongType(),
      );
      // deno-lint-ignore no-explicit-any
      assert(!(type as any).canReadFromLogical(timestampMillisType));
    });

    it("generates random timestamp values", () => {
      const randomValue = type.random();
      assert(typeof randomValue === "number");
      assert(Number.isInteger(randomValue));
    });

    it("returns correct JSON representation", () => {
      assertEquals(type.toJSON(), {
        type: "long",
        logicalType: "local-timestamp-millis",
      });
    });
  });

  describe("LocalTimestampMicrosLogicalType", () => {
    const type = new LocalTimestampMicrosLogicalType(new LongType());

    it("round-trips local microseconds", async () => {
      const value = 1_700_000_000_000_000n;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("can read from same logical type", () => {
      const sameType = new LocalTimestampMicrosLogicalType(new LongType());
      // deno-lint-ignore no-explicit-any
      assert((type as any).canReadFromLogical(sameType));
    });

    it("cannot read from different logical type", () => {
      const localTimestampMillisType = new LocalTimestampMillisLogicalType(
        new LongType(),
      );
      // deno-lint-ignore no-explicit-any
      assert(!(type as any).canReadFromLogical(localTimestampMillisType));
    });

    it("generates random timestamp values", () => {
      const randomValue = type.random();
      assert(typeof randomValue === "bigint");
    });

    it("returns correct JSON representation", () => {
      assertEquals(type.toJSON(), {
        type: "long",
        logicalType: "local-timestamp-micros",
      });
    });
  });

  describe("LocalTimestampNanosLogicalType", () => {
    const type = new LocalTimestampNanosLogicalType(new LongType());

    it("round-trips local nanoseconds", async () => {
      const value = 1_700_000_000_000_000_000n;
      const buffer = await type.toBuffer(value);
      const decoded = await type.fromBuffer(buffer);
      assertEquals(decoded, value);
    });

    it("can read from same logical type", () => {
      const sameType = new LocalTimestampNanosLogicalType(new LongType());
      // deno-lint-ignore no-explicit-any
      assert((type as any).canReadFromLogical(sameType));
    });

    it("cannot read from different logical type", () => {
      const localTimestampMicrosType = new LocalTimestampMicrosLogicalType(
        new LongType(),
      );
      // deno-lint-ignore no-explicit-any
      assert(!(type as any).canReadFromLogical(localTimestampMicrosType));
    });

    it("generates random timestamp values", () => {
      const randomValue = type.random();
      assert(typeof randomValue === "bigint");
    });

    it("returns correct JSON representation", () => {
      assertEquals(type.toJSON(), {
        type: "long",
        logicalType: "local-timestamp-nanos",
      });
    });
  });
});
