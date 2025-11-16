import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { randomSeeded } from "@std/random";

import { createType } from "./mod.ts";

const RECORD_SCHEMA_URL = new URL(
  "../../../test-data/schemas/RecordWithRequiredFields.avsc",
  import.meta.url,
);
const RECORD_SCHEMA = JSON.parse(await Deno.readTextFile(RECORD_SCHEMA_URL));
const recordType = createType(RECORD_SCHEMA);

const UNION_BRANCH_NAME = "org.apache.avro.UnionRecord" as const;
const ENUM_VALUES = ["A", "B"] as const;
const RANDOM_SEED_BASE = 0xc0ffee1234n;
const ALPHABET = "abcdefghijklmnopqrstuvwxyz";

type MapRecordEnum = typeof ENUM_VALUES[number];
type MapRecordValue = {
  enumField: MapRecordEnum;
  strField: string;
};
type ArrayRecordValue = {
  strField: string;
  mapField: Map<string, MapRecordValue>;
};
type UnionRecordValue = {
  strField: string;
  arrayField: ArrayRecordValue[];
};
type UnionBranch = { [UNION_BRANCH_NAME]: UnionRecordValue };
type RecordWithUnion = {
  strField: string;
  unionField: null | UnionBranch;
};

interface RandomContext {
  randomInt(min: number, max: number): number;
  randomString(minLength: number, maxLength: number): string;
  pick<T>(values: readonly T[]): T;
}

const createRandomContext = (seed: bigint): RandomContext => {
  const rng = randomSeeded(seed);
  const random = () => rng();
  const randomInt = (min: number, max: number): number => {
    if (max <= min) {
      return min;
    }
    return Math.floor(random() * (max - min)) + min;
  };
  const randomString = (minLength: number, maxLength: number): string => {
    const length = randomInt(minLength, maxLength + 1);
    let value = "";
    for (let i = 0; i < length; i++) {
      value += ALPHABET[randomInt(0, ALPHABET.length)];
    }
    return value;
  };
  const pick = <T>(values: readonly T[]): T =>
    values[randomInt(0, values.length)];
  return { randomInt, randomString, pick };
};

const buildMapRecord = (ctx: RandomContext): MapRecordValue => ({
  enumField: ctx.pick(ENUM_VALUES),
  strField: ctx.randomString(3, 11),
});

const buildArrayRecord = (ctx: RandomContext): ArrayRecordValue => {
  const mapSize = ctx.randomInt(1, 5);
  const map = new Map<string, MapRecordValue>();
  for (let index = 0; index < mapSize; index++) {
    map.set(`${ctx.randomString(2, 5)}-${index}`, buildMapRecord(ctx));
  }
  return {
    strField: ctx.randomString(4, 12),
    mapField: map,
  };
};

const buildUnionRecord = (ctx: RandomContext): UnionRecordValue => ({
  strField: ctx.randomString(5, 14),
  arrayField: Array.from(
    { length: ctx.randomInt(1, 5) },
    () => buildArrayRecord(ctx),
  ),
});

const buildRecordWithUnion = (ctx: RandomContext): RecordWithUnion => ({
  strField: ctx.randomString(6, 15),
  unionField: { [UNION_BRANCH_NAME]: buildUnionRecord(ctx) },
});

const buildNullUnionRecord = (ctx: RandomContext): RecordWithUnion => ({
  strField: ctx.randomString(4, 12),
  unionField: null,
});

describe("createType deep union coverage", () => {
  it("round trips random nested union records with arrays and maps", async () => {
    const samples = [0n, 1n, 2n].map((offset) => {
      const context = createRandomContext(RANDOM_SEED_BASE + offset);
      return buildRecordWithUnion(context);
    });

    for (const sample of samples) {
      assertEquals(recordType.check(sample), true);
      const buffer = await recordType.toBuffer(sample);
      const decoded = await recordType.fromBuffer(buffer);
      assertEquals(decoded, sample);
    }
  });

  it("supports the explicit null union branch", async () => {
    const context = createRandomContext(RANDOM_SEED_BASE + 0x99n);
    const sample = buildNullUnionRecord(context);
    assertEquals(recordType.check(sample), true);

    const buffer = await recordType.toBuffer(sample);
    const decoded = await recordType.fromBuffer(buffer);
    assertEquals(decoded, sample);
  });
});
