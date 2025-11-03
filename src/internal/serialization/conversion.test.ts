import { describe, it } from "@std/testing/bdd";
import { expect } from "@std/expect";

import { bigIntToSafeNumber } from "./conversion.ts";

describe("conversion", () => {
  it("bigIntToSafeNumber returns safe values", () => {
    const value = BigInt(Number.MAX_SAFE_INTEGER);
    expect(bigIntToSafeNumber(value, "test")).toBe(Number.MAX_SAFE_INTEGER);
  });

  it("bigIntToSafeNumber throws on overflow", () => {
    const value = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    expect(() => bigIntToSafeNumber(value, "overflow")).toThrow(RangeError);
  });
});
