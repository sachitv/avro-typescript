import { assert, assertEquals, assertFalse, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { isNeedMore, type NeedMore, needMore } from "../need_more.ts";

describe("needMore", () => {
  it("creates a NeedMore result with the given lower bound", () => {
    const result = needMore(12);
    assertEquals(result.needMore, true);
    assertEquals(result.minBytes, 12);
  });

  it("omits minBytes entirely when the bound is unknown", () => {
    const result = needMore();
    assertEquals(result, { needMore: true });
    assertFalse("minBytes" in result);
  });

  it("rejects bounds that are not positive safe integers", () => {
    for (const minBytes of [0, -1, 1.5, NaN, Infinity, 2 ** 53]) {
      assertThrows(
        () => needMore(minBytes),
        RangeError,
        `minBytes must be a positive safe integer, got ${minBytes}`,
      );
    }
  });

  it("cannot be built from a literal with an unchecked minBytes", () => {
    // @ts-expect-error minBytes must come from needMore(), which rejects 0.
    const zero: NeedMore = { needMore: true, minBytes: 0 };
    // @ts-expect-error the same applies to any plain number.
    const four: NeedMore = { needMore: true, minBytes: 4 };
    const unknown: NeedMore = { needMore: true };
    assertEquals([zero.needMore, four.needMore, unknown.needMore], [
      true,
      true,
      true,
    ]);
  });
});

describe("isNeedMore", () => {
  it("recognizes NeedMore results", () => {
    assert(isNeedMore(needMore()));
    assert(isNeedMore(needMore(4)));
  });

  it("rejects parsed values", () => {
    assertFalse(isNeedMore({ count: 1, byteLength: 2, headLength: 2 }));
    assertFalse(isNeedMore({}));
  });

  it("requires the discriminant to be exactly true", () => {
    assertFalse(
      isNeedMore({ needMore: 1, minBytes: 4 } as unknown as object),
    );
  });
});
