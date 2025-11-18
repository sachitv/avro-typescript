import { assert, assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

import { createType } from "../create_type.ts";
import { EnumType } from "../../schemas/complex/enum_type.ts";

describe("createType Reserved Fields Tests", () => {
  it("should fail to create type from schema with reserved keywords", () => {
    const reservedSchema = {
      name: "org.apache.avro.test.Reserved",
      type: "enum",
      symbols: ["default", "class", "int"],
    };

    const reservedType = createType(reservedSchema);
    assert(reservedType instanceof EnumType);
    const symbols = (reservedType as EnumType).getSymbols();
    assertEquals(symbols.sort(), ["default", "class", "int"].sort());
  });
});
