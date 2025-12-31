import { RecordType } from "../record_type.ts";
import { resolveNames } from "../resolve_names.ts";
import type { Type } from "../../type.ts";
import type { RecordWriterStrategy } from "../record_writer_strategy.ts";

/**
 * Specification for a record field in tests.
 */
export interface FieldSpec {
  name: string;
  type: Type;
  aliases?: string[];
  order?: "ascending" | "descending" | "ignore";
  default?: unknown;
}

/**
 * Parameters for creating a test record.
 */
export interface CreateRecordParams {
  name: string;
  namespace?: string;
  aliases?: string[];
  fields: FieldSpec[];
  validate?: boolean;
  writerStrategy?: RecordWriterStrategy;
}

/**
 * Helper function to create a RecordType for testing.
 *
 * @param params The record parameters.
 * @returns A new RecordType instance.
 */
export function createRecord(params: CreateRecordParams): RecordType {
  const { fields, validate, writerStrategy, ...names } = params;
  const resolved = resolveNames(names);
  return new RecordType({
    ...resolved,
    fields,
    validate,
    writerStrategy,
  });
}
