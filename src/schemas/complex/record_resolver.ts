import type { ReadableTapLike } from "../../serialization/tap.ts";
import type { SyncReadableTapLike } from "../../serialization/sync_tap.ts";
import { Resolver } from "../resolver.ts";
import type { RecordField } from "./record_field.ts";
import type { RecordType } from "./record_type.ts";

/**
 * Mapping between writer and reader fields during schema evolution.
 */
export interface FieldMapping {
  /** Index in the reader's field array, or -1 if the field should be skipped. */
  readerIndex: number;
  /** The writer field being mapped. */
  writerField: RecordField;
  /** Optional resolver for type promotion between writer and reader field types. */
  resolver?: Resolver;
}

/**
 * Resolver for record schema evolution.
 *
 * Handles reading data written with a writer schema into a reader schema,
 * mapping fields by name/alias, skipping removed fields, and applying
 * default values for new fields.
 */
export class RecordResolver extends Resolver<Record<string, unknown>> {
  #mappings: FieldMapping[];
  #readerFields: RecordField[];

  /**
   * Creates a new RecordResolver.
   * @param reader The reader record type.
   * @param mappings Field mappings from writer to reader.
   * @param readerFields The reader's field definitions.
   */
  constructor(
    reader: RecordType,
    mappings: FieldMapping[],
    readerFields: RecordField[],
  ) {
    super(reader);
    this.#mappings = mappings;
    this.#readerFields = readerFields;
  }

  /**
   * Reads a record from the tap using the resolution mappings.
   * @param tap The tap to read from.
   * @returns The resolved record value.
   */
  public override async read(
    tap: ReadableTapLike,
  ): Promise<Record<string, unknown>> {
    const result: Record<string, unknown> = {};
    const seen = new Array(this.#readerFields.length).fill(false);

    for (const mapping of this.#mappings) {
      if (mapping.readerIndex === -1) {
        await mapping.writerField.getType().skip(tap);
        continue;
      }

      const value = mapping.resolver
        ? await mapping.resolver.read(tap)
        : await mapping.writerField.getType().read(tap);

      const readerField = this.#readerFields[mapping.readerIndex];
      result[readerField.getName()] = value;
      seen[mapping.readerIndex] = true;
    }

    for (let i = 0; i < this.#readerFields.length; i++) {
      if (!seen[i]) {
        const field = this.#readerFields[i];
        result[field.getName()] = field.getDefault();
      }
    }

    return result;
  }

  /**
   * Resolves incoming data synchronously, applying missing defaults as needed.
   * @param tap The tap to read from.
   * @returns The resolved record value.
   */
  public override readSync(
    tap: SyncReadableTapLike,
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    const seen = new Array(this.#readerFields.length).fill(false);

    for (const mapping of this.#mappings) {
      if (mapping.readerIndex === -1) {
        mapping.writerField.getType().skipSync(tap);
        continue;
      }

      const value = mapping.resolver
        ? mapping.resolver.readSync(tap)
        : mapping.writerField.getType().readSync(tap);

      const readerField = this.#readerFields[mapping.readerIndex];
      result[readerField.getName()] = value;
      seen[mapping.readerIndex] = true;
    }

    for (let i = 0; i < this.#readerFields.length; i++) {
      if (!seen[i]) {
        const field = this.#readerFields[i];
        result[field.getName()] = field.getDefault();
      }
    }

    return result;
  }
}
