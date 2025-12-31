import type { Type } from "../type.ts";
import type {
  CompiledSyncWriter,
  CompiledWriter,
  RecordWriterContext,
  RecordWriterStrategy,
} from "./record_writer_strategy.ts";
import { defaultWriterStrategy } from "./record_writer_strategy.ts";

/**
 * Cache for compiled record writers with support for recursive types.
 *
 * This class manages the four cache slots (strict/unchecked × sync/async)
 * and implements the placeholder pattern that allows recursive record types
 * to compile without infinite recursion.
 */
export class RecordWriterCache {
  #compiledSyncWriterStrict?: CompiledSyncWriter;
  #compiledSyncWriterUnchecked?: CompiledSyncWriter;
  #compiledWriterStrict?: CompiledWriter;
  #compiledWriterUnchecked?: CompiledWriter;
  #strategy: RecordWriterStrategy;

  /**
   * Creates a new RecordWriterCache.
   * @param strategy The writer strategy to use for compilation.
   */
  constructor(strategy: RecordWriterStrategy = defaultWriterStrategy) {
    this.#strategy = strategy;
  }

  /**
   * Gets the writer strategy.
   */
  public getStrategy(): RecordWriterStrategy {
    return this.#strategy;
  }

  /**
   * Clears all cached writers.
   * Call this when the record's fields change.
   */
  public clear(): void {
    this.#compiledSyncWriterStrict = undefined;
    this.#compiledSyncWriterUnchecked = undefined;
    this.#compiledWriterStrict = undefined;
    this.#compiledWriterUnchecked = undefined;
  }

  /**
   * Gets or creates a compiled async writer.
   *
   * @param validate Whether to validate values during writes.
   * @param context The record writer context.
   * @param getNestedWriter Callback to get writers for nested RecordTypes.
   * @returns The compiled writer function.
   */
  public getOrCreateWriter(
    validate: boolean,
    context: RecordWriterContext,
    getNestedWriter: (type: Type, validate: boolean) => CompiledWriter,
  ): CompiledWriter {
    const cached = validate
      ? this.#compiledWriterStrict
      : this.#compiledWriterUnchecked;
    if (cached) {
      return cached;
    }

    // Support recursive record types by installing a placeholder before walking
    // field types; recursive references will see this function and avoid
    // infinite recursion during compilation. The impl array allows the
    // placeholder to delegate to the actual writer once it's created.
    const impl: [CompiledWriter | null] = [null];
    const placeholder: CompiledWriter = (tap, value) => impl[0]!(tap, value);
    if (validate) {
      this.#compiledWriterStrict = placeholder;
    } else {
      this.#compiledWriterUnchecked = placeholder;
    }

    // Compile field writers using the strategy
    const fieldCount = context.fieldTypes.length;
    const fieldWriters = new Array<CompiledWriter>(fieldCount);
    for (let i = 0; i < fieldCount; i++) {
      const fieldType = context.fieldTypes[i]!;
      fieldWriters[i] = this.#strategy.compileFieldWriter(
        fieldType,
        validate,
        getNestedWriter,
      );
    }

    // Assemble the complete record writer
    const actualWriter = this.#strategy.assembleRecordWriter(
      context,
      fieldWriters,
    );

    if (validate) {
      this.#compiledWriterStrict = actualWriter;
    } else {
      this.#compiledWriterUnchecked = actualWriter;
    }
    impl[0] = actualWriter;
    return placeholder;
  }

  /**
   * Gets or creates a compiled sync writer.
   *
   * @param validate Whether to validate values during writes.
   * @param context The record writer context.
   * @param getNestedWriter Callback to get writers for nested RecordTypes.
   * @returns The compiled sync writer function.
   */
  public getOrCreateSyncWriter(
    validate: boolean,
    context: RecordWriterContext,
    getNestedWriter: (type: Type, validate: boolean) => CompiledSyncWriter,
  ): CompiledSyncWriter {
    const cached = validate
      ? this.#compiledSyncWriterStrict
      : this.#compiledSyncWriterUnchecked;
    if (cached) {
      return cached;
    }

    // Support recursive record types by installing a placeholder before walking
    // field types; recursive references will see this function and avoid
    // infinite recursion during compilation. The impl array allows the
    // placeholder to delegate to the actual writer once it's created.
    const impl: [CompiledSyncWriter | null] = [null];
    const placeholder: CompiledSyncWriter = (tap, value) =>
      impl[0]!(tap, value);
    if (validate) {
      this.#compiledSyncWriterStrict = placeholder;
    } else {
      this.#compiledSyncWriterUnchecked = placeholder;
    }

    // Compile field writers using the strategy
    const fieldCount = context.fieldTypes.length;
    const fieldWriters = new Array<CompiledSyncWriter>(fieldCount);
    for (let i = 0; i < fieldCount; i++) {
      const fieldType = context.fieldTypes[i]!;
      fieldWriters[i] = this.#strategy.compileSyncFieldWriter(
        fieldType,
        validate,
        getNestedWriter,
      );
    }

    // Assemble the complete record writer
    const actualWriter = this.#strategy.assembleSyncRecordWriter(
      context,
      fieldWriters,
    );

    impl[0] = actualWriter;
    if (validate) {
      this.#compiledSyncWriterStrict = actualWriter;
    } else {
      this.#compiledSyncWriterUnchecked = actualWriter;
    }
    return placeholder;
  }
}
