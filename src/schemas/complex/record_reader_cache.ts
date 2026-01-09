import type { Type } from "../type.ts";
import type {
  CompiledReader,
  CompiledSyncReader,
  RecordReaderContext,
  RecordReaderStrategy,
} from "./record_reader_strategy.ts";
import { defaultReaderStrategy } from "./record_reader_strategy.ts";

/**
 * Cache for compiled record readers with support for recursive types.
 *
 * This class manages the cache slots (sync/async) and implements the placeholder
 * pattern that allows recursive record types to compile without infinite recursion.
 */
export class RecordReaderCache {
  #compiledSyncReader?: CompiledSyncReader;
  #compiledReader?: CompiledReader;
  #strategy: RecordReaderStrategy;

  constructor(strategy: RecordReaderStrategy = defaultReaderStrategy) {
    this.#strategy = strategy;
  }

  public getStrategy(): RecordReaderStrategy {
    return this.#strategy;
  }

  public clear(): void {
    this.#compiledSyncReader = undefined;
    this.#compiledReader = undefined;
  }

  public getOrCreateReader(
    context: RecordReaderContext,
    getNestedReader: (type: Type) => CompiledReader,
  ): CompiledReader {
    if (this.#compiledReader) {
      return this.#compiledReader;
    }

    // Support recursive record types by installing a placeholder before walking
    // field types; recursive references will see this function and avoid
    // infinite recursion during compilation.
    const impl: [CompiledReader | null] = [null];
    const placeholder: CompiledReader = (tap) => impl[0]!(tap);
    this.#compiledReader = placeholder;

    const fieldCount = context.fieldTypes.length;
    const fieldReaders = new Array<CompiledReader>(fieldCount);
    for (let i = 0; i < fieldCount; i++) {
      const fieldType = context.fieldTypes[i]!;
      fieldReaders[i] = this.#strategy.compileFieldReader(
        fieldType,
        getNestedReader,
      );
    }

    const actualReader = this.#strategy.assembleRecordReader(
      context,
      fieldReaders,
    );

    this.#compiledReader = actualReader;
    impl[0] = actualReader;
    return placeholder;
  }

  public getOrCreateSyncReader(
    context: RecordReaderContext,
    getNestedReader: (type: Type) => CompiledSyncReader,
  ): CompiledSyncReader {
    if (this.#compiledSyncReader) {
      return this.#compiledSyncReader;
    }

    // Support recursive record types by installing a placeholder before walking
    // field types; recursive references will see this function and avoid
    // infinite recursion during compilation.
    const impl: [CompiledSyncReader | null] = [null];
    const placeholder: CompiledSyncReader = (tap) => impl[0]!(tap);
    this.#compiledSyncReader = placeholder;

    const fieldCount = context.fieldTypes.length;
    const fieldReaders = new Array<CompiledSyncReader>(fieldCount);
    for (let i = 0; i < fieldCount; i++) {
      const fieldType = context.fieldTypes[i]!;
      fieldReaders[i] = this.#strategy.compileSyncFieldReader(
        fieldType,
        getNestedReader,
      );
    }

    const actualReader = this.#strategy.assembleSyncRecordReader(
      context,
      fieldReaders,
    );

    impl[0] = actualReader;
    this.#compiledSyncReader = actualReader;
    return placeholder;
  }
}
