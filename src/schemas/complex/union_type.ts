import { BaseType } from "../base_type.ts";
import { NamedType } from "./named_type.ts";
import { Resolver } from "../resolver.ts";
import { type JSONType, Type } from "../type.ts";
import { type ErrorHook, throwInvalidError } from "../error.ts";
import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../../serialization/tap.ts";
import {
  type SyncReadableTapLike,
  SyncWritableTap,
  type SyncWritableTapLike,
} from "../../serialization/sync_tap.ts";
import { bigIntToSafeNumber } from "../../serialization/conversion.ts";
import { calculateVarintSize } from "../../internal/varint.ts";

/**
 * Represents a wrapped value for a union type.
 */
export type UnionWrappedValue = Record<string, unknown>;
/**
 * Represents a value for a union type, which can be a wrapped value or null.
 */
export type UnionValue = UnionWrappedValue | null;

/**
 * Parameters for creating a UnionType.
 */
export interface UnionTypeParams {
  /** The types that make up the union. */
  types: Type[];
  /** Whether to validate during writes. Defaults to true. */
  validate?: boolean;
}

interface BranchInfo {
  readonly type: Type;
  readonly name: string;
  readonly isNull: boolean;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null &&
    !Array.isArray(value);
}

/** @internal */
/**
 * Gets the branch name for a given type.
 * @param type The type to get the name for.
 * @returns The branch name.
 * @internal
 */
export function getBranchTypeName(type: Type): string {
  if (type instanceof NamedType) {
    return type.getFullName();
  }

  const schema = type.toJSON();
  if (typeof schema === "string") {
    return schema;
  }

  if (schema && typeof schema === "object") {
    const maybeType = (schema as Record<string, unknown>).type;
    if (typeof maybeType === "string") {
      return maybeType;
    }
  }

  throw new Error("Unable to determine union branch name.");
}

/**
 * Avro `union` type that supports values conforming to one of multiple schemas.
 */
export class UnionType extends BaseType<UnionValue> {
  readonly #types: Type[];
  readonly #branches: BranchInfo[];
  readonly #indices: Map<string, number>;
  readonly #validate: boolean;

  /**
   * Creates a new UnionType.
   * @param params The union type parameters.
   */
  constructor(params: UnionTypeParams) {
    super();

    const { types } = params;
    if (!Array.isArray(types)) {
      throw new Error("UnionType requires an array of branch types.");
    }
    if (types.length === 0) {
      throw new Error("UnionType requires at least one branch type.");
    }

    this.#types = [];
    this.#branches = [];
    this.#indices = new Map();
    this.#validate = params.validate ?? true;

    types.forEach((type, index) => {
      if (!(type instanceof Type)) {
        throw new Error("UnionType branches must be Avro types.");
      }
      if (type instanceof UnionType) {
        throw new Error("Unions cannot be directly nested.");
      }
      const name = getBranchTypeName(type);
      if (this.#indices.has(name)) {
        throw new Error(`Duplicate union branch of type name: ${name}`);
      }

      const branch: BranchInfo = {
        type,
        name,
        isNull: name === "null",
      };

      this.#types.push(type);
      this.#branches.push(branch);
      this.#indices.set(name, index);
    });
  }

  /**
   * Gets the types in this union.
   */
  public getTypes(): Type[] {
    return this.#types.slice();
  }

  /**
   * Checks if the given value conforms to this union type.
   * For null values, verifies if a null branch exists.
   * For wrapped values, ensures it's an object with a single key matching a branch name,
   * and validates the inner value against the corresponding branch type.
   * @param value The value to check.
   * @param errorHook Optional error hook for validation errors.
   * @param path The current path for error reporting.
   * @returns True if the value is valid, false otherwise.
   */
  public override check(
    value: unknown,
    errorHook?: ErrorHook,
    path: string[] = [],
  ): boolean {
    if (value === null) {
      const hasNull = this.#indices.has("null");
      if (!hasNull && errorHook) {
        errorHook(path.slice(), value, this);
      }
      return hasNull;
    }

    if (!isPlainObject(value)) {
      if (errorHook) {
        errorHook(path.slice(), value, this);
      }
      return false;
    }

    const keys = Object.keys(value);
    if (keys.length !== 1) {
      if (errorHook) {
        errorHook(path.slice(), value, this);
      }
      return false;
    }

    const key = keys[0];

    // Handle the case where the key itself is "null", this should never validate.
    if (key === "null") {
      if (errorHook) {
        errorHook(path.slice(), value, this);
      }
      return false;
    }

    const branchIndex = this.#indices.get(key);
    if (branchIndex === undefined) {
      if (errorHook) {
        errorHook(path.slice(), value, this);
      }
      return false;
    }

    const branch = this.#branches[branchIndex];
    const branchValue = value[key];
    const branchPath = errorHook ? [...path, key] : undefined;
    const isValid = branch.type.check(branchValue, errorHook, branchPath);

    if (!isValid && errorHook) {
      // Underlying type already invoked errorHook with extended path.
      return false;
    }

    return isValid;
  }

  /**
   * Serializes the union value to the writable tap.
   * Writes the branch index as a long, followed by the branch value if not null.
   * @param tap The writable tap to write to.
   * @param value The union value to serialize.
   */
  public override async write(
    tap: WritableTapLike,
    value: UnionValue,
  ): Promise<void> {
    if (!this.#validate) {
      await this.writeUnchecked(tap, value);
      return;
    }
    const { index, branchValue } = this.#resolveBranch(value as UnionValue);
    await tap.writeLong(BigInt(index));
    if (branchValue !== undefined) {
      await this.#branches[index].type.write(tap, branchValue as never);
    }
  }

  /**
   * Writes the union branch index and payload through sync taps.
   */
  public override writeSync(
    tap: SyncWritableTapLike,
    value: UnionValue,
  ): void {
    if (!this.#validate) {
      this.writeSyncUnchecked(tap, value);
      return;
    }
    const { index, branchValue } = this.#resolveBranch(value);
    tap.writeLong(BigInt(index));
    if (branchValue !== undefined) {
      this.#branches[index].type.writeSync(tap, branchValue as never);
    }
  }

  /**
   * Writes union value without validation.
   */
  public override async writeUnchecked(
    tap: WritableTapLike,
    value: UnionValue,
  ): Promise<void> {
    const { index, branchValue } = this.#resolveBranch(value);
    await tap.writeLong(BigInt(index));
    if (branchValue !== undefined) {
      await this.#branches[index].type.writeUnchecked(
        tap,
        branchValue as never,
      );
    }
  }

  /**
   * Writes union value without validation synchronously.
   */
  public override writeSyncUnchecked(
    tap: SyncWritableTapLike,
    value: UnionValue,
  ): void {
    const { index, branchValue } = this.#resolveBranch(value);
    tap.writeLong(BigInt(index));
    if (branchValue !== undefined) {
      this.#branches[index].type.writeSyncUnchecked(
        tap,
        branchValue as never,
      );
    }
  }

  /**
   * Deserializes a union value from the readable tap.
   * Reads the branch index, and if not null, reads the branch value and wraps it in an object.
   * @param tap The readable tap to read from.
   * @returns The deserialized union value.
   */
  public override async read(tap: ReadableTapLike): Promise<UnionValue> {
    const index = await this.#readBranchIndex(tap);
    const branch = this.#branches[index];
    if (branch.isNull) {
      return null;
    }
    const branchValue = await branch.type.read(tap);
    return { [branch.name]: branchValue };
  }

  /**
   * Reads a union branch index and its payload synchronously.
   */
  /**
   * Delegates synchronous read to the branch resolver.
   */
  public override readSync(tap: SyncReadableTapLike): UnionValue {
    const index = this.#readBranchIndexSync(tap);
    const branch = this.#branches[index];
    if (branch.isNull) {
      return null;
    }
    const branchValue = branch.type.readSync(tap);
    return { [branch.name]: branchValue };
  }

  /**
   * Skips over a union value in the readable tap.
   * Reads the branch index and skips the branch value if not null.
   * @param tap The readable tap to skip in.
   */
  public override async skip(tap: ReadableTapLike): Promise<void> {
    const index = await this.#readBranchIndex(tap);
    const branch = this.#branches[index];
    if (!branch.isNull) {
      await branch.type.skip(tap);
    }
  }

  /**
   * Skips a union value by skipping its branch payload synchronously.
   */
  public override skipSync(tap: SyncReadableTapLike): void {
    const index = this.#readBranchIndexSync(tap);
    const branch = this.#branches[index];
    if (!branch.isNull) {
      branch.type.skipSync(tap);
    }
  }

  /**
   * Converts the union value to an ArrayBuffer.
   * Calculates the size needed for the index and branch value, then writes them to a new buffer.
   * @param value The union value to convert.
   * @returns The ArrayBuffer containing the serialized value.
   */
  public override async toBuffer(value: UnionValue): Promise<ArrayBuffer> {
    const { index, branchValue } = this.#resolveBranch(value);
    const indexSize = calculateVarintSize(index);
    let totalSize = indexSize;
    let branchBytes: Uint8Array | undefined;
    if (branchValue !== undefined) {
      branchBytes = new Uint8Array(
        await this.#branches[index].type.toBuffer(branchValue),
      );
      totalSize += branchBytes.byteLength;
    }

    const buffer = new ArrayBuffer(totalSize);
    const tap = new WritableTap(buffer);
    await tap.writeLong(BigInt(index));
    if (branchBytes) {
      await tap.writeFixed(branchBytes);
    }
    return buffer;
  }

  /**
   * Encodes a union value synchronously into an ArrayBuffer.
   */
  public override toSyncBuffer(value: UnionValue): ArrayBuffer {
    const { index, branchValue } = this.#resolveBranch(value);
    const indexSize = calculateVarintSize(index);
    let totalSize = indexSize;
    let branchBytes: Uint8Array | undefined;
    if (branchValue !== undefined) {
      branchBytes = new Uint8Array(
        this.#branches[index].type.toSyncBuffer(branchValue),
      );
      totalSize += branchBytes.byteLength;
    }

    const buffer = new ArrayBuffer(totalSize);
    const tap = new SyncWritableTap(buffer);
    tap.writeLong(BigInt(index));
    if (branchBytes) {
      tap.writeFixed(branchBytes);
    }
    return buffer;
  }

  /**
   * Creates a deep clone of the given value as a union value.
   * Handles null values and wrapped objects by cloning the inner value using the branch type.
   * @param value The value to clone.
   * @returns The cloned union value.
   */
  public override cloneFromValue(value: unknown): UnionValue {
    if (value === null) {
      if (!this.#indices.has("null")) {
        throw new Error("Cannot clone null for a union without null branch.");
      }
      return null;
    }

    const { index, branchValue } = this.#resolveBranch(value);
    if (branchValue === undefined) {
      return null;
    }

    const cloned = this.#branches[index].type.cloneFromValue(branchValue);
    return { [this.#branches[index].name]: cloned };
  }

  /**
   * Compares two union values for ordering.
   * First compares by branch index; if equal, compares the branch values using the branch type's compare method.
   * @param val1 The first union value.
   * @param val2 The second union value.
   * @returns Negative if val1 < val2, zero if equal, positive if val1 > val2.
   */
  public override compare(val1: UnionValue, val2: UnionValue): number {
    const branch1 = this.#resolveBranch(val1);
    const branch2 = this.#resolveBranch(val2);

    if (branch1.index === branch2.index) {
      if (branch1.branchValue === undefined) {
        return 0;
      }
      return this.#branches[branch1.index].type.compare(
        branch1.branchValue,
        branch2.branchValue as unknown,
      );
    }

    return branch1.index < branch2.index ? -1 : 1;
  }

  /**
   * Generates a random union value.
   * Selects a random branch and generates a random value for that branch, wrapping it appropriately.
   * @returns A random union value.
   */
  public override random(): UnionValue {
    const index = Math.floor(Math.random() * this.#branches.length);
    const branch = this.#branches[index];
    if (branch.isNull) {
      return null;
    }
    const value = branch.type.random();
    return { [branch.name]: value };
  }

  /**
   * Returns the JSON representation of the union type.
   * An array of the JSON representations of each branch type.
   * @returns The JSON type array.
   */
  public override toJSON(): JSONType {
    return this.#types.map((type) => type.toJSON());
  }

  /**
   * Compares two serialized union values from taps for ordering.
   * Reads the branch indices from both taps; if equal, compares the branch values using the branch type's match method.
   * @param tap1 The first readable tap.
   * @param tap2 The second readable tap.
   * @returns Negative if tap1 < tap2, zero if equal, positive if tap1 > tap2.
   */
  public override async match(
    tap1: ReadableTapLike,
    tap2: ReadableTapLike,
  ): Promise<number> {
    const idx1 = await this.#readBranchIndex(tap1);
    const idx2 = await this.#readBranchIndex(tap2);
    if (idx1 === idx2) {
      const branch = this.#branches[idx1];
      if (branch.isNull) {
        return 0;
      }
      return await branch.type.match(tap1, tap2);
    }
    return idx1 < idx2 ? -1 : 1;
  }

  /**
   * Compares union-encoded buffers synchronously.
   */
  public override matchSync(
    tap1: SyncReadableTapLike,
    tap2: SyncReadableTapLike,
  ): number {
    const idx1 = this.#readBranchIndexSync(tap1);
    const idx2 = this.#readBranchIndexSync(tap2);
    if (idx1 === idx2) {
      const branch = this.#branches[idx1];
      if (branch.isNull) {
        return 0;
      }
      return branch.type.matchSync(tap1, tap2);
    }
    return idx1 < idx2 ? -1 : 1;
  }

  /**
   * Creates a resolver for schema evolution from the writer type to this union type.
   * If the writer is also a union, creates resolvers for each branch.
   * Otherwise, finds a compatible branch in this union and creates a resolver for it.
   * @param writerType The writer type to resolve from.
   * @returns A resolver for the union value.
   */
  public override createResolver(writerType: Type): Resolver<UnionValue> {
    if (writerType instanceof UnionType) {
      const branchResolvers = writerType.getTypes().map((branch) =>
        this.createResolver(branch)
      );
      return new UnionFromUnionResolver(this, branchResolvers);
    }

    for (let i = 0; i < this.#branches.length; i++) {
      const branch = this.#branches[i];
      try {
        const resolver = branch.type.createResolver(writerType);
        return new UnionBranchResolver(this, branch, resolver);
      } catch {
        // Continue searching for compatible branch.
      }
    }

    return super.createResolver(writerType) as Resolver<UnionValue>;
  }

  #resolveBranch(
    value: unknown,
  ): { index: number; branchValue: unknown | undefined } {
    if (value === null) {
      const index = this.#indices.get("null");
      if (index === undefined) {
        throwInvalidError([], value, this);
      }
      return { index, branchValue: undefined };
    }

    if (!isPlainObject(value)) {
      throwInvalidError([], value, this);
    }

    const keys = Object.keys(value);
    if (keys.length !== 1) {
      throwInvalidError([], value, this);
    }

    const key = keys[0];
    const branchIndex = this.#indices.get(key);
    if (branchIndex === undefined) {
      throwInvalidError([], value, this);
    }

    const branch = this.#branches[branchIndex];
    const branchValue = value[key];

    if (!branch.isNull && branchValue === undefined) {
      throwInvalidError([], value, this);
    }

    if (branch.isNull && branchValue !== undefined) {
      throwInvalidError([], value, this);
    }

    return { index: branchIndex, branchValue };
  }

  async #readBranchIndex(tap: ReadableTapLike): Promise<number> {
    const indexBigInt = await tap.readLong();
    const index = bigIntToSafeNumber(indexBigInt, "Union branch index");
    if (index < 0 || index >= this.#branches.length) {
      throw new Error(`Invalid union index: ${index}`);
    }
    return index;
  }

  /**
   * Decodes a branch index from a sync tap with validation.
   */
  #readBranchIndexSync(tap: SyncReadableTapLike): number {
    const indexBigInt = tap.readLong();
    const index = bigIntToSafeNumber(indexBigInt, "Union branch index");
    if (index < 0 || index >= this.#branches.length) {
      throw new Error(`Invalid union index: ${index}`);
    }
    return index;
  }
}

class UnionBranchResolver extends Resolver<UnionValue> {
  readonly #branch: BranchInfo;
  readonly #branchResolver: Resolver<unknown>;

  constructor(
    reader: UnionType,
    branch: BranchInfo,
    branchResolver: Resolver<unknown>,
  ) {
    super(reader);
    this.#branch = branch;
    this.#branchResolver = branchResolver;
  }

  public override async read(
    tap: ReadableTapLike,
  ): Promise<UnionValue> {
    const resolvedValue = await this.#branchResolver.read(tap);
    if (this.#branch.isNull) {
      return null;
    }
    return { [this.#branch.name]: resolvedValue };
  }

  /**
   * Reads the branch index synchronously and forwards to the per-branch resolver.
   */
  public override readSync(tap: SyncReadableTapLike): UnionValue {
    const resolvedValue = this.#branchResolver.readSync(tap);
    if (this.#branch.isNull) {
      return null;
    }
    return { [this.#branch.name]: resolvedValue };
  }
}

class UnionFromUnionResolver extends Resolver<UnionValue> {
  readonly #resolvers: Resolver<UnionValue>[];

  constructor(reader: UnionType, resolvers: Resolver<UnionValue>[]) {
    super(reader);
    this.#resolvers = resolvers;
  }

  public override async read(
    tap: ReadableTapLike,
  ): Promise<UnionValue> {
    const indexBigInt = await tap.readLong();
    const index = bigIntToSafeNumber(indexBigInt, "Union branch index");
    const resolver = this.#resolvers[index];
    if (!resolver) {
      throw new Error(`Invalid union index: ${index}`);
    }
    return await resolver.read(tap);
  }

  public override readSync(tap: SyncReadableTapLike): UnionValue {
    const indexBigInt = tap.readLong();
    const index = bigIntToSafeNumber(indexBigInt, "Union branch index");
    const resolver = this.#resolvers[index];
    if (!resolver) {
      throw new Error(`Invalid union index: ${index}`);
    }
    return resolver.readSync(tap);
  }
}
