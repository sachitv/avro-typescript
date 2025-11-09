import { BaseType } from "./base_type.ts";
import { NamedType } from "./named_type.ts";
import { Resolver } from "./resolver.ts";
import { JSONType, Type } from "./type.ts";
import { ErrorHook, throwInvalidError } from "./error.ts";
import {
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "../serialization/tap.ts";
import { bigIntToSafeNumber } from "../serialization/conversion.ts";
import { calculateVarintSize } from "./varint.ts";

export type UnionWrappedValue = Record<string, unknown>;
export type UnionValue = UnionWrappedValue | null;

export interface UnionTypeParams {
  types: Type[];
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

export class UnionType extends BaseType<UnionValue> {
  readonly #types: Type[];
  readonly #branches: BranchInfo[];
  readonly #indices: Map<string, number>;

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

  public getTypes(): Type[] {
    return this.#types.slice();
  }

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

  public override async write(
    tap: WritableTapLike,
    value: UnionValue,
  ): Promise<void> {
    const { index, branchValue } = this.#resolveBranch(value);
    await tap.writeLong(BigInt(index));
    if (branchValue !== undefined) {
      await this.#branches[index].type.write(tap, branchValue);
    }
  }

  public override async read(tap: ReadableTapLike): Promise<UnionValue> {
    const index = await this.#readBranchIndex(tap);
    const branch = this.#branches[index];
    if (branch.isNull) {
      return null;
    }
    const branchValue = await branch.type.read(tap);
    return { [branch.name]: branchValue };
  }

  public override async skip(tap: ReadableTapLike): Promise<void> {
    const index = await this.#readBranchIndex(tap);
    const branch = this.#branches[index];
    if (!branch.isNull) {
      await branch.type.skip(tap);
    }
  }

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

  public override clone(
    value: UnionValue,
    opts?: Record<string, unknown>,
  ): UnionValue {
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

    const cloned = this.#branches[index].type.clone(branchValue, opts);
    return { [this.#branches[index].name]: cloned };
  }

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

  public override random(): UnionValue {
    const index = Math.floor(Math.random() * this.#branches.length);
    const branch = this.#branches[index];
    if (branch.isNull) {
      return null;
    }
    const value = branch.type.random();
    return { [branch.name]: value };
  }

  public override toJSON(): JSONType {
    return this.#types.map((type) => type.toJSON());
  }

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
    value: UnionValue,
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
}
