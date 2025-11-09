import {
  ReadableTap,
  type ReadableTapLike,
  WritableTap,
  type WritableTapLike,
} from "./tap.ts";

/**
 * Test-only adapter that preserves the legacy combined Tap API by delegating to
 * the new ReadableTap and WritableTap implementations. Each operation spins up
 * a fresh reader or writer positioned at the current cursor so the semantics
 * remain identical to the previous monolithic Tap used throughout the tests.
 */
export class TestTap implements ReadableTapLike, WritableTapLike {
  #buffer: ArrayBuffer;
  #pos: number;

  constructor(buffer: ArrayBuffer, pos = 0) {
    this.#buffer = buffer;
    this.#pos = pos;
  }

  #reader(): ReadableTap {
    return new ReadableTap(this.#buffer, this.#pos);
  }

  #writer(): WritableTap {
    return new WritableTap(this.#buffer, this.#pos);
  }

  #syncPos(tap: ReadableTap | WritableTap): void {
    this.#pos = tap._testOnlyPos;
  }

  async _testOnlyBuf(): Promise<Uint8Array> {
    return await new ReadableTap(this.#buffer)._testOnlyBuf();
  }

  get _testOnlyPos(): number {
    return this.#pos;
  }

  resetPos(): void {
    this.#pos = 0;
  }

  async isValid(): Promise<boolean> {
    return await this.#reader().isValid();
  }

  async getValue(): Promise<Uint8Array> {
    return await this.#reader().getValue();
  }

  async readBoolean(): Promise<boolean> {
    const tap = this.#reader();
    const value = await tap.readBoolean();
    this.#syncPos(tap);
    return value;
  }

  skipBoolean(): void {
    const tap = this.#reader();
    tap.skipBoolean();
    this.#syncPos(tap);
  }

  async writeBoolean(value: boolean): Promise<void> {
    const tap = this.#writer();
    await tap.writeBoolean(value);
    this.#syncPos(tap);
  }

  async readInt(): Promise<number> {
    const tap = this.#reader();
    const value = await tap.readInt();
    this.#syncPos(tap);
    return value;
  }

  async readLong(): Promise<bigint> {
    const tap = this.#reader();
    const value = await tap.readLong();
    this.#syncPos(tap);
    return value;
  }

  async skipInt(): Promise<void> {
    const tap = this.#reader();
    await tap.skipInt();
    this.#syncPos(tap);
  }

  async skipLong(): Promise<void> {
    const tap = this.#reader();
    await tap.skipLong();
    this.#syncPos(tap);
  }

  async writeInt(value: number): Promise<void> {
    const tap = this.#writer();
    await tap.writeInt(value);
    this.#syncPos(tap);
  }

  async writeLong(value: bigint): Promise<void> {
    const tap = this.#writer();
    await tap.writeLong(value);
    this.#syncPos(tap);
  }

  async readFloat(): Promise<number | undefined> {
    const tap = this.#reader();
    const value = await tap.readFloat();
    this.#syncPos(tap);
    return value;
  }

  skipFloat(): void {
    const tap = this.#reader();
    tap.skipFloat();
    this.#syncPos(tap);
  }

  async writeFloat(value: number): Promise<void> {
    const tap = this.#writer();
    await tap.writeFloat(value);
    this.#syncPos(tap);
  }

  async readDouble(): Promise<number | undefined> {
    const tap = this.#reader();
    const value = await tap.readDouble();
    this.#syncPos(tap);
    return value;
  }

  skipDouble(): void {
    const tap = this.#reader();
    tap.skipDouble();
    this.#syncPos(tap);
  }

  async writeDouble(value: number): Promise<void> {
    const tap = this.#writer();
    await tap.writeDouble(value);
    this.#syncPos(tap);
  }

  async readFixed(len: number): Promise<Uint8Array | undefined> {
    const tap = this.#reader();
    const value = await tap.readFixed(len);
    this.#syncPos(tap);
    return value;
  }

  skipFixed(len: number): void {
    const tap = this.#reader();
    tap.skipFixed(len);
    this.#syncPos(tap);
  }

  async writeFixed(buf: Uint8Array, len?: number): Promise<void> {
    const tap = this.#writer();
    await tap.writeFixed(buf, len);
    this.#syncPos(tap);
  }

  async readBytes(): Promise<Uint8Array | undefined> {
    const tap = this.#reader();
    const value = await tap.readBytes();
    this.#syncPos(tap);
    return value;
  }

  async skipBytes(): Promise<void> {
    const tap = this.#reader();
    await tap.skipBytes();
    this.#syncPos(tap);
  }

  async readString(): Promise<string | undefined> {
    const tap = this.#reader();
    const value = await tap.readString();
    this.#syncPos(tap);
    return value;
  }

  async skipString(): Promise<void> {
    const tap = this.#reader();
    await tap.skipString();
    this.#syncPos(tap);
  }

  async writeBytes(buf: Uint8Array): Promise<void> {
    const tap = this.#writer();
    await tap.writeBytes(buf);
    this.#syncPos(tap);
  }

  async writeString(str: string): Promise<void> {
    const tap = this.#writer();
    await tap.writeString(str);
    this.#syncPos(tap);
  }

  async writeBinary(str: string, len: number): Promise<void> {
    const tap = this.#writer();
    await tap.writeBinary(str, len);
    this.#syncPos(tap);
  }

  async matchBoolean(tap: TestTap): Promise<number> {
    const thisReader = this.#reader();
    const otherReader = tap.#reader();
    const result = await thisReader.matchBoolean(otherReader);
    this.#syncPos(thisReader);
    tap.#syncPos(otherReader);
    return result;
  }

  async matchInt(tap: TestTap): Promise<number> {
    const thisReader = this.#reader();
    const otherReader = tap.#reader();
    const result = await thisReader.matchInt(otherReader);
    this.#syncPos(thisReader);
    tap.#syncPos(otherReader);
    return result;
  }

  async matchLong(tap: TestTap): Promise<number> {
    const thisReader = this.#reader();
    const otherReader = tap.#reader();
    const result = await thisReader.matchLong(otherReader);
    this.#syncPos(thisReader);
    tap.#syncPos(otherReader);
    return result;
  }

  async matchFloat(tap: TestTap): Promise<number> {
    const thisReader = this.#reader();
    const otherReader = tap.#reader();
    const result = await thisReader.matchFloat(otherReader);
    this.#syncPos(thisReader);
    tap.#syncPos(otherReader);
    return result;
  }

  async matchDouble(tap: TestTap): Promise<number> {
    const thisReader = this.#reader();
    const otherReader = tap.#reader();
    const result = await thisReader.matchDouble(otherReader);
    this.#syncPos(thisReader);
    tap.#syncPos(otherReader);
    return result;
  }

  async matchFixed(tap: TestTap, len: number): Promise<number> {
    const thisReader = this.#reader();
    const otherReader = tap.#reader();
    const result = await thisReader.matchFixed(otherReader, len);
    this.#syncPos(thisReader);
    tap.#syncPos(otherReader);
    return result;
  }

  async matchBytes(tap: TestTap): Promise<number> {
    const thisReader = this.#reader();
    const otherReader = tap.#reader();
    const result = await thisReader.matchBytes(otherReader);
    this.#syncPos(thisReader);
    tap.#syncPos(otherReader);
    return result;
  }

  async matchString(tap: TestTap): Promise<number> {
    const thisReader = this.#reader();
    const otherReader = tap.#reader();
    const result = await thisReader.matchString(otherReader);
    this.#syncPos(thisReader);
    tap.#syncPos(otherReader);
    return result;
  }

  async unpackLongBytes(): Promise<Uint8Array> {
    const tap = this.#reader();
    const value = await tap.unpackLongBytes();
    this.#syncPos(tap);
    return value;
  }

  async packLongBytes(arr: Uint8Array): Promise<void> {
    const tap = this.#writer();
    await tap.packLongBytes(arr);
    this.#syncPos(tap);
  }
}
