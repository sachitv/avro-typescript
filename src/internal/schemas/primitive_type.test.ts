import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { Tap } from '../serialization/tap.ts';
import { PrimitiveType } from './primitive_type.ts';
import { Type } from './type.ts';
import { ValidationError, throwInvalidError } from './error.ts';
import { encode } from "../serialization/text_encoding.ts";

/**
 * A simple concrete implementation of PrimitiveType for testing purposes.
 * Handles number values.
 */
class TestPrimitiveType extends PrimitiveType<number> {
  public override check(value: unknown, errorHook?: (path: string[], invalidValue: unknown, schemaType: PrimitiveType<number>) => void, path: string[] = []): boolean {
    const isValid = typeof value === 'number' && Number.isInteger(value) && value >= 0 && value <= 100;
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  public override toBuffer(value: number): ArrayBuffer {
    // Allocate 5 bytes (max size for 32-bit int varint)
    const buf = new ArrayBuffer(5);
    const tap = new Tap(buf);
    this.write(tap, value);
    const result = tap.getValue();
    return (result.buffer as ArrayBuffer).slice(result.byteOffset, result.byteOffset + result.byteLength);
  }

  public override read(tap: Tap): number {
    return tap.readInt();
  }

  public override write(tap: Tap, value: number): void {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    tap.writeInt(value);
  }

  public override toJSON(): string {
    return 'test';
  }

  public override random(): number {
    return Math.floor(Math.random() * 101);
  }
}

/**
 * Another fake primitive type for testing different constructors.
 */
class FakePrimitiveType extends PrimitiveType<string> {
  public override check(value: unknown, errorHook?: (path: string[], invalidValue: unknown, schemaType: Type) => void, path: string[] = []): boolean {
    const isValid = typeof value === 'string';
    if (!isValid && errorHook) {
      errorHook(path, value, this);
    }
    return isValid;
  }

  public override toBuffer(value: string): ArrayBuffer {
    const strBytes = encode(value);
    const buf = new ArrayBuffer(5 + strBytes.length);
    const tap = new Tap(buf);
    tap.writeString(value);
    const result = tap.getValue();
    return (result.buffer as ArrayBuffer).slice(result.byteOffset, result.byteOffset + result.byteLength);
  }

  public override read(tap: Tap): string {
    return tap.readString()!;
  }

  public override write(tap: Tap, value: string): void {
    if (!this.check(value)) {
      throwInvalidError([], value, this);
    }
    tap.writeString(value);
  }

  public override toJSON(): string {
    return 'fake';
  }

  public override random(): string {
    return 'fake';
  }
}

describe('PrimitiveType', () => {
  const type = new TestPrimitiveType();

  describe('clone', () => {
    it('should clone a valid number value', () => {
      const value = 42;
      const cloned = type.clone(value);
      assertEquals(cloned, value);
      assert(cloned !== value || typeof value !== 'object'); // Primitives are immutable
    });

    it('should throw ValidationError for invalid values', () => {
      assertThrows(() => {
        type.clone(150); // Invalid since > 100
      }, ValidationError);
    });
  });

  describe('compare', () => {
    it('should compare numbers correctly', () => {
      assertEquals(type.compare(1, 2), -1);
      assertEquals(type.compare(2, 1), 1);
      assertEquals(type.compare(1, 1), 0);
    });

    it('should handle edge cases', () => {
      assertEquals(type.compare(0, 100), -1);
      assertEquals(type.compare(100, 0), 1);
    });
  });

  describe('inheritance from BaseType', () => {
    it('should have toBuffer and fromBuffer from BaseType', () => {
      const value = 50;
      const buffer = type.toBuffer(value);
      const result = type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it('should have isValid from BaseType', () => {
      assert(type.isValid(50));
      assert(!type.isValid(150));
    });



    it('should create resolver for same type', () => {
      const resolver = type.createResolver(type);
      const value = 42;
      const buffer = type.toBuffer(value);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, value);
    });

    it('should throw error for different type', () => {
      const otherType = new FakePrimitiveType();
      assertThrows(() => {
        type.createResolver(otherType);
      }, Error, 'Schema evolution not supported from writer type: fake to reader type: test');
    });
  });
});