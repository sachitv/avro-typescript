import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { FixedSizeBaseType } from './fixed_size_base_type.ts';
import { Tap } from '../serialization/tap.ts';

// Simple concrete implementation for testing
class TestFixedSizeType extends FixedSizeBaseType<number> {
  public sizeBytes(): number {
    return 4;
  }

  public check(value: unknown): boolean {
    return typeof value === 'number';
  }

  public read(tap: Tap): number {
    return tap.readInt() || 0;
  }

  public write(tap: Tap, value: number): void {
    tap.writeInt(value);
  }

  public clone(value: number): number {
    return value;
  }

  public compare(val1: number, val2: number): number {
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0;
  }

  public random(): number {
    return Math.floor(Math.random() * 100);
  }

  public toJSON(): string {
    return 'test';
  }
}

describe('FixedSizeBaseType', () => {
  const type = new TestFixedSizeType();

  describe('toBuffer', () => {
    it('should serialize value using fixed size', () => {
      const value = 42;
      const buffer = type.toBuffer(value);
      assertEquals(buffer.byteLength, 4);
      const tap = new Tap(buffer);
      assertEquals(type.read(tap), value);
    });
  });
});