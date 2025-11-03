import { assert, assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { Tap } from '../serialization/tap.ts';
import { FloatType } from './float_type.ts';
import { IntType } from './int_type.ts';
import { LongType } from './long_type.ts';
import { ValidationError } from './error.ts';
import { BooleanType } from "./boolean_type.ts";

describe('FloatType', () => {
  const type = new FloatType();

  describe('check', () => {
    it('should return true for all numbers including NaN and infinity', () => {
      assert(type.check(0));
      assert(type.check(42.5));
      assert(type.check(-42.5));
      assert(type.check(NaN));
      assert(type.check(Infinity));
      assert(type.check(-Infinity));
    });

    it('should return false for non-numbers', () => {
      assert(!type.check('42'));
      assert(!type.check(null));
      assert(!type.check(undefined));
      assert(!type.check({}));
    });

    it('should call errorHook for invalid values', () => {
      let called = false;
      const errorHook = () => { called = true; };
      type.check('invalid', errorHook);
      assert(called);
    });
  });

  describe('read', () => {
    it('should read float from tap', () => {
      const buffer = new ArrayBuffer(4);
      const writeTap = new Tap(buffer);
      writeTap.writeFloat(123.5);
      const readTap = new Tap(buffer);
      assertEquals(type.read(readTap), 123.5);
    });

    it('should handle NaN and infinity', () => {
      const values = [NaN, Infinity, -Infinity];
      for (const val of values) {
        const buffer = type.toBuffer(val);
        const result = type.fromBuffer(buffer);
        if (isNaN(val)) {
          assert(isNaN(result));
        } else {
          assertEquals(result, val);
        }
      }
    });

    it('should throw when insufficient data', () => {
      const buffer = new ArrayBuffer(2); // Less than 4 bytes needed for float
      const tap = new Tap(buffer);
      assertThrows(() => {
        type.read(tap);
      }, Error, 'Insufficient data for float');
    });
  });

  describe('write', () => {
    it('should write float to tap', () => {
      const buffer = new ArrayBuffer(4);
      const writeTap = new Tap(buffer);
      type.write(writeTap, 456.0);
      const readTap = new Tap(buffer);
      assertEquals(readTap.readFloat(), 456.0);
    });

    it('should throw for invalid value', () => {
      const buffer = new ArrayBuffer(4);
      const tap = new Tap(buffer);
      assertThrows(() => {
        (type as any).write(tap, 'invalid');
      }, ValidationError);
    });
  });

  describe('sizeBytes', () => {
    it('should return 4', () => {
      assertEquals(type.sizeBytes(), 4);
    });
  });

  describe('compare', () => {
    it('should compare numbers correctly', () => {
      assertEquals(type.compare(1.0, 2.0), -1);
      assertEquals(type.compare(2.0, 1.0), 1);
      assertEquals(type.compare(1.0, 1.0), 0);
    });
  });

  describe('random', () => {
    it('should return a number', () => {
      const randomValue = type.random();
      assert(typeof randomValue === 'number');
      assert(isFinite(randomValue));
    });
  });

  describe('toJSON', () => {
    it('should return "float"', () => {
      assertEquals(type.toJSON(), 'float');
    });
  });

  describe('createResolver', () => {
    it('should create resolver for same type', () => {
      const resolver = type.createResolver(type);
      const value = 789.5;
      const buffer = type.toBuffer(value);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, value);
    });

    it('should create resolver for IntType writer', () => {
      const intType = new IntType();
      const resolver = type.createResolver(intType);
      const intValue = 123;
      const buffer = intType.toBuffer(intValue);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, 123);
    });

    it('should create resolver for LongType writer', () => {
      const longType = new LongType();
      const resolver = type.createResolver(longType);
      const longValue = 123n;
      const buffer = longType.toBuffer(longValue);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, 123);
    });

    it('should demonstrate lossy promotion from long to float', () => {
      const longType = new LongType();
      const resolver = type.createResolver(longType);
      // Use a large bigint that loses precision when converted to float
      const largeLong = 2n ** 53n + 1n; // Larger than MAX_SAFE_INTEGER
      assert(largeLong > BigInt(Number.MAX_SAFE_INTEGER));
      const buffer = longType.toBuffer(largeLong);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      // The result should be rounded to the nearest representable float
      assertEquals(result, 2 ** 53); // Rounded down to 2^53
    });

    it('should throw error for unsupported type', () => {
      const otherType = new BooleanType();
      assertThrows(() => {
        type.createResolver(otherType);
      }, Error, 'Schema evolution not supported from writer type: boolean to reader type: float');
    });
  });

  describe('inheritance from PrimitiveType and BaseType', () => {
    it('should clone number values', () => {
      assertEquals(type.clone(42.5), 42.5);
      assertEquals(type.clone(-42.5), -42.5);
    });

    it('should throw ValidationError for invalid clone', () => {
      assertThrows(() => {
        (type as any).clone('invalid');
      }, ValidationError);
    });

    it('should have toBuffer and fromBuffer', () => {
      const value = 123.5;
      const buffer = type.toBuffer(value);
      const result = type.fromBuffer(buffer);
      assertEquals(result, value);
    });

    it('should have isValid', () => {
      assert(type.isValid(42.5));
      assert(type.isValid(NaN));
      assert(!type.isValid('invalid'));
    });
  });
});