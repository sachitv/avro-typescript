import { assertEquals, assertThrows } from "@std/assert";
import { describe, it } from "@std/testing/bdd";
import { StringType } from './string_type.ts';
import { BytesType } from './bytes_type.ts';
import { Tap } from '../serialization/tap.ts';

describe('StringType', () => {
  const type = new StringType();

  it('check should validate strings', () => {
    assertEquals(type.check('hello'), true);
    assertEquals(type.check(''), true);
    assertEquals(type.check(123), false);
    assertEquals(type.check(null), false);
    assertEquals(type.check(undefined), false);

    let errorCalled = false;
    const errorHook = () => { errorCalled = true; };
    assertEquals(type.check(123, errorHook), false);
    assertEquals(errorCalled, true);
  });

  it('toBuffer should allocate enough space for multi-byte strings', () => {
    const value = '\u00e9'.repeat(40);
    const buf = type.toBuffer(value);
    assertEquals(buf.byteLength, 82);
    const tap = new Tap(buf);
    assertEquals(type.read(tap), value);
  });

  it('toBuffer and read should serialize and deserialize strings', () => {
    const testStrings = ['hello', '', 'test string', '🚀 emoji'];

    for (const str of testStrings) {
      const buf = type.toBuffer(str);
      const tap = new Tap(buf);
      assertEquals(type.read(tap), str);
    }
  });

  it('write should write strings to tap', () => {
    const buf = new ArrayBuffer(100);
    const writeTap = new Tap(buf);
    type.write(writeTap, 'test');
    const readTap = new Tap(buf);
    assertEquals(type.read(readTap), 'test');
  });

  it('write should throw for non-strings', () => {
    const buf = new ArrayBuffer(10);
    const tap = new Tap(buf);
    const invalidValue: any = 123;
    assertThrows(() => type.write(tap, invalidValue), Error);
  });

  it('read should throw for insufficient data', () => {
    const buf = new ArrayBuffer(5);
    const writeTap = new Tap(buf);
    writeTap.writeLong(10n);
    const readTap = new Tap(buf);
    assertThrows(() => type.read(readTap), Error, 'Insufficient data for string');
  });

  it('compare should compare strings lexicographically', () => {
    assertEquals(type.compare('a', 'b'), -1);
    assertEquals(type.compare('b', 'a'), 1);
    assertEquals(type.compare('a', 'a'), 0);
    assertEquals(type.compare('', 'a'), -1);
  });

  it('random should return a string', () => {
    const rand = type.random();
    assertEquals(typeof rand, 'string');
    assertEquals(rand.length > 0, true);
  });

  it('toJSON should return "string"', () => {
    assertEquals(type.toJSON(), 'string');
  });

  describe('createResolver', () => {
    it('should create resolver for same type', () => {
      const resolver = type.createResolver(type);
      const str = 'test';
      const buffer = type.toBuffer(str);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, str);
    });

    it('should create resolver for BytesType writer', () => {
      const bytesType = new BytesType();
      const resolver = type.createResolver(bytesType);
      const bytes = new Uint8Array([72, 101, 108, 108, 111]); // 'Hello'
      const buffer = bytesType.toBuffer(bytes);
      const tap = new Tap(buffer);
      const result = resolver.read(tap);
      assertEquals(result, 'Hello');
    });

    it('should throw when reading bytes with insufficient data in resolver', () => {
      const bytesType = new BytesType();
      const resolver = type.createResolver(bytesType);
      const buf = new ArrayBuffer(5);
      const writeTap = new Tap(buf);
      writeTap.writeLong(10n);
      const readTap = new Tap(buf);
      assertThrows(() => resolver.read(readTap), Error, 'Insufficient data for bytes');
    });
  });
});
