import { describe, expect, it } from 'vitest';
import { tryParseJsonObject } from './json_parse';

describe('tryParseJsonObject', () => {
  it('parses plain JSON object', () => {
    const result = tryParseJsonObject('{"key": "value"}');
    expect(result).toEqual({ key: 'value' });
  });

  it('strips markdown code fences', () => {
    const text = '```json\n{"data": 123}\n```';
    const result = tryParseJsonObject(text);
    expect(result).toEqual({ data: 123 });
  });

  it('strips code fences without language hint', () => {
    const text = '```\n{"name": "test"}\n```';
    const result = tryParseJsonObject(text);
    expect(result).toEqual({ name: 'test' });
  });

  it('extracts JSON object from text', () => {
    const text = 'Some prefix text {"extracted": true} some suffix';
    const result = tryParseJsonObject(text);
    expect(result).toEqual({ extracted: true });
  });

  it('throws error when no JSON object found', () => {
    expect(() => tryParseJsonObject('no json here')).toThrow('Failed to parse JSON');
  });

  it('handles whitespace', () => {
    const result = tryParseJsonObject('   {"spaced": "out"}   ');
    expect(result).toEqual({ spaced: 'out' });
  });
});
