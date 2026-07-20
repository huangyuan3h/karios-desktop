import { describe, expect, it } from 'vitest';
import {
  asTrimmedString,
  jsonStringifyPretty,
  jsonStringifyCompact,
  toIndentedText,
  buildContextMarkdown,
  buildPromptDebug,
  normalizeOptionalString,
} from './utils';

describe('asTrimmedString', () => {
  it('returns trimmed string for string input', () => {
    expect(asTrimmedString('  hello  ')).toBe('hello');
  });

  it('returns empty string for non-string input', () => {
    expect(asTrimmedString(123)).toBe('');
    expect(asTrimmedString(null)).toBe('');
    expect(asTrimmedString(undefined)).toBe('');
  });
});

describe('jsonStringifyPretty', () => {
  it('returns pretty JSON string', () => {
    expect(jsonStringifyPretty({ a: 1 })).toBe('{\n  "a": 1\n}');
  });

  it('returns string representation for non-serializable values', () => {
    const circular: { self?: unknown } = {};
    circular.self = circular;
    expect(jsonStringifyPretty(circular)).toBe('[object Object]');
  });
});

describe('jsonStringifyCompact', () => {
  it('returns compact JSON string', () => {
    expect(jsonStringifyCompact({ a: 1 })).toBe('{"a":1}');;;
  });

  it('returns string representation for non-serializable values', () => {
    const circular: { self?: unknown } = {};
    circular.self = circular;
    expect(jsonStringifyCompact(circular)).toBe('[object Object]');
  });
});

describe('toIndentedText', () => {
  it('returns dash for null', () => {
    expect(toIndentedText(null)).toBe('—');
  });

  it('returns padded string for string input', () => {
    expect(toIndentedText('hello', 2)).toBe('  hello');
  });

  it('returns padded number for number input', () => {
    expect(toIndentedText(42, 4)).toBe('    42');
  });

  it('returns empty array representation', () => {
    expect(toIndentedText([])).toBe('[]');
  });

  it('returns formatted array for scalar items', () => {
    expect(toIndentedText([1, 2, 3])).toBe('- 1\n- 2\n- 3');
  });

  it('returns formatted array for object items', () => {
    const result = toIndentedText([{ a: 1 }]);
    expect(result).toBe('-\n  a: 1');
  });

  it('returns empty object representation', () => {
    expect(toIndentedText({})).toBe('{}');
  });

  it('returns formatted object', () => {
    expect(toIndentedText({ b: 2, a: 1 })).toBe('a: 1\nb: 2');
  });

  it('returns formatted nested object', () => {
    const result = toIndentedText({ a: { b: 1 } });
    expect(result).toBe('a:\n  b: 1');
  });
});

describe('buildContextMarkdown', () => {
  it('returns markdown for null context', () => {
    expect(buildContextMarkdown(null)).toContain('### context');
  });

  it('returns markdown for non-object context', () => {
    expect(buildContextMarkdown('test')).toContain('### context');
    expect(buildContextMarkdown(123)).toContain('### context');
  });

  it('returns markdown for object context', () => {
    const result = buildContextMarkdown({ date: '2024-01-01' });
    expect(result).toContain('### date');
    expect(result).toContain('2024-01-01');
  });

  it('orders keys by preferred order', () => {
    const result = buildContextMarkdown({ stocks: [], date: '2024-01-01', account: 'test' });
    const dateIdx = result.indexOf('### date');
    const accountIdx = result.indexOf('### account');
    const stocksIdx = result.indexOf('### stocks');
    expect(dateIdx).toBeLessThan(accountIdx);
    expect(accountIdx).toBeLessThan(stocksIdx);
  });
});

describe('buildPromptDebug', () => {
  it('returns all debug fields', () => {
    const result = buildPromptDebug({
      system: 'test system',
      promptText: 'test prompt',
      context: { a: 1 },
    });
    expect(result.system).toBe('test system');
    expect(result.promptText).toBe('test prompt');
    expect(result.contextJsonCompact).toBe('{"a":1}');
    expect(result.contextJsonPretty).toBe('{\n  "a": 1\n}');
    expect(result.contextMarkdown).toContain('### a');
    expect(result.promptMarkdown).toContain('## System');
    expect(result.promptMarkdown).toContain('## Prompt');
    expect(result.promptMarkdown).toContain('## Context');
  });
});

describe('normalizeOptionalString', () => {
  it('returns undefined for empty string', () => {
    expect(normalizeOptionalString('')).toBeUndefined();
    expect(normalizeOptionalString('   ')).toBeUndefined();
  });

  it('returns trimmed string for non-empty input', () => {
    expect(normalizeOptionalString('  hello  ')).toBe('hello');
  });

  it('returns undefined for non-string input', () => {
    expect(normalizeOptionalString(123)).toBeUndefined();
    expect(normalizeOptionalString(null)).toBeUndefined();
  });
});