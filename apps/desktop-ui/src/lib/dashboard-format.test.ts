import { describe, expect, it } from 'vitest';

import { fmtAmountCn, fmtSignedAmountCn, mdTable } from './dashboard-format';

describe('fmtAmountCn', () => {
  it('formats 亿 and 万', () => {
    expect(fmtAmountCn(150_000_000)).toBe('1.50亿');
    expect(fmtAmountCn(25_000)).toBe('2.5万');
    expect(fmtAmountCn(500)).toBe('500');
  });

  it('returns dash for invalid input', () => {
    expect(fmtAmountCn(null)).toBe('—');
    expect(fmtAmountCn('')).toBe('—');
  });
});

describe('fmtSignedAmountCn', () => {
  it('prefixes positive and negative amounts', () => {
    expect(fmtSignedAmountCn(5_230_000_000)).toBe('+52.30亿');
    expect(fmtSignedAmountCn(-1_240_000_000)).toBe('-12.40亿');
    expect(fmtSignedAmountCn(null)).toBe('—');
  });
});

describe('mdTable', () => {
  it('builds markdown table with header separator', () => {
    const md = mdTable(['A', 'B'], [[1, 2]]);
    expect(md).toContain('| A | B |');
    expect(md).toContain('| --- | --- |');
    expect(md).toContain('| 1 | 2 |');
  });

  it('escapes pipe characters in cells', () => {
    const md = mdTable(['X'], [['a|b']]);
    expect(md).toContain('a\\|b');
  });
});
