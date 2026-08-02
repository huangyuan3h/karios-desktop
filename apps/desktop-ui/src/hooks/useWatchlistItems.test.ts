import { describe, expect, it } from 'vitest';

import { normalizeSymbolInput } from '@/hooks/useWatchlistItems';

describe('normalizeSymbolInput', () => {
  it('passes through explicit CN:/HK:/ETF: prefixes', () => {
    expect(normalizeSymbolInput('CN:600519')).toEqual({ symbol: 'CN:600519' });
    expect(normalizeSymbolInput('HK:00700')).toEqual({ symbol: 'HK:00700' });
    expect(normalizeSymbolInput('ETF:510300')).toEqual({ symbol: 'ETF:510300' });
  });

  it('maps 5xxxxx/1xxxxx/9xxxxx 6-digit codes to ETF:', () => {
    expect(normalizeSymbolInput('510300')).toEqual({ symbol: 'ETF:510300' });
    expect(normalizeSymbolInput('512480')).toEqual({ symbol: 'ETF:512480' });
    expect(normalizeSymbolInput('159819')).toEqual({ symbol: 'ETF:159819' });
    expect(normalizeSymbolInput('513050')).toEqual({ symbol: 'ETF:513050' });
    expect(normalizeSymbolInput('588000')).toEqual({ symbol: 'ETF:588000' });
  });

  it('maps 0xxxxx/3xxxxx 6-digit codes to CN:', () => {
    expect(normalizeSymbolInput('000001')).toEqual({ symbol: 'CN:000001' });
    expect(normalizeSymbolInput('300750')).toEqual({ symbol: 'CN:300750' });
  });

  it('maps 6xxxxx 6-digit codes to CN: (default — user can prefix ETF: if needed)', () => {
    expect(normalizeSymbolInput('600000')).toEqual({ symbol: 'CN:600000' });
    expect(normalizeSymbolInput('688981')).toEqual({ symbol: 'CN:688981' });
  });

  it('maps 4-5 digit codes to HK:', () => {
    expect(normalizeSymbolInput('00700')).toEqual({ symbol: 'HK:00700' });
    expect(normalizeSymbolInput('09988')).toEqual({ symbol: 'HK:09988' });
    expect(normalizeSymbolInput('0700')).toEqual({ symbol: 'HK:0700' });
  });

  it('returns error for 3-digit codes (ambiguous — too short)', () => {
    expect(normalizeSymbolInput('700')).toHaveProperty('error');
  });

  it('returns error on empty input', () => {
    expect(normalizeSymbolInput('')).toEqual({ error: 'Empty input' });
    expect(normalizeSymbolInput('   ')).toEqual({ error: 'Empty input' });
  });

  it('returns error on garbage input', () => {
    const r = normalizeSymbolInput('abc');
    expect(r).toHaveProperty('error');
  });

  it('normalizes to uppercase', () => {
    expect(normalizeSymbolInput('cn:600519')).toEqual({ symbol: 'CN:600519' });
    expect(normalizeSymbolInput('etf:510300')).toEqual({ symbol: 'ETF:510300' });
  });
});