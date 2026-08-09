import { describe, expect, it } from 'vitest';

import { buildHash, pageHref, parseHash } from './hash-router';

describe('parseHash', () => {
  it('defaults to dashboard for empty or unknown hashes', () => {
    expect(parseHash('')).toEqual({ page: 'dashboard', symbol: null, journalMode: null, journalId: null });
    expect(parseHash('#/nope')).toEqual({ page: 'dashboard', symbol: null, journalMode: null, journalId: null });
    expect(parseHash('#')).toEqual({ page: 'dashboard', symbol: null, journalMode: null, journalId: null });
  });

  it('parses simple pages', () => {
    expect(parseHash('#/watchlist').page).toBe('watchlist');
    expect(parseHash('#/decision').page).toBe('decision');
    expect(parseHash('#/industry-flow').page).toBe('industryFlow');
    expect(parseHash('#/backtest').page).toBe('backtest');
    expect(parseHash('#/scheduler').page).toBe('scheduler');
    expect(parseHash('#/settings').page).toBe('settings');
  });

  it('parses stock deep links with the symbol decoded', () => {
    const r = parseHash('#/stock/HK%3A00700');
    expect(r.page).toBe('stock');
    expect(r.symbol).toBe('HK:00700');
    expect(parseHash('#/stock/CN%3A300628').symbol).toBe('CN:300628');
  });

  it('parses journal sub-modes', () => {
    expect(parseHash('#/journal').journalMode).toBe('read');
    expect(parseHash('#/journal/write').journalMode).toBe('write');
    expect(parseHash('#/journal/review').journalMode).toBe('review');
    const w = parseHash('#/journal/write/abc-123');
    expect(w.journalMode).toBe('write');
    expect(w.journalId).toBe('abc-123');
  });
});

describe('buildHash / pageHref', () => {
  it('round-trips routes', () => {
    const cases: Array<[string, string]> = [
      ['#/dashboard', '#/dashboard'],
      ['#/watchlist', '#/watchlist'],
      ['#/stock/HK%3A00700', '#/stock/HK%3A00700'],
      ['#/journal/write', '#/journal/write'],
      ['#/journal/review', '#/journal/review'],
    ];
    for (const [hash, expected] of cases) {
      const r = parseHash(hash);
      expect(buildHash(r)).toBe(expected);
    }
  });

  it('builds shareable hrefs for markdown links', () => {
    expect(pageHref('watchlist')).toBe('#/watchlist');
    expect(pageHref('stock', 'HK:00700')).toBe('#/stock/HK%3A00700');
    expect(pageHref('decision')).toBe('#/decision');
  });
});
