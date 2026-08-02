import { describe, expect, it } from 'vitest';

import {
  filterWatchlistForTable,
  shouldShowInWatchlistTable,
} from './watchlist-table-filter';

describe('shouldShowInWatchlistTable', () => {
  it('shows held positions regardless of score', () => {
    expect(
      shouldShowInWatchlistTable(
        { symbol: 'CN:A', positionPct: 5 },
        { trendOk: false, trendStatus: 'no', score: 0 },
        'WATCH_SILENT',
      ),
    ).toBe(true);
  });

  it('hides Score=0 + TrendOK=no + Pos%=— silent rows', () => {
    expect(
      shouldShowInWatchlistTable(
        { symbol: 'CN:B', positionPct: null },
        { trendOk: false, trendStatus: 'no', score: 0 },
        'WATCH_SILENT',
      ),
    ).toBe(false);
    expect(
      shouldShowInWatchlistTable(
        { symbol: 'CN:B', positionPct: undefined },
        { trendOk: false, trendStatus: 'no', score: 0 },
        'WATCH_SILENT',
      ),
    ).toBe(false);
  });

  it('shows Score >= 60 even when Pos%=— and TrendOK=no', () => {
    expect(
      shouldShowInWatchlistTable(
        { symbol: 'CN:C', positionPct: null },
        { trendOk: false, trendStatus: 'no', score: 80 },
        'WATCH_SILENT',
      ),
    ).toBe(true);
  });

  it('shows TrendOK=ok even when Score=0', () => {
    expect(
      shouldShowInWatchlistTable(
        { symbol: 'CN:D', positionPct: null },
        { trendOk: true, trendStatus: 'ok', score: 0 },
        'WATCH_SILENT',
      ),
    ).toBe(true);
  });

  it('shows TrendOK=recovering even when Score=0', () => {
    expect(
      shouldShowInWatchlistTable(
        { symbol: 'CN:E', positionPct: null },
        { trendOk: null, trendStatus: 'recovering', score: 0 },
        'WATCH_SILENT',
      ),
    ).toBe(true);
  });

  it('shows Action != WATCH_SILENT even with low score and bad trend', () => {
    expect(
      shouldShowInWatchlistTable(
        { symbol: 'CN:F', positionPct: null },
        { trendOk: false, trendStatus: 'no', score: 10 },
        'WATCH',
      ),
    ).toBe(true);
    expect(
      shouldShowInWatchlistTable(
        { symbol: 'CN:G', positionPct: null },
        { trendOk: false, trendStatus: 'no', score: 10 },
        'BUY',
      ),
    ).toBe(true);
    expect(
      shouldShowInWatchlistTable(
        { symbol: 'CN:H', positionPct: null },
        { trendOk: false, trendStatus: 'no', score: 10 },
        'EXIT',
      ),
    ).toBe(true);
  });

  it('hides null trend + null action + Pos%=— rows', () => {
    expect(shouldShowInWatchlistTable({ symbol: 'CN:I' }, null, null)).toBe(false);
  });

  it('treats null score as not-high-score', () => {
    expect(
      shouldShowInWatchlistTable(
        { symbol: 'CN:J' },
        { trendOk: false, trendStatus: 'no', score: null },
        'WATCH_SILENT',
      ),
    ).toBe(false);
  });
});

describe('filterWatchlistForTable', () => {
  it('preserves input order and drops silent rows', () => {
    const items = [
      { symbol: 'CN:HELD', positionPct: 8 },
      { symbol: 'CN:SILENT', positionPct: null },
      { symbol: 'CN:HIGHSCORE', positionPct: null },
      { symbol: 'CN:RECOVERING', positionPct: null },
      { symbol: 'CN:ACTIVE', positionPct: null },
      { symbol: 'CN:PURGEME', positionPct: null },
    ];
    const trendMap = {
      'CN:HELD': { trendOk: false, trendStatus: 'no' as const, score: 0 },
      'CN:SILENT': { trendOk: false, trendStatus: 'no' as const, score: 0 },
      'CN:HIGHSCORE': { trendOk: false, trendStatus: 'no' as const, score: 90 },
      'CN:RECOVERING': { trendOk: null, trendStatus: 'recovering' as const, score: 30 },
      'CN:ACTIVE': { trendOk: false, trendStatus: 'no' as const, score: 10 },
      'CN:PURGEME': { trendOk: false, trendStatus: 'no' as const, score: 0 },
    };
    const actionBySymbol = {
      'CN:HELD': 'HOLD',
      'CN:SILENT': 'WATCH_SILENT',
      'CN:HIGHSCORE': 'WATCH_SILENT',
      'CN:RECOVERING': 'WATCH_SILENT',
      'CN:ACTIVE': 'BUY',
      'CN:PURGEME': 'WATCH_SILENT',
    };
    const visible = filterWatchlistForTable(items, trendMap, actionBySymbol);
    expect(visible.map((x) => x.symbol)).toEqual([
      'CN:HELD',
      'CN:HIGHSCORE',
      'CN:RECOVERING',
      'CN:ACTIVE',
    ]);
  });
});
