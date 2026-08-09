import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  apiGetJson: vi.fn(),
  apiPostJson: vi.fn(),
  fetchScreenerSnapshotsMap: vi.fn(),
  fetchTrendOkMap: vi.fn(),
  loadWatchlist: vi.fn(),
  saveWatchlist: vi.fn(),
  isShanghaiQuoteWindow: vi.fn(),
}));

vi.mock('@/lib/api/client', () => ({
  apiGetJson: mocks.apiGetJson,
  apiPostJson: mocks.apiPostJson,
}));
vi.mock('@/lib/queries/screener', () => ({
  fetchScreenerSnapshotsMap: mocks.fetchScreenerSnapshotsMap,
}));
vi.mock('@/lib/screenerExport', () => ({
  fetchTrendOkMap: mocks.fetchTrendOkMap,
  normalizeScreenerSymbol: (raw: string) => {
    const s = String(raw || '').trim().toUpperCase();
    if (/^\d{6}$/.test(s)) return `CN:${s}`;
    if (/^\d{4,5}$/.test(s)) return `HK:${s}`;
    return null;
  },
}));
vi.mock('@/lib/watchlist-storage', () => ({
  loadWatchlist: mocks.loadWatchlist,
  saveWatchlist: mocks.saveWatchlist,
}));
vi.mock('@/lib/quote-window', () => ({
  isShanghaiQuoteWindow: mocks.isShanghaiQuoteWindow,
}));

import { importFromScreener } from './watchlist-screener-import';

const SCREENER = { id: 's1', name: 'pullback', enabled: true };
const SNAP = {
  id: 's1',
  rows: [
    { Ticker: '601088', Price: '43.94', 'High 52W': '' },
    { Ticker: '000333', Price: '83.50', 'High 52W': '' },
    { Ticker: '601899', Price: '10.00', 'High 52W': '' },
  ],
};

describe('importFromScreener pullback gate (K-line)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.apiGetJson.mockResolvedValue({ items: [SCREENER] });
    mocks.fetchScreenerSnapshotsMap.mockResolvedValue({ s1: SNAP });
    mocks.loadWatchlist.mockReturnValue([]);
    mocks.isShanghaiQuoteWindow.mockReturnValue(false);
  });

  it('uses pullback-filter results (not TV High 52W column)', async () => {
    mocks.apiPostJson.mockResolvedValue({
      ok: true,
      asOf: '2026-08-07',
      unparsed: [],
      results: [
        { symbol: 'CN:601088', tsCode: '601088.SH', price: 43.94, high52w: 51.18, pullbackRatio: -0.141, inWindow: true, windowBars: 300, missing: false },
        { symbol: 'CN:000333', tsCode: '000333.SZ', price: 83.5, high52w: 89.5, pullbackRatio: -0.067, inWindow: true, windowBars: 300, missing: false },
        { symbol: 'CN:601899', tsCode: '601899.SH', price: 10.0, high52w: 15.0, pullbackRatio: -0.333, inWindow: false, windowBars: 300, missing: false },
      ],
    });
    mocks.fetchTrendOkMap.mockResolvedValue(
      new Map([
        ['CN:601088', { symbol: 'CN:601088', trendOk: true, score: 85 }],
        ['CN:000333', { symbol: 'CN:000333', trendOk: false, score: 30 }],
      ]),
    );

    const res = await importFromScreener({ existingItems: [] });

    expect(mocks.apiPostJson).toHaveBeenCalledWith('/watchlist/automation/pullback-filter', {
      symbols: ['CN:601088', 'CN:000333', 'CN:601899'],
    });
    // 601088 passed pullback + TrendOK → added; 000333 passed pullback but
    // failed TrendOK; 601899 dropped by pullback (ratio -33%).
    expect(res.addedCount).toBe(1);
    expect(res.debug.funnel).toMatchObject({
      tvHit: 3,
      passPullback: 2,
      passTrendOk: 1,
      addedNew: 1,
      droppedByPullback: 1,
      fallbackUsed: false,
    });
  });

  it('falls back when pullback filter returns nothing (TV column data gap)', async () => {
    mocks.apiPostJson.mockResolvedValue({
      ok: true,
      asOf: null,
      unparsed: [],
      results: [
        { symbol: 'CN:601088', tsCode: '601088.SH', price: null, high52w: null, pullbackRatio: null, inWindow: false, windowBars: 0, missing: true },
        { symbol: 'CN:000333', tsCode: '000333.SZ', price: null, high52w: null, pullbackRatio: null, inWindow: false, windowBars: 0, missing: true },
        { symbol: 'CN:601899', tsCode: '601899.SH', price: null, high52w: null, pullbackRatio: null, inWindow: false, windowBars: 0, missing: true },
      ],
    });
    mocks.apiGetJson
      .mockResolvedValueOnce({ items: [SCREENER] })
      .mockResolvedValueOnce({
        symbols: ['CN:601088'],
        namesBySymbol: { 'CN:601088': 'Shenhua' },
      });
    mocks.fetchTrendOkMap.mockResolvedValue(
      new Map([['CN:601088', { symbol: 'CN:601088', trendOk: true, score: 60 }]]),
    );

    const res = await importFromScreener({ existingItems: [] });

    expect(res.addedCount).toBe(1);
    expect(res.debug.funnel.fallbackUsed).toBe(true);
    expect(res.debug.funnel.fallbackHit).toBe(1);
  });
});
