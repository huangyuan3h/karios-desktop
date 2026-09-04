import { describe, expect, it } from 'vitest';

import { buildAttributionDiff, symbolToTrackPick } from './attribution-diff';

describe('attribution-diff', () => {
  it('maps symbols to track picks', () => {
    expect(symbolToTrackPick('CN:300628')).toBe('STOCK');
    expect(symbolToTrackPick('ETF:513350')).toBe('OIL');
    expect(symbolToTrackPick('ETF:513110')).toBe('NASDAQ');
  });

  it('flags under-capture of STOCK engine when book is ETF-heavy', () => {
    const report = buildAttributionDiff({
      byPick: {
        STOCK: { days: 100, contribAddPct: 43.4, contribGeoPct: 60 },
        NASDAQ: { days: 50, contribAddPct: 20, contribGeoPct: 25 },
        OIL: { days: 40, contribAddPct: 10, contribGeoPct: 12 },
        GOLD: { days: 20, contribAddPct: 5, contribGeoPct: 5 },
        BOND10: { days: 5, contribAddPct: 1, contribGeoPct: 1 },
        REPO: { days: 10, contribAddPct: 0, contribGeoPct: 0 },
      },
      userByBucket: {
        STOCK_CN: { count: 2, sumPnlPct: 8 },
        NASDAQ: { count: 1, sumPnlPct: 3 },
      },
      holdings: [
        { symbol: 'CN:300628', positionPct: 10, pnlPct: 5 },
        { symbol: 'ETF:513110', positionPct: 49, pnlPct: 12 },
        { symbol: 'ETF:513350', positionPct: 42, pnlPct: 8 },
      ],
    });
    expect(report.trackEngine).toBe('STOCK');
    expect(report.userTopWeight).toBe('NASDAQ');
    const stock = report.rows.find((r) => r.pick === 'STOCK')!;
    expect(stock.kind).toBe('under_capture');
    const nas = report.rows.find((r) => r.pick === 'NASDAQ')!;
    expect(nas.kind).toBe('over_weight');
    expect(report.insights.some((i) => i.id === 'engine_under' || i.id === 'engine_mismatch')).toBe(
      true,
    );
  });
});
