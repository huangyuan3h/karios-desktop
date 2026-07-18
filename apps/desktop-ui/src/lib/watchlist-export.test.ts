import { describe, expect, it } from 'vitest';

import { buildWatchlistMarkdown } from './watchlist-export';

const baseOptions = {
  sortedItems: [{ symbol: 'CN:000001', name: 'Test', costPrice: 9.5, addedAt: '2026-06-18T00:00:00Z' }],
  trendUpdatedAt: null,
  tradingTime: true,
  todaySh: '2026-06-18',
};

describe('buildWatchlistMarkdown', () => {
  it('emits unified combat table with quant + execution columns', () => {
    const md = buildWatchlistMarkdown({
      ...baseOptions,
      trendSnap: {
        'CN:000001': {
          symbol: 'CN:000001',
          name: 'Test',
          score: 82,
          trendOk: true,
          asOfDate: '2026-06-18',
          values: { close: 10, volumeRatio: 1.58, rsValue: 5.2 },
          rs: 5.2,
          intradayChgPct: 2.1,
          gapUp: false,
          missingData: [],
        },
      },
      quotesSnap: {
        'CN:000001': {
          tsCode: '000001.SZ',
          price: 10.2,
          tradeTime: '2026-06-18 14:30:00',
          amount: 102000,
          volume: 100,
          preClose: 10,
          pctChg: 2,
        },
      },
    });

    expect(md).toContain('## Combat Positions & Watchlist (Unified)');
    expect(md).toContain(
      '| Symbol | Name | RS | Score | TrendOK | Current | Pos% | Action | Suggest% | Entry_Trigger | Exit_Stop | HardStop | TrailStop | Dist% | Mainline | Why |',
    );
    expect(md).toContain('| CN:000001 | Test |');
    expect(md).toContain('| 82 |');
    expect(md).toContain('| ok |');
    expect(md).not.toContain('| Intraday% | VR | Inst_Flow | GapUp |');
    expect(md).not.toContain('## Watchlist');
    expect(md).not.toContain('## Positions (execution)');
  });
});
