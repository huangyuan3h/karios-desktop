import { describe, expect, it } from 'vitest';

import { buildWatchlistMarkdown } from './watchlist-export';

const baseOptions = {
  sortedItems: [{ symbol: 'CN:000001', name: 'Test', costPrice: 9.5, addedAt: '2026-06-18T00:00:00Z' }],
  trendUpdatedAt: null,
  tradingTime: true,
  todaySh: '2026-06-18',
};

describe('buildWatchlistMarkdown', () => {
  it('aligns Intraday, VR, Inst_Flow, and GapUp columns', () => {
    const md = buildWatchlistMarkdown({
      ...baseOptions,
      trendSnap: {
        'CN:000001': {
          symbol: 'CN:000001',
          name: 'Test',
          score: 82,
          trendOk: true,
          asOfDate: '2026-06-18',
          values: { close: 10, volumeRatio: 1.58 },
          intradayChgPct: 2.1,
          gapUp: false,
          instFlow: {
            onBoard: true,
            instNetBuyYi: 1.2,
            label: '机构主买',
            display: '+1.2亿 (机构主买)',
          },
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

    expect(md).toContain('| Intraday% | VR | Inst_Flow | GapUp |');
    expect(md).toContain('| CN:000001 | Test |');
    expect(md).toContain('| +2.1% | 1.58x | +1.2亿 (机构主买) | No |');
  });
});
