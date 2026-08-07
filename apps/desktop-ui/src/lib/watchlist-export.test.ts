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

    expect(md).toContain('## Combat Positions & Watchlist（A股 / 港股 分表）');
    expect(md).toContain('## A股 卫星仓（CN: 个股 + ETF: 篮子）');
    expect(md).toContain(
      '| Symbol | Name | RS | Score | TrendOK | Current | Pos% | CostPrice | P&L% | EntryDate | Locked_T1 | Action | Suggest% | Entry_Trigger | Exit_Stop | HardStop | TrailStop | Dist% | Mainline | Why |',
    );
    expect(md).toContain('| CN:000001 | Test |');
    expect(md).toContain('| 82 |');
    expect(md).toContain('| ok |');
    expect(md).not.toContain('| Intraday% | VR | Inst_Flow | GapUp |');
    expect(md).not.toContain('## Watchlist');
    expect(md).not.toContain('## Positions (execution)');
  });

  it('splits HK rows into their own sleeve table', () => {
    const md = buildWatchlistMarkdown({
      ...baseOptions,
      sortedItems: [
        { symbol: 'CN:000001', name: 'TestA', costPrice: 9.5, addedAt: '2026-06-18T00:00:00Z' },
        { symbol: 'HK:00700', name: 'Tencent', costPrice: 476, addedAt: '2026-06-18T00:00:00Z' },
        { symbol: 'ETF:513180', name: '恒生科技ETF', costPrice: 0.61, addedAt: '2026-06-18T00:00:00Z' },
      ],
      trendSnap: {
        'CN:000001': {
          symbol: 'CN:000001',
          name: 'TestA',
          score: 82,
          trendOk: true,
          asOfDate: '2026-06-18',
          values: { close: 10, volumeRatio: 1.58, rsValue: 5.2 },
          rs: 5.2,
          intradayChgPct: 2.1,
          gapUp: false,
          missingData: [],
        },
        'HK:00700': {
          symbol: 'HK:00700',
          name: 'Tencent',
          score: 70,
          trendOk: true,
          asOfDate: '2026-06-18',
          values: { close: 480, volumeRatio: 1.1, rsValue: 4.0 },
          rs: 4.0,
          intradayChgPct: 0.5,
          gapUp: false,
          missingData: [],
        },
        'ETF:513180': {
          symbol: 'ETF:513180',
          name: '恒生科技ETF',
          score: 65,
          trendOk: false,
          asOfDate: '2026-06-18',
          values: { close: 0.61, volumeRatio: 1.0, rsValue: 3.0 },
          rs: 3.0,
          intradayChgPct: -0.3,
          gapUp: false,
          missingData: [],
        },
      },
      quotesSnap: {
        'CN:000001': { tsCode: '000001.SZ', price: 10.2, tradeTime: '2026-06-18 14:30:00', amount: 1, volume: 1, preClose: 10, pctChg: 2 },
        'HK:00700': { tsCode: '00700.HK', price: 480.5, tradeTime: '2026-06-18 15:30:00', amount: 1, volume: 1, preClose: 476, pctChg: 1 },
        'ETF:513180': { tsCode: '513180.SH', price: 0.61, tradeTime: '2026-06-18 14:30:00', amount: 1, volume: 1, preClose: 0.61, pctChg: 0 },
      },
    });

    expect(md).toContain('## A股 卫星仓（CN: 个股 + ETF: 篮子）');
    expect(md).toContain('## 港股 卫星仓（HK: 个股/基金）');
    const cnTable = md.slice(0, md.indexOf('## 港股 卫星仓'));
    expect(cnTable).toContain('| CN:000001 |');
    expect(cnTable).toContain('| ETF:513180 |');
    expect(cnTable).not.toContain('| HK:00700 |');
    const hkTable = md.slice(md.indexOf('## 港股 卫星仓'));
    expect(hkTable).toContain('| HK:00700 |');
    expect(hkTable).not.toContain('| CN:000001 |');
    expect(hkTable).not.toContain('| ETF:513180 |');
  });
});
