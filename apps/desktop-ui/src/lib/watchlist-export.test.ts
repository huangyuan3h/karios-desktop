import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest';

import { buildWatchlistMarkdown } from './watchlist-export';

const baseOptions = {
  sortedItems: [{ symbol: 'CN:000001', name: 'Test', costPrice: 9.5, addedAt: '2026-06-18T00:00:00Z' }],
  trendUpdatedAt: null,
  tradingTime: true,
  todaySh: '2026-06-18',
};

describe('buildWatchlistMarkdown', () => {
  it('emits unified combat table with quant + execution columns', async () => {
    const md = await buildWatchlistMarkdown({
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

  it('splits HK rows into their own sleeve table', async () => {
    const md = await buildWatchlistMarkdown({
      ...baseOptions,
      sortedItems: [
        { symbol: 'CN:000001', name: 'TestA', costPrice: 9.5, addedAt: '2026-06-18T00:00:00Z' },
        { symbol: 'HK:00700', name: 'Tencent', costPrice: 476, addedAt: '2026-06-18T00:00:00Z' },
        { symbol: 'ETF:510300', name: '沪深300ETF', costPrice: 3.9, addedAt: '2026-06-18T00:00:00Z' },
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
        'ETF:510300': {
          symbol: 'ETF:510300',
          name: '恒生科技ETF',
          score: 65,
          trendOk: false,
          asOfDate: '2026-06-18',
          values: { close: 3.9, volumeRatio: 1.0, rsValue: 3.0 },
          rs: 3.0,
          intradayChgPct: -0.3,
          gapUp: false,
          missingData: [],
        },
      },
      quotesSnap: {
        'CN:000001': { tsCode: '000001.SZ', price: 10.2, tradeTime: '2026-06-18 14:30:00', amount: 1, volume: 1, preClose: 10, pctChg: 2 },
        'HK:00700': { tsCode: '00700.HK', price: 480.5, tradeTime: '2026-06-18 15:30:00', amount: 1, volume: 1, preClose: 476, pctChg: 1 },
        'ETF:510300': { tsCode: '510300.SH', price: 3.9, tradeTime: '2026-06-18 14:30:00', amount: 1, volume: 1, preClose: 3.9, pctChg: 0 },
      },
    });

    expect(md).toContain('## A股 卫星仓（CN: 个股 + ETF: 篮子）');
    expect(md).toContain('## 港股 卫星仓（HK: 个股/基金）');
    const cnTable = md.slice(0, md.indexOf('## 港股 卫星仓'));
    expect(cnTable).toContain('| CN:000001 |');
    expect(cnTable).toContain('| ETF:510300 |');
    expect(cnTable).not.toContain('| HK:00700 |');
    const hkTable = md.slice(md.indexOf('## 港股 卫星仓'));
    expect(hkTable).toContain('| HK:00700 |');
    expect(hkTable).not.toContain('| CN:000001 |');
    expect(hkTable).not.toContain('| ETF:510300 |');
  });
});

describe('S-3 backtest candidate block', () => {
  beforeEach(() => {
    global.fetch = vi.fn(async (url: RequestInfo | URL) => {
      const s = String(url);
      if (s.includes('panic-cooldown')) {
        return {
          ok: true,
          json: async () => ({
            lastPanicDate: null,
            cooldownEndDate: null,
            active: false,
          }),
        } as Response;
      }
      const ranks: Record<string, number> = { 'CN:600001': 0.8, 'CN:600002': 0.6 };
      return {
        ok: true,
        json: async () => ({ ok: true, asOfDate: '2026-08-07', ranks }),
      } as Response;
    }) as never;
  });
  afterEach(() => {
    vi.restoreAllMocks();
  });

  const s3Gate = {
    mode: 'ATTACK',
    allowNewEntries: true,
    marketRegime: 'Strong',
    positionRangeHint: '50%-60%',
  };

  const items = [
    { symbol: 'CN:600001', name: '测试A', addedAt: '2026-08-01', source: 'manual', color: '#000', positionPct: null, costPrice: null, maxPrice: null, entryDate: null },
    { symbol: 'CN:600002', name: '测试B', addedAt: '2026-08-01', source: 'manual', color: '#000', positionPct: 5, costPrice: 10, maxPrice: null, entryDate: '2026-07-01' },
  ];
  const trend = {
    'CN:600001': { symbol: 'CN:600001', score: 72, trendOk: true, asOfDate: '2026-08-07', values: { emIndustry: '计算机', close: 10 }, marketRegime: 'Strong', intradayChgPct: 0, gapUp: false, missingData: [] },
    'CN:600002': { symbol: 'CN:600002', score: 80, trendOk: true, asOfDate: '2026-08-07', values: { emIndustry: '计算机', close: 12 }, marketRegime: 'Strong', intradayChgPct: 0, gapUp: false, missingData: [] },
  } as never;
  const quotes = { 'CN:600001': { price: 10 }, 'CN:600002': { price: 12 } } as never;

  it('lists only flat, S-3-qualified symbols with backtest size', async () => {
    const md = await buildWatchlistMarkdown({
      sortedItems: items as never,
      trendSnap: trend,
      quotesSnap: quotes,
      trendUpdatedAt: null,
      tradingTime: false,
      todaySh: '2026-08-07',
      executionGate: s3Gate as never,
      mainlineAllow: { ready: true, names: new Set(['计算机']), byName: new Map() } as never,
      sectorOutflowBlock: false,
    });
    expect(md).toContain('S-3 回测口径买入候选');
    const s3Block = md.slice(md.indexOf('S-3 回测口径买入候选'), md.indexOf('A股 卫星仓'));
    expect(s3Block).toContain('| CN:600001 |');
    expect(s3Block).not.toContain('CN:600002');  // held position excluded
    expect(s3Block).toContain('仓位');
  });

  it('emits wait message when regime is Weak', async () => {
    const md = await buildWatchlistMarkdown({
      sortedItems: items as never,
      trendSnap: trend,
      quotesSnap: quotes,
      trendUpdatedAt: null,
      tradingTime: false,
      todaySh: '2026-08-07',
      executionGate: { mode: 'DEFEND', allowNewEntries: false, marketRegime: 'Weak' } as never,
      mainlineAllow: { ready: true, names: new Set(['计算机']), byName: new Map() } as never,
      sectorOutflowBlock: false,
    });
    expect(md).toContain('空仓等待');
  });

  it('shows panic cooldown and suppresses candidates during cooldown', async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      async (url: RequestInfo | URL) => {
        if (String(url).includes('panic-cooldown')) {
          return {
            ok: true,
            json: async () => ({
              lastPanicDate: '2026-08-06',
              cooldownEndDate: '2026-08-11',
              active: true,
            }),
          } as Response;
        }
        return {
          ok: true,
          json: async () => ({ ok: true, asOfDate: '2026-08-07', ranks: { 'CN:600001': 0.8 } }),
        } as Response;
      },
    );
    const md = await buildWatchlistMarkdown({
      sortedItems: items as never,
      trendSnap: trend,
      quotesSnap: quotes,
      trendUpdatedAt: null,
      tradingTime: false,
      todaySh: '2026-08-07',
      executionGate: s3Gate as never,
      mainlineAllow: { ready: true, names: new Set(['计算机']), byName: new Map() } as never,
      sectorOutflowBlock: false,
    });
    expect(md).toContain('恐慌冷却期');
    expect(md).toContain('最近恐慌日 2026-08-06');
    expect(md).toContain('冷却至 2026-08-11');
    const s3Block = md.slice(md.indexOf('S-3 回测口径买入候选'), md.indexOf('A股 卫星仓'));
    expect(s3Block).not.toContain('| CN:600001 |');
  });
});
