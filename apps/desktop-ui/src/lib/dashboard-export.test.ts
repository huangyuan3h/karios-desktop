import { beforeEach, describe, expect, it, vi } from 'vitest';
import { QueryClient } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import { fetchDashboardSummary } from '@/lib/queries/dashboard';
import { watchlistMarketQueryOptions } from '@/lib/queries/watchlist';
import { fetchWatchlistMarketSnapshot } from '@/lib/watchlist-market';

import {
  buildDashboardCopyAllMarkdown,
  buildIndustryMarkdown,
  buildMarketAndMacroMarkdown,
  buildSentimentMarkdown,
  buildWatchlistMarkdown,
  SCREENER_COPY_MIN_SCORE,
  SCREENER_COPY_TOP_N,
} from './dashboard-export';
import { copyBlockingMissingData } from './watchlist-export';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
}));

vi.mock('@/lib/queries/dashboard', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/dashboard')>();
  return {
    ...actual,
    fetchDashboardSummary: vi.fn(),
  };
});

vi.mock('@/lib/alpha-radar-catalyst', () => ({
  buildAutoQaMarkdown: vi.fn(() => ''),
  buildCatalystPurgeMap: vi.fn(() => new Map()),
  buildCatalystStocksMarkdown: vi.fn(() => '## Catalyst\n'),
  buildAlphaRadarTrendsMarkdown: vi.fn(() => '## Trends\n'),
  DEFAULT_CATALYST_MAX_AGE_DAYS: 7,
  fetchAlphaRadarTrendsForCopy: vi.fn(async () => ({ items: [], scope: 'latest' })),
  fetchAutoQaStats: vi.fn(async () => null),
  fetchCatalystStocks: vi.fn(async () => ({ items: [] })),
  normalizeCatalystSymbol: vi.fn((s: string) => s),
}));

vi.mock('@/lib/watchlist-storage', () => ({
  loadWatchlist: vi.fn(() => [{ symbol: 'CN:000001', name: 'Test' }]),
  ensureWatchlistHydrated: vi.fn(async () => ({ ok: true, imported: 0 })),
}));

vi.mock('@/lib/watchlist-market', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/watchlist-market')>();
  return {
    ...actual,
    fetchWatchlistMarketSnapshot: vi.fn(),
  };
});

vi.mock('@/lib/market-hours', () => ({
  getShanghaiTodayIso: vi.fn(() => '2026-06-18'),
  isShanghaiSyncWindow: vi.fn(() => false),
  isShanghaiTradingTime: vi.fn(() => false),
  isShanghaiQuoteWindow: vi.fn(() => false),
}));

const mockedApiGetJson = vi.mocked(apiGetJson);
const mockedFetchDashboardSummary = vi.mocked(fetchDashboardSummary);
const mockedFetchWatchlistMarketSnapshot = vi.mocked(fetchWatchlistMarketSnapshot);

describe('buildIndustryMarkdown', () => {
  it('renders industry fund flow sections from fixture', () => {
    const summary = {
      asOfDate: '2026-06-18',
      industryFundFlow: {
        asOfDate: '2026-06-18',
        dates: ['2026-06-14', '2026-06-15', '2026-06-16', '2026-06-17', '2026-06-18'],
        topByDate: [
          { date: '2026-06-18', top: ['Semiconductors', 'AI', 'EV', 'Pharma', 'Banking'] },
        ],
        flow5d: {
          dates: ['2026-06-14', '2026-06-15', '2026-06-16', '2026-06-17', '2026-06-18'],
          top: [
            {
              industryName: 'Semiconductors',
              sum5d: 3_000_000_000,
              series: [{ date: '2026-06-18', netInflow: 500_000_000 }],
            },
          ],
        },
      },
    };

    const md = buildIndustryMarkdown(summary);

    expect(md).toContain('## Industry fund flow');
    expect(md).toContain('- asOfDate: 2026-06-18');
    expect(md).toContain('## Top5×Date hotspots (names only)');
    expect(md).toContain('Semiconductors');
    expect(md).toContain('## 5D net inflow (Top by 5D sum)');
    expect(md).toContain('30.00亿');
    expect(md).toContain('5.00亿');
  });

  it('returns minimal markdown for null summary', () => {
    const md = buildIndustryMarkdown(null);
    expect(md).toContain('## Industry fund flow');
    expect(md).not.toContain('Top5×Date');
  });

  it('dedupes duplicate-date columns in 5D flow table (forward-fill guard)', () => {
    // Two consecutive dates with identical Top5 signature should collapse to one column.
    const summary = {
      asOfDate: '2026-06-25',
      industryFundFlow: {
        asOfDate: '2026-06-25',
        dates: ['2026-06-19', '2026-06-22', '2026-06-24', '2026-06-25'],
        topByDate: [
          { date: '2026-06-24', top: ['Electronics', 'Pharma', 'AI', 'Banking', 'EV'] },
          { date: '2026-06-25', top: ['Electronics', 'Pharma', 'AI', 'Banking', 'EV'] },
        ],
        flow5d: {
          dates: ['2026-06-19', '2026-06-22', '2026-06-24', '2026-06-25'],
          top: [
            {
              industryName: 'Electronics',
              sum5d: 28_223_000_000,
              series: [
                { date: '2026-06-24', netInflow: 28_223_000_000 },
                { date: '2026-06-25', netInflow: 28_223_000_000 },
              ],
            },
          ],
        },
        flow5dOut: { dates: [], top: [] },
      },
    };

    const md = buildIndustryMarkdown(summary);

    // The 5D flow table header should NOT contain both 06-24 and 06-25
    // because their Top5 signatures are identical (forward-fill duplicate).
    const flowSection = md.split('5D net inflow (Top by 5D sum)')[1] ?? '';
    expect(flowSection).toContain('06-24');
    expect(flowSection).not.toContain('06-25');
  });
});

describe('buildSentimentMarkdown', () => {
  it('renders market sentiment and index signals from fixture', () => {
    const summary = {
      asOfDate: '2026-06-18',
      marketEnvironmentZh: '市场震荡，控制仓位。',
      marketSentiment: {
        asOfDate: '2026-06-18',
        indexSignals: [
          {
            name: '上证指数',
            signal: 'yellow',
            positionRange: 'mid',
            pctChg: 0.35,
            close: 3200.5,
            ma5: 3180.2,
            ma20: 3150.1,
            asOfDate: '2026-06-18',
            source: 'cn_index',
          },
        ],
        items: [
          {
            date: '2026-06-18',
            upCount: 2500,
            downCount: 1800,
            flatCount: 200,
            upDownRatio: 1.39,
            marketTurnoverCny: 800_000_000_000,
            yesterdayLimitUpPremium: 2.5,
            failedLimitUpRate: 15.2,
            riskMode: 'neutral',
            rules: ['rule-a', 'rule-b'],
          },
        ],
        etfFundFlow: {
          asOfDate: '2026-06-18',
          shareLag: false,
          items: [
            {
              name: '沪深300 ETF',
              symbol: '510300',
              netFlow1d: 5_230_000_000,
              superLargeNetInflow: 3_100_000_000,
              largeNetInflow: 2_130_000_000,
              netFlow3d: 12_050_000_000,
              tradeTime: '2026-06-18T06:30:00+00:00',
              source: 'eastmoney.realtime_flow',
              live: true,
              flowStatus: 'Live',
              flowProvider: 'eastmoney',
              signal: 'National Team Buy',
              signalDisplay: '🛡️ National Team Buy',
            },
            {
              name: '半导体 ETF',
              symbol: '512480',
              netFlow1d: -1_240_000_000,
              superLargeNetInflow: -800_000_000,
              largeNetInflow: -440_000_000,
              netFlow3d: -3_520_000_000,
              tradeTime: '2026-06-18T06:30:00+00:00',
              source: 'eastmoney.realtime_flow',
              live: true,
              flowStatus: 'Live',
              flowProvider: 'eastmoney',
              signal: 'Inst Outflow',
              signalDisplay: '⚠️ Inst Outflow',
            },
          ],
        },
        srvIndex: {
          asOfDate: '2026-06-18',
          dates: ['2026-06-16', '2026-06-17', '2026-06-18'],
          score: 92.5,
          overlapCount: 0,
          overlapSectors: [],
          level: 'Extreme_High',
          labelZh: '恶性电风扇绞肉机',
        },
      },
      macroSnapshot: {
        macro: [
          {
            name: '300ETF Put IV',
            category: 'volatility',
            close: 18.5,
            signalLabel: 'calm',
            asOfDate: '2026-06-18',
            source: 'iv',
          },
        ],
      },
    };

    const md = buildSentimentMarkdown(summary);

    expect(md).toContain('## 市场环境摘要');
    expect(md).toContain('市场震荡，控制仓位。');
    expect(md).toContain('## Market sentiment');
    expect(md).toContain('- risk: neutral');
    expect(md).toContain(
      '- SRV 轮动指数: 92.5/100 极高（3D重叠 = 0）',
    );
    expect(md).toContain('## 市场环境摘要');
    expect(md).toContain('市场震荡，控制仓位。');
    expect(md).toContain('## Market sentiment');
    expect(md).toContain('- risk: neutral');
    expect(md).toContain(
      '- SRV 轮动指数: 92.5/100 极高（3D重叠 = 0）',
    );
    expect(md).not.toContain('## Index traffic lights');
    expect(md).not.toContain('## Market & Macro overview');
    expect(md).not.toContain('## 300ETF Put IV');
    expect(md).toContain('| date | up | down |');
    expect(md).toContain('## ETF Fund Flow (Top Watchlist)');
    expect(md).toContain(
      '| 沪深300 ETF | 510300 | +52.30亿 | +31.00亿 | +21.30亿 | +120.50亿 | 2026-06-18T06:30:00+00:00 | eastmoney.realtime_flow | Live |',
    );
    expect(md).toContain('🛡️ National Team Buy');
    expect(md).toContain('⚠️ Inst Outflow');
  });

  it('omits environment section when absent', () => {
    const md = buildSentimentMarkdown({ marketSentiment: { items: [] } });
    expect(md).not.toContain('市场环境摘要');
    expect(md).toContain('## Market sentiment');
  });

  it('renders FTD and macro lock lines for confirmed uptrend', () => {
    const md = buildSentimentMarkdown({
      marketSentiment: {
        items: [
          {
            date: '2026-06-27',
            upCount: 3200,
            downCount: 800,
            riskMode: 'confirmed_uptrend',
            rules: ['follow_through_day(...)'],
          },
        ],
      },
    });
    expect(md).toContain('- risk: confirmed_uptrend');
    expect(md).toContain('- ftd: triggered (右侧确立，死锁解除)');
  });

  it('renders macro lock line for extreme caution', () => {
    const md = buildSentimentMarkdown({
      marketSentiment: {
        items: [
          {
            date: '2026-06-27',
            upCount: 400,
            downCount: 4600,
            riskMode: 'extreme_caution',
            rules: ['breadth_panic(down>=3000 => red + extreme_caution)'],
          },
        ],
      },
    });
    expect(md).toContain('- macroLock: active');
  });

  it('omits Index traffic lights and Put IV blocks (moved to Market & Macro)', () => {
    const md = buildSentimentMarkdown({
      marketSentiment: {
        items: [],
        indexSignals: [
          { name: '上证指数', signal: 'green', positionRange: 'mid', pctChg: 0.5, close: 3200, asOfDate: '2026-08-01' },
        ],
      },
      macroSnapshot: {
        macro: [
          { name: '300ETF Put IV', category: 'volatility', close: 18.5, signalLabel: 'calm', asOfDate: '2026-08-01' },
        ],
      },
    });
    expect(md).not.toContain('## Index traffic lights');
    expect(md).not.toContain('## 300ETF Put IV');
  });
});

describe('buildMarketAndMacroMarkdown', () => {
  it('returns empty when no signals or macro', () => {
    expect(buildMarketAndMacroMarkdown({})).toBe('');
    expect(buildMarketAndMacroMarkdown(null)).toBe('');
  });

  it('combines index signals + macro items + put IV in one table', () => {
    const md = buildMarketAndMacroMarkdown({
      marketSentiment: {
        indexSignals: [
          { name: '上证指数', featured: true, signal: 'green', positionRange: '50%-60%', pctChg: 0.5, close: 3200, ma5: 3180, ma20: 3150, asOfDate: '2026-08-01', source: 'cn_index' },
          { name: '创业板指', signal: 'yellow', positionRange: '30%', pctChg: -0.3, close: 2100, ma5: 2110, ma20: 2150, asOfDate: '2026-08-01', source: 'cn_index' },
        ],
      },
      macroSnapshot: {
        macro: [
          { name: 'WTI 原油', category: 'commodity', pctChg: 1.5, close: 82.4, signal: 'up', asOfDate: '2026-08-01', source: 'macro' },
          { name: '300ETF Put IV', category: 'volatility', close: 18.5, signalLabel: 'calm', asOfDate: '2026-08-01', source: 'iv' },
        ],
      },
    });
    expect(md).toContain('## Market & Macro overview');
    expect(md).toContain('上证指数');
    expect(md).toContain('★ 上证指数');
    expect(md).toContain('创业板指');
    expect(md).toContain('WTI 原油');
    expect(md).toContain('300ETF Put IV');
    expect(md).toContain('| Index |');
    expect(md).toContain('| Macro |');
    expect(md).toContain('| Vol (IV) |');
    expect(md).toContain('| 50%-60% |');
    expect(md).toContain('| 30% |');
  });
});

describe('screener Top N + Score threshold constants', () => {
  it('exposes constants for filtering', () => {
    expect(SCREENER_COPY_TOP_N).toBe(10);
    expect(SCREENER_COPY_MIN_SCORE).toBe(60);
  });
});

describe('buildWatchlistMarkdown with QueryClient cache', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('uses fetchQuery cache instead of raw trendok requests', async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    });
    const snapshot = {
      trend: {
        'CN:000001': {
          symbol: 'CN:000001',
          name: 'Test',
          score: 90,
          asOfDate: '2026-06-18',
          values: { volumeRatio: 1.58 },
          missingData: [],
        },
      },
      quotes: {
        'CN:000001': {
          price: 10,
          tsCode: '000001.SZ',
          tradeTime: '2026-06-18 15:00:00',
          amount: 1000,
          volume: 100,
          preClose: 9.8,
          pctChg: 2,
        },
      },
    };

    await queryClient.setQueryData(watchlistMarketQueryOptions(['CN:000001']).queryKey, snapshot);

    const md = await buildWatchlistMarkdown(queryClient);

    expect(md).toContain('## Combat Positions & Watchlist（A股 / 港股 分表）');
    expect(md).toContain(
      '| Symbol | Name | RS | Score | TrendOK | Current | Pos% | CostPrice | P&L% | EntryDate | Locked_T1 | Action |',
    );
    expect(md).toContain('| CN:000001 | Test |');
    expect(md).not.toContain('## Watchlist\n');
    expect(md).not.toContain('## Positions (execution)');
    expect(mockedFetchWatchlistMarketSnapshot).not.toHaveBeenCalled();
    expect(mockedApiGetJson).not.toHaveBeenCalledWith(expect.stringContaining('/market/stocks/trendok'));
  });

  it('force refreshes watchlist bars when cached TrendOK has missing inputs', async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    });
    const key = watchlistMarketQueryOptions(['CN:000001']).queryKey;
    await queryClient.setQueryData(key, {
      trend: {
        'CN:000001': {
          symbol: 'CN:000001',
          name: 'Test',
          score: null,
          asOfDate: null,
          values: {},
          missingData: ['no_bars'],
        },
      },
      quotes: {},
    });
    mockedFetchWatchlistMarketSnapshot.mockResolvedValue({
      trend: {
        'CN:000001': {
          symbol: 'CN:000001',
          name: 'Test',
          score: 91,
          asOfDate: '2026-06-18',
          values: {},
          missingData: [],
        },
      },
      quotes: {},
      barSync: { failures: 0, total: 1 },
    });

    const md = await buildWatchlistMarkdown(queryClient);

    expect(mockedFetchWatchlistMarketSnapshot).toHaveBeenCalledWith(['CN:000001'], {
      forceMarket: true,
      realtime: false,
    });
    expect(queryClient.getQueryData(key)).toMatchObject({ barSync: { failures: 0, total: 1 } });
    expect(md).toContain('| CN:000001 | Test |');
  });

  it('forceFresh bypasses a healthy cache and refetches (TIP-014)', async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    });
    const key = watchlistMarketQueryOptions(['CN:000001']).queryKey;
    await queryClient.setQueryData(key, {
      trend: {
        'CN:000001': {
          symbol: 'CN:000001',
          name: 'Test',
          score: 90,
          asOfDate: '2026-06-18',
          values: { volumeRatio: 1.58 },
          missingData: [],
        },
      },
      quotes: {},
    });
    mockedFetchWatchlistMarketSnapshot.mockResolvedValue({
      trend: {
        'CN:000001': {
          symbol: 'CN:000001',
          name: 'Test',
          score: 92,
          asOfDate: '2026-06-19',
          values: { volumeRatio: 1.6 },
          missingData: [],
        },
      },
      quotes: {},
      barSync: { failures: 0, total: 1 },
    });

    const md = await buildWatchlistMarkdown(queryClient, null, null, false, true);

    expect(mockedFetchWatchlistMarketSnapshot).toHaveBeenCalledWith(['CN:000001'], {
      forceMarket: false,
      realtime: false,
    });
    expect(queryClient.getQueryData(key)).toMatchObject({
      trend: { 'CN:000001': { score: 92, asOfDate: '2026-06-19' } },
    });
    expect(md).toContain('| CN:000001 | Test |');
  });
});

describe('buildDashboardCopyAllMarkdown cache', () => {
  it('does not call raw dashboard summary when queryClient prefetched', async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    });
    const summary = {
      asOfDate: '2026-06-18',
      industryFundFlow: {
        asOfDate: '2026-06-18',
        dates: ['2026-06-17', '2026-06-18'],
        topByDate: [
          { date: '2026-06-17', top: ['半导体'] },
          { date: '2026-06-18', top: ['半导体'] },
        ],
        flow5d: {
          dates: ['2026-06-17', '2026-06-18'],
          top: [
            { industryName: '半导体', sum5d: 50e8 },
            { industryName: 'AI应用', sum5d: 40e8 },
            { industryName: '消费电子', sum5d: 30e8 },
          ],
        },
        dailyRankings: [
          {
            date: '2026-06-17',
            ranked: [{ industryName: '半导体', value: 10e8, rank: 1 }],
          },
          {
            date: '2026-06-18',
            ranked: [{ industryName: '半导体', value: 12e8, rank: 1 }],
          },
        ],
      },
      screeners: [],
      news: { hours: 24, total: 0 },
      marketSentiment: {
        executionGate: {
          mode: 'DEFEND',
          allowNewEntries: false,
          marketRegime: 'Weak',
          indexLight: 'red',
          srvLevel: 'Extreme_High',
          srvOverlapCount: 0,
          downCount: 1000,
          reasons: ['SRV_EXTREME_HIGH'],
          positionRangeHint: '0%-10%',
          satelliteNote: '防守优先',
        },
      },
    };

    mockedFetchDashboardSummary.mockResolvedValue(summary);

    await queryClient.setQueryData(['dashboard', 'summary', 'full'], summary);
    await queryClient.setQueryData(watchlistMarketQueryOptions(['CN:000001']).queryKey, {
      trend: {
        'CN:000001': {
          symbol: 'CN:000001',
          name: 'Test',
          score: 90,
          asOfDate: '2026-06-18',
          values: {},
          missingData: [],
        },
      },
      quotes: {},
    });

    const md = await buildDashboardCopyAllMarkdown({
      summary,
      queryClient,
    });

    expect(mockedFetchDashboardSummary).not.toHaveBeenCalled();
    expect(mockedApiGetJson).not.toHaveBeenCalledWith('/dashboard/summary');
    expect(md).not.toContain('## AI instructions (embedded)');
    expect(md).toContain('## Since last copy');
    expect(md).toContain('## Execution Gate');
    expect(md).toContain('- mode: DEFEND');
    expect(md).toContain('## Exec Attention');
    expect(md).toContain('### Must act');
    expect(md).toContain('### Fire');
    expect(md).toContain('Gate blocks new entries');
    expect(md).toContain('## Cond order draft');
    expect(md).toContain('## Combat Positions & Watchlist（A股 / 港股 分表）');
    expect(md).toContain('Mainline');
    expect(md).toContain('mainline bind');
    expect(md).toContain('INTRADAY_SURGE_BLOCK');
  });
});

describe('copyBlockingMissingData', () => {
  it('blocks only hard missing inputs, not optional instFlow', () => {
    expect(copyBlockingMissingData(['instFlow', 'stoploss_missing_inputs'])).toEqual([]);
    expect(copyBlockingMissingData(['no_bars', 'instFlow'])).toEqual(['no_bars']);
    expect(copyBlockingMissingData(['bars_lt_60'])).toEqual(['bars_lt_60']);
  });
});
