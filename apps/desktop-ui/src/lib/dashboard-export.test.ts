import { describe, expect, it, vi } from 'vitest';
import { QueryClient } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import { fetchDashboardSummary } from '@/lib/queries/dashboard';
import { watchlistMarketQueryOptions } from '@/lib/queries/watchlist';

import {
  buildDashboardCopyAllMarkdown,
  buildIndustryMarkdown,
  buildSentimentMarkdown,
  buildWatchlistMarkdown,
} from './dashboard-export';

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
  buildCatalystStocksMarkdown: vi.fn(() => '## Catalyst\n'),
  buildAlphaRadarTrendsMarkdown: vi.fn(() => '## Trends\n'),
  DEFAULT_CATALYST_MAX_AGE_DAYS: 7,
  fetchAlphaRadarTrendsForCopy: vi.fn(async () => ({ items: [], scope: 'latest' })),
  fetchCatalystStocks: vi.fn(async () => ({ items: [] })),
  normalizeCatalystSymbol: vi.fn((s: string) => s),
}));

vi.mock('@/lib/watchlist-storage', () => ({
  loadWatchlist: vi.fn(() => [{ symbol: 'CN:000001', name: 'Test' }]),
  ensureWatchlistHydrated: vi.fn(async () => ({ ok: true, imported: 0 })),
}));

vi.mock('@/lib/market-hours', () => ({
  getShanghaiTodayIso: vi.fn(() => '2026-06-18'),
  isShanghaiSyncWindow: vi.fn(() => false),
  isShanghaiTradingTime: vi.fn(() => false),
  isShanghaiQuoteWindow: vi.fn(() => false),
}));

const mockedApiGetJson = vi.mocked(apiGetJson);
const mockedFetchDashboardSummary = vi.mocked(fetchDashboardSummary);

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
      },
    };

    const md = buildSentimentMarkdown(summary);

    expect(md).toContain('## 市场环境摘要');
    expect(md).toContain('市场震荡，控制仓位。');
    expect(md).toContain('## Market sentiment');
    expect(md).toContain('- risk: neutral');
    expect(md).toContain('## Index traffic lights');
    expect(md).toContain('上证指数');
    expect(md).toContain('+0.35%');
    expect(md).toContain('| date | up | down |');
  });

  it('omits environment section when absent', () => {
    const md = buildSentimentMarkdown({ marketSentiment: { items: [] } });
    expect(md).not.toContain('市场环境摘要');
    expect(md).toContain('## Market sentiment');
  });
});

describe('buildWatchlistMarkdown with QueryClient cache', () => {
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
          values: {},
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

    expect(md).toContain('## Watchlist');
    expect(mockedApiGetJson).not.toHaveBeenCalledWith(expect.stringContaining('/market/stocks/trendok'));
  });
});

describe('buildDashboardCopyAllMarkdown cache', () => {
  it('does not call raw dashboard summary when queryClient prefetched', async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    });
    const summary = {
      asOfDate: '2026-06-18',
      industryFundFlow: { asOfDate: '2026-06-18', dates: [], topByDate: [] },
      screeners: [],
      news: { hours: 24, total: 0 },
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

    await buildDashboardCopyAllMarkdown({
      summary,
      queryClient,
    });

    expect(mockedFetchDashboardSummary).not.toHaveBeenCalled();
    expect(mockedApiGetJson).not.toHaveBeenCalledWith('/dashboard/summary');
  });
});
