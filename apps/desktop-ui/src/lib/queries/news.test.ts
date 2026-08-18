import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/lib/market-hours', () => ({
  isShanghaiSyncWindow: () => true,
}));
vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
}));
vi.mock('@tanstack/react-query', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@tanstack/react-query')>();
  return { ...actual, useQuery: vi.fn(() => ({ data: undefined })) };
});

import { apiGetJson } from '@/lib/api/client';
import { useQuery } from '@tanstack/react-query';

import {
  buildDashboardSummaryPath,
  dashboardLiteQueryKey,
  dashboardSummaryQueryKey,
  DASHBOARD_LITE_INCLUDES,
  DASHBOARD_NEWS_INCLUDES,
  DASHBOARD_SENTIMENT_INCLUDES,
} from './dashboard';
import {
  dashboardNewsQueryKey,
  dashboardNewsQueryOptions,
  fetchDashboardNews,
  fetchMorningBrief,
  fetchNewsItems,
  fetchNewsSources,
  invalidateNewsPageQueries,
  morningBriefQueryKey,
  morningBriefQueryOptions,
  newsItemsQueryKey,
  newsItemsQueryOptions,
  newsSourcesQueryKey,
  newsSourcesQueryOptions,
  useDashboardNewsQuery,
  useMorningBriefQuery,
  useNewsItemsQuery,
  useNewsSourcesQuery,
} from './news';
import { dashboardSentimentQueryKey } from './sentiment';

const mockedApiGetJson = vi.mocked(apiGetJson);
const mockedUseQuery = vi.mocked(useQuery);

type CapturedOptions = {
  queryKey: unknown;
  queryFn: () => Promise<unknown>;
  enabled?: boolean;
  staleTime?: number;
  refetchInterval?: number | boolean | ((...args: unknown[]) => unknown);
  refetchIntervalInBackground?: boolean;
};

function lastOptions(): CapturedOptions {
  return mockedUseQuery.mock.calls[mockedUseQuery.mock.calls.length - 1][0] as CapturedOptions;
}

describe('buildDashboardSummaryPath', () => {
  it('builds lite path without macro, sentiment, or news', () => {
    expect(buildDashboardSummaryPath(DASHBOARD_LITE_INCLUDES)).toBe(
      '/dashboard/summary?include_macro=false&include_sentiment=false&include_news=false',
    );
  });

  it('omits macro when includeMacro is false', () => {
    expect(buildDashboardSummaryPath(false)).toBe(
      '/dashboard/summary?include_macro=false',
    );
  });

  it('uses full path when all blocks are included', () => {
    expect(buildDashboardSummaryPath(true)).toBe('/dashboard/summary');
  });
});

describe('dashboardSummaryQueryKey', () => {
  it('distinguishes full vs lite vs partial variants', () => {
    expect(dashboardSummaryQueryKey(true)).toEqual(['dashboard', 'summary', 'full']);
    expect(dashboardSummaryQueryKey(false)).toEqual(['dashboard', 'summary', 'no-macro']);
    expect(dashboardLiteQueryKey()).toEqual(['dashboard', 'summary', 'lite']);
  });
});

describe('sub query keys', () => {
  it('uses stable sentiment and news keys', () => {
    expect(dashboardSentimentQueryKey()).toEqual(['dashboard', 'summary', 'sentiment']);
    expect(dashboardNewsQueryKey()).toEqual(['dashboard', 'summary', 'news']);
    expect(newsItemsQueryKey(24, 100)).toEqual(['news', 'items', 24, 100]);
  });
});

describe('partial include presets', () => {
  it('defines sentiment-only and news-only presets', () => {
    expect(DASHBOARD_SENTIMENT_INCLUDES.includeSentiment).toBe(true);
    expect(DASHBOARD_SENTIMENT_INCLUDES.includeMacro).toBe(true);
    expect(DASHBOARD_NEWS_INCLUDES.includeNews).toBe(true);
    expect(DASHBOARD_LITE_INCLUDES.includeSentiment).toBe(false);
  });
});

describe('dashboard news query', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('fetchDashboardNews requests news-only path', async () => {
    mockedApiGetJson.mockResolvedValue({ news: [] });
    await fetchDashboardNews();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/dashboard/summary?include_macro=false&include_sentiment=false&include_industry=false&include_screeners=false',
    );
  });

  it('dashboardNewsQueryOptions wires key and fn', () => {
    const opts = dashboardNewsQueryOptions();
    expect(opts.queryKey).toEqual(['dashboard', 'summary', 'news']);
    expect(typeof opts.queryFn).toBe('function');
  });

  it('useDashboardNewsQuery spreads refetch interval', () => {
    useDashboardNewsQuery();
    expect(lastOptions().queryKey).toEqual(['dashboard', 'summary', 'news']);
    expect(typeof lastOptions().refetchInterval).toBe('function');
  });
});

describe('news items/sources', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('fetchNewsItems builds limit and hours params', async () => {
    mockedApiGetJson.mockResolvedValue({ total: 0, items: [] });
    await fetchNewsItems(24, 50);
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/news/items?limit=50&hours=24',
    );
  });

  it('fetchNewsSources hits sources endpoint', async () => {
    mockedApiGetJson.mockResolvedValue({ sources: [] });
    await fetchNewsSources();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe('/api/news/sources');
  });

  it('newsItemsQueryOptions default limit', () => {
    expect(newsItemsQueryOptions(12).queryKey).toEqual(['news', 'items', 12, 100]);
  });

  it('useNewsItemsQuery / useNewsSourcesQuery forward options', () => {
    useNewsItemsQuery(6, 20);
    expect(lastOptions().queryKey).toEqual(['news', 'items', 6, 20]);
    useNewsSourcesQuery();
    expect(lastOptions().queryKey).toEqual(['news', 'sources']);
  });
});

describe('invalidateNewsPageQueries', () => {
  it('invalidates the news subtree', async () => {
    const invalidateQueries = vi.fn().mockResolvedValue(undefined);
    await invalidateNewsPageQueries({ invalidateQueries } as never);
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['news'] });
  });
});

describe('morning brief', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('query key is stable', () => {
    expect(morningBriefQueryKey()).toEqual(['news', 'brief', 'latest']);
  });

  it('fetchMorningBrief hits latest endpoint', async () => {
    mockedApiGetJson.mockResolvedValue({ brief: null });
    await fetchMorningBrief();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe('/api/news/brief/latest?brief_type=morning');
  });

  it('morningBriefQueryOptions uses 60s stale', () => {
    const opts = morningBriefQueryOptions();
    expect(opts.queryKey).toEqual(['news', 'brief', 'latest']);
    expect(opts.staleTime).toBe(60_000);
  });

  it('useMorningBriefQuery forwards options', () => {
    useMorningBriefQuery();
    expect(lastOptions().queryKey).toEqual(['news', 'brief', 'latest']);
  });
});
