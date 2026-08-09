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
  dashboardSentimentQueryKey,
  dashboardSentimentQueryOptions,
  fetchDashboardSentiment,
  useDashboardSentimentQuery,
} from './sentiment';

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

describe('dashboardSentimentQueryKey', () => {
  it('maps to the sentiment summary variant', () => {
    expect(dashboardSentimentQueryKey()).toEqual(['dashboard', 'summary', 'sentiment']);
  });
});

describe('fetchDashboardSentiment', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('requests sentiment-only summary path', async () => {
    mockedApiGetJson.mockResolvedValue({ marketSentiment: {} });
    await fetchDashboardSentiment();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/dashboard/summary?include_news=false&include_industry=false&include_screeners=false',
    );
  });
});

describe('dashboardSentimentQueryOptions', () => {
  it('wires key and queryFn', () => {
    const opts = dashboardSentimentQueryOptions();
    expect(opts.queryKey).toEqual(['dashboard', 'summary', 'sentiment']);
    expect(typeof opts.queryFn).toBe('function');
  });
});

describe('useDashboardSentimentQuery', () => {
  beforeEach(() => {
    mockedUseQuery.mockClear();
  });

  it('spreads options with refetch interval', () => {
    useDashboardSentimentQuery();
    const opts = lastOptions();
    expect(opts.queryKey).toEqual(['dashboard', 'summary', 'sentiment']);
    expect(typeof opts.refetchInterval).toBe('function');
    expect(opts.refetchIntervalInBackground).toBe(false);
  });
});
