import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
}));
vi.mock('@tanstack/react-query', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@tanstack/react-query')>();
  return { ...actual, useQuery: vi.fn(() => ({ data: undefined })) };
});

import { apiGetJson } from '@/lib/api/client';
import { useQuery } from '@tanstack/react-query';

import { useWeeklyReviewQuery } from './weekly-review';

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

describe('useWeeklyReviewQuery', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('enabled by default with 5min stale time', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, markdown: '' });
    useWeeklyReviewQuery();
    const opts = lastOptions();
    expect(opts.queryKey).toEqual(['weekly-review']);
    expect(opts.enabled).toBe(true);
    expect(opts.staleTime).toBe(5 * 60_000);
    await opts.queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/backtest/weekly-review',
    );
  });

  it('honors disabled flag', () => {
    useWeeklyReviewQuery(false);
    expect(lastOptions().enabled).toBe(false);
  });
});
