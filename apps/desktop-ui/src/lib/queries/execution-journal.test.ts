import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/lib/market-hours', () => ({
  getShanghaiTodayIso: () => '2026-08-07',
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
  executionChangesKey,
  executionSnapshotsKey,
  useExecutionChangesQuery,
  useExecutionRecentSnapshotsQuery,
  useExecutionSnapshotsQuery,
} from './execution-journal';

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

describe('query keys', () => {
  it('builds stable keys', () => {
    expect(executionChangesKey('2026-08-07')).toEqual([
      'execution',
      'changes',
      '2026-08-07',
    ]);
    expect(executionSnapshotsKey('2026-08-07')).toEqual([
      'execution',
      'snapshots',
      '2026-08-07',
    ]);
  });
});

describe('useExecutionChangesQuery', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('uses provided trade date and polls every 60s', async () => {
    mockedApiGetJson.mockResolvedValue({ items: [] });
    useExecutionChangesQuery('2026-08-07');
    const opts = lastOptions();
    expect(opts.queryKey).toEqual(['execution', 'changes', '2026-08-07']);
    expect(opts.refetchInterval).toBe(60_000);
    await opts.queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/execution/changes?trade_date=2026-08-07&limit=100',
    );
  });

  it('falls back to Shanghai today', async () => {
    mockedApiGetJson.mockResolvedValue({ items: [] });
    useExecutionChangesQuery();
    await lastOptions().queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toContain('trade_date=2026-08-07');
  });
});

describe('useExecutionSnapshotsQuery', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('builds snapshots path with trade date and limit', async () => {
    mockedApiGetJson.mockResolvedValue({ items: [] });
    useExecutionSnapshotsQuery('2026-08-07', 20);
    await lastOptions().queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/execution/snapshots?trade_date=2026-08-07&limit=20',
    );
  });
});

describe('useExecutionRecentSnapshotsQuery', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('builds recent path without trade date and polls every 120s', async () => {
    mockedApiGetJson.mockResolvedValue({ items: [] });
    useExecutionRecentSnapshotsQuery(30);
    const opts = lastOptions();
    expect(opts.queryKey).toEqual(['execution', 'snapshots', 'recent', 30]);
    expect(opts.refetchInterval).toBe(120_000);
    await opts.queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/execution/snapshots?limit=30',
    );
  });
});
