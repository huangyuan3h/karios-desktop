import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
  apiPostJson: vi.fn(),
}));
vi.mock('@tanstack/react-query', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@tanstack/react-query')>();
  return { ...actual, useQuery: vi.fn(() => ({ data: undefined })) };
});

import { apiGetJson, apiPostJson } from '@/lib/api/client';
import { useQuery } from '@tanstack/react-query';

import {
  fetchResearchReports,
  fetchResearchStats,
  invalidateResearchQueries,
  researchReportsQueryKey,
  researchReportsQueryOptions,
  researchStatsQueryKey,
  researchStatsQueryOptions,
  triggerResearchSync,
  useResearchReportsQuery,
  useResearchStatsQuery,
} from './research';

const mockedApiGetJson = vi.mocked(apiGetJson);
const mockedApiPostJson = vi.mocked(apiPostJson);
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
  it('distinguishes reports vs stats', () => {
    expect(researchReportsQueryKey(7, 50)).toEqual(['research', 'reports', 7, 50]);
    expect(researchReportsQueryKey(7)).toEqual(['research', 'reports', 7, 50]);
    expect(researchStatsQueryKey()).toEqual(['research', 'stats']);
  });
});

describe('fetch functions', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('fetchResearchReports uses defaults', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, reports: [] });
    await fetchResearchReports();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/research/reports?limit=50&days=7',
    );
  });

  it('fetchResearchReports uses custom params', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, reports: [] });
    await fetchResearchReports(14, 100);
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/research/reports?limit=100&days=14',
    );
  });

  it('fetchResearchStats hits stats endpoint', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, stats: { total: 1 } });
    const out = await fetchResearchStats();
    expect(out.stats.total).toBe(1);
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe('/api/research/stats');
  });
});

describe('query options', () => {
  it('wires stale time and keys', () => {
    expect(researchReportsQueryOptions(7, 50).queryKey).toEqual([
      'research',
      'reports',
      7,
      50,
    ]);
    expect(researchStatsQueryOptions().queryKey).toEqual(['research', 'stats']);
  });
});

describe('hooks', () => {
  beforeEach(() => {
    mockedUseQuery.mockClear();
  });

  it('useResearchReportsQuery forwards params', () => {
    useResearchReportsQuery(14, 100);
    expect(lastOptions().queryKey).toEqual(['research', 'reports', 14, 100]);
  });

  it('useResearchStatsQuery forwards options', () => {
    useResearchStatsQuery();
    expect(lastOptions().queryKey).toEqual(['research', 'stats']);
  });
});

describe('triggerResearchSync', () => {
  beforeEach(() => {
    mockedApiPostJson.mockReset();
  });

  it('posts with days param', async () => {
    mockedApiPostJson.mockResolvedValue({ ok: true });
    await triggerResearchSync(7);
    expect(String(mockedApiPostJson.mock.calls[0][0])).toBe('/api/research/sync?days=7');
  });

  it('defaults to 3 days', async () => {
    mockedApiPostJson.mockResolvedValue({ ok: true });
    await triggerResearchSync();
    expect(String(mockedApiPostJson.mock.calls[0][0])).toBe('/api/research/sync?days=3');
  });
});

describe('invalidateResearchQueries', () => {
  it('invalidates the research subtree', async () => {
    const invalidateQueries = vi.fn().mockResolvedValue(undefined);
    await invalidateResearchQueries({ invalidateQueries } as never);
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['research'] });
  });
});
