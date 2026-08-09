import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
  apiPostJson: vi.fn(),
  apiDeleteJson: vi.fn(),
}));
vi.mock('@tanstack/react-query', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@tanstack/react-query')>();
  return { ...actual, useQuery: vi.fn(() => ({ data: undefined })) };
});

import { apiDeleteJson, apiGetJson, apiPostJson } from '@/lib/api/client';
import { useQuery } from '@tanstack/react-query';

import {
  alphaRadarCatalystQueryKey,
  alphaRadarCatalystQueryOptions,
  alphaRadarRssQueryKey,
  alphaRadarRssQueryOptions,
  alphaRadarStatusQueryKey,
  alphaRadarStatusQueryOptions,
  alphaRadarTrendsQueryKey,
  alphaRadarTrendsQueryOptions,
  deleteAlphaRadarTrend,
  fetchAlphaRadarCatalyst,
  fetchAlphaRadarRss,
  fetchAlphaRadarStatus,
  fetchAlphaRadarTrends,
  invalidateAlphaRadarQueries,
  remapAlphaRadarTrend,
  runAlphaRadarPipeline,
  useAlphaRadarCatalystQuery,
  useAlphaRadarRssQuery,
  useAlphaRadarStatusQuery,
  useAlphaRadarTrendsQuery,
} from './alphaRadar';

const mockedApiGetJson = vi.mocked(apiGetJson);
const mockedApiPostJson = vi.mocked(apiPostJson);
const mockedApiDeleteJson = vi.mocked(apiDeleteJson);
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
  it('uses stable alpha radar keys', () => {
    expect(alphaRadarStatusQueryKey()).toEqual(['alphaRadar', 'status']);
    expect(alphaRadarTrendsQueryKey('all')).toEqual(['alphaRadar', 'trends', 'all']);
    expect(alphaRadarCatalystQueryKey(7)).toEqual(['alphaRadar', 'catalyst', 7]);
    expect(alphaRadarCatalystQueryKey()).toEqual(['alphaRadar', 'catalyst', 30]);
    expect(alphaRadarRssQueryKey()).toEqual(['alphaRadar', 'rss']);
  });
});

describe('fetch functions', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('fetchAlphaRadarStatus hits status endpoint', async () => {
    mockedApiGetJson.mockResolvedValue({ currentTrendCount: 3 });
    const out = await fetchAlphaRadarStatus();
    expect(out.currentTrendCount).toBe(3);
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe('/api/alpha-radar/status');
  });

  it('fetchAlphaRadarTrends batch vs all', async () => {
    mockedApiGetJson.mockResolvedValue({ total: 0, items: [] });
    await fetchAlphaRadarTrends('batch');
    expect(String(mockedApiGetJson.mock.calls[0][0])).toContain('latest_batch=true');
    await fetchAlphaRadarTrends('all');
    expect(String(mockedApiGetJson.mock.calls[1][0])).toContain('latest_batch=false');
  });

  it('fetchAlphaRadarCatalyst uses maxAgeDays', async () => {
    mockedApiGetJson.mockResolvedValue({ total: 0, maxAgeDays: 7, items: [] });
    await fetchAlphaRadarCatalyst();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toContain('maxAgeDays=30');
    await fetchAlphaRadarCatalyst(14);
    expect(String(mockedApiGetJson.mock.calls[1][0])).toContain('maxAgeDays=14');
  });

  it('fetchAlphaRadarRss merges documents and source names', async () => {
    mockedApiGetJson
      .mockResolvedValueOnce({ total: 2, items: [{ id: 'd1', sourceId: 's1' }] })
      .mockResolvedValueOnce({ sources: [{ id: 's1', name: '财联社' }] });
    const out = await fetchAlphaRadarRss();
    expect(out.documents).toEqual([{ id: 'd1', sourceId: 's1' }]);
    expect(out.total).toBe(2);
    expect(out.sourceNames).toEqual({ s1: '财联社' });
  });

  it('fetchAlphaRadarRss tolerates missing fields', async () => {
    mockedApiGetJson.mockResolvedValueOnce({}).mockResolvedValueOnce({ sources: null });
    const out = await fetchAlphaRadarRss();
    expect(out.documents).toEqual([]);
    expect(out.total).toBe(0);
    expect(out.sourceNames).toEqual({});
  });
});

describe('query options', () => {
  it('wires stale time and query fns', () => {
    expect(alphaRadarStatusQueryOptions().queryKey).toEqual(['alphaRadar', 'status']);
    expect(alphaRadarTrendsQueryOptions('batch').queryKey).toEqual([
      'alphaRadar',
      'trends',
      'batch',
    ]);
    expect(alphaRadarCatalystQueryOptions(14).queryKey).toEqual([
      'alphaRadar',
      'catalyst',
      14,
    ]);
    expect(alphaRadarRssQueryOptions().queryKey).toEqual(['alphaRadar', 'rss']);
  });
});

describe('hooks', () => {
  beforeEach(() => {
    mockedUseQuery.mockClear();
  });

  it('useAlphaRadarStatusQuery forwards options', () => {
    useAlphaRadarStatusQuery();
    expect(lastOptions().queryKey).toEqual(['alphaRadar', 'status']);
  });

  it('useAlphaRadarTrendsQuery forwards scope', () => {
    useAlphaRadarTrendsQuery('all');
    expect(lastOptions().queryKey).toEqual(['alphaRadar', 'trends', 'all']);
  });

  it('useAlphaRadarCatalystQuery forwards maxAgeDays', () => {
    useAlphaRadarCatalystQuery(14);
    expect(lastOptions().queryKey).toEqual(['alphaRadar', 'catalyst', 14]);
  });

  it('useAlphaRadarRssQuery defaults enabled true', () => {
    useAlphaRadarRssQuery();
    expect(lastOptions().enabled).toBe(true);
  });

  it('useAlphaRadarRssQuery honors disabled', () => {
    useAlphaRadarRssQuery({ enabled: false });
    expect(lastOptions().enabled).toBe(false);
  });
});

describe('mutations', () => {
  beforeEach(() => {
    mockedApiPostJson.mockReset();
    mockedApiDeleteJson.mockReset();
  });

  it('runAlphaRadarPipeline posts force flag', async () => {
    mockedApiPostJson.mockResolvedValue({ ok: true });
    await runAlphaRadarPipeline(true);
    expect(String(mockedApiPostJson.mock.calls[0][0])).toBe('/api/alpha-radar/run-pipeline');
    expect(mockedApiPostJson.mock.calls[0][1]).toEqual({ force: true });
  });

  it('deleteAlphaRadarTrend deletes encoded id', async () => {
    mockedApiDeleteJson.mockResolvedValue({ ok: true });
    await deleteAlphaRadarTrend('t/1');
    expect(String(mockedApiDeleteJson.mock.calls[0][0])).toBe(
      '/api/alpha-radar/trends/t%2F1',
    );
  });

  it('remapAlphaRadarTrend posts to remap endpoint', async () => {
    mockedApiPostJson.mockResolvedValue({ ok: true, cnSymbols: [] });
    await remapAlphaRadarTrend('t1');
    expect(String(mockedApiPostJson.mock.calls[0][0])).toBe(
      '/api/alpha-radar/trends/t1/remap',
    );
  });
});

describe('invalidateAlphaRadarQueries', () => {
  it('invalidates the alphaRadar subtree', async () => {
    const invalidateQueries = vi.fn().mockResolvedValue(undefined);
    await invalidateAlphaRadarQueries({ invalidateQueries } as never);
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['alphaRadar'] });
  });
});
