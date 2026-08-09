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
  deleteUserTrade,
  fetchUserTrades,
  fetchUserTradesStats,
  invalidateUserTradesQueries,
  recordUserTrade,
  useUserTradesListQuery,
  useUserTradesStatsQuery,
  userTradesListQueryKey,
  userTradesStatsQueryKey,
} from './userTrades';

const mockedApiGetJson = vi.mocked(apiGetJson);
const mockedApiPostJson = vi.mocked(apiPostJson);
const mockedApiDeleteJson = vi.mocked(apiDeleteJson);
const mockedUseQuery = vi.mocked(useQuery);

type CapturedOptions = {
  queryKey: unknown;
  queryFn: () => Promise<unknown>;
  staleTime?: number;
};

function lastOptions(): CapturedOptions {
  return mockedUseQuery.mock.calls[mockedUseQuery.mock.calls.length - 1][0] as CapturedOptions;
}

describe('query keys', () => {
  it('builds stable keys', () => {
    expect(userTradesListQueryKey()).toEqual(['userTrades', 'list', 50]);
    expect(userTradesListQueryKey(10)).toEqual(['userTrades', 'list', 10]);
    expect(userTradesStatsQueryKey()).toEqual(['userTrades', 'stats']);
  });
});

describe('fetch functions', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedApiPostJson.mockReset();
    mockedApiDeleteJson.mockReset();
  });

  it('fetchUserTrades returns trades array', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, trades: [{ id: 't1' }], count: 1 });
    const rows = await fetchUserTrades(25);
    expect(rows).toEqual([{ id: 't1' }]);
    expect(mockedApiGetJson).toHaveBeenCalledWith('/trades?limit=25');
  });

  it('fetchUserTradesStats returns stats', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, stats: { total: 3 } });
    const stats = await fetchUserTradesStats();
    expect(stats).toEqual({ total: 3 });
    expect(mockedApiGetJson).toHaveBeenCalledWith('/trades/stats');
  });

  it('recordUserTrade posts and returns the trade', async () => {
    const leg = {
      symbol: 'CN:600000',
      side: 'SELL' as const,
      price: 11,
      positionPct: 5,
      costBasis: 10,
      entryDate: '2026-08-01',
    };
    mockedApiPostJson.mockResolvedValue({ ok: true, trade: { id: 't1', ...leg } });
    const trade = await recordUserTrade(leg);
    expect(trade.id).toBe('t1');
    expect(mockedApiPostJson).toHaveBeenCalledWith('/trades', leg);
  });

  it('deleteUserTrade deletes by id', async () => {
    mockedApiDeleteJson.mockResolvedValue({ ok: true });
    await deleteUserTrade('t1');
    expect(mockedApiDeleteJson).toHaveBeenCalledWith('/trades/t1');
  });

  it('invalidateUserTradesQueries invalidates the prefix', async () => {
    const invalidateQueries = vi.fn().mockResolvedValue(undefined);
    await invalidateUserTradesQueries({ invalidateQueries } as never);
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['userTrades'] });
  });
});

describe('hooks', () => {
  beforeEach(() => {
    mockedUseQuery.mockReset();
    mockedUseQuery.mockImplementation(() => ({ data: undefined } as never));
  });

  it('useUserTradesListQuery wires options', () => {
    useUserTradesListQuery(30);
    const opts = lastOptions();
    expect(opts.queryKey).toEqual(['userTrades', 'list', 30]);
    expect(typeof opts.queryFn).toBe('function');
  });

  it('useUserTradesStatsQuery wires options', () => {
    useUserTradesStatsQuery();
    const opts = lastOptions();
    expect(opts.queryKey).toEqual(['userTrades', 'stats']);
    expect(typeof opts.queryFn).toBe('function');
  });
});
