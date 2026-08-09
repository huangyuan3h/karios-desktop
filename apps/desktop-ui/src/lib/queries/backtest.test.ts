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

import {
  clusterExposureForSymbol,
  useBacktestRunQuery,
  useCorrelationStatusQuery,
  useExitAttributionQuery,
  useSensitivityQuery,
  type CorrelationStatusResponse,
} from './backtest';

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

const PARAMS = {
  start: '2026-01-01',
  end: '2026-06-01',
  scoreThreshold: 80,
  maxHoldDays: 5,
  stopLossPct: -5,
  gates: 'full',
};

describe('useBacktestRunQuery', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('builds run path with all params and zero stale time', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, summary: {} });
    useBacktestRunQuery(PARAMS);
    const opts = lastOptions();
    expect(opts.queryKey).toEqual(['backtest', 'run', PARAMS, 0]);
    expect(opts.staleTime).toBe(0);
    await opts.queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/backtest/run?start=2026-01-01&end=2026-06-01&score_threshold=80&max_hold_days=5&stop_loss_pct=-5&gates=full',
    );
  });
});

describe('useSensitivityQuery', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('passes start/end, stale time and enabled flag', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, configs: 1, results: [] });
    useSensitivityQuery('2026-01-01', '2026-06-01', false);
    const opts = lastOptions();
    expect(opts.enabled).toBe(false);
    expect(opts.staleTime).toBe(60_000);
    await opts.queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/backtest/sensitivity?start=2026-01-01&end=2026-06-01',
    );
  });
});

describe('useExitAttributionQuery', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('defaults to 5 days and enabled', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, days: 5 });
    useExitAttributionQuery();
    const opts = lastOptions();
    expect(opts.enabled).toBe(true);
    expect(opts.staleTime).toBe(30_000);
    await opts.queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/backtest/exit-attribution?days=5',
    );
  });

  it('uses custom days and disabled flag', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, days: 10 });
    useExitAttributionQuery(10, false);
    expect(lastOptions().enabled).toBe(false);
  });
});

describe('useCorrelationStatusQuery', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    mockedUseQuery.mockClear();
  });

  it('defaults include_matrix to true', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true });
    useCorrelationStatusQuery();
    await lastOptions().queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/backtest/correlation-status?include_matrix=true',
    );
  });

  it('encodes include_matrix false', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true });
    useCorrelationStatusQuery(false, false);
    await lastOptions().queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/backtest/correlation-status?include_matrix=false',
    );
  });
});

describe('clusterExposureForSymbol', () => {
  const status: CorrelationStatusResponse = {
    ok: true,
    capPct: 30,
    clusters: {
      baijiu: {
        label: '白酒',
        exposurePct: 25,
        symbols: ['CN:600519', 'CN:000858'],
        industries: ['白酒'],
      },
    },
    overLimit: [],
    blockedSymbols: [],
    topPairs: [],
    empiricalNote: null,
  };

  it('returns null for undefined status', () => {
    expect(clusterExposureForSymbol(undefined, 'CN:600519')).toBeNull();
  });

  it('normalizes symbol case and whitespace', () => {
    expect(clusterExposureForSymbol(status, '  cn:600519  ')).toBe(25);
  });

  it('returns null when symbol not in any cluster', () => {
    expect(clusterExposureForSymbol(status, 'HK:00700')).toBeNull();
  });
});
