import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { AutomationRun } from '@/lib/watchlist-automation';

import { PoolHistoryTable, toPoolRow } from './PoolHistoryTable';

const { useFunnelHistoryQuery } = vi.hoisted(() => ({
  useFunnelHistoryQuery: vi.fn(),
}));
vi.mock('@/lib/queries/funnel', () => ({ useFunnelHistoryQuery }));

function run(meta: Record<string, unknown>, tradeDate = '2026-09-15'): AutomationRun {
  return { runId: 'r1', tradeDate, meta } as AutomationRun;
}

describe('toPoolRow', () => {
  it('maps the OPT-209 pool meta', () => {
    const row = toPoolRow(
      run({
        s3PoolSize: 10,
        satellitePoolSize: 4,
        poolAdded: { s3: 3, satellite: 4 },
        poolRemoved: { s3: 2, satellite: 1 },
      }),
    );
    expect(row).toEqual({
      tradeDate: '2026-09-15',
      s3Size: 10,
      satelliteSize: 4,
      addedS3: 3,
      addedSatellite: 4,
      removedS3: 2,
      removedSatellite: 1,
    });
  });

  it('returns null for legacy funnel runs and empty meta', () => {
    expect(toPoolRow(run({ funnel: { tvHit: 10 } }))).toBeNull();
    expect(toPoolRow(run({}))).toBeNull();
    expect(toPoolRow({ runId: 'x' } as AutomationRun)).toBeNull();
  });
});

function renderTable() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <PoolHistoryTable />
    </QueryClientProvider>,
  );
}

describe('PoolHistoryTable', () => {
  beforeEach(() => {
    useFunnelHistoryQuery.mockReset();
  });

  it('renders per-day pool sizes and add/remove counts', () => {
    useFunnelHistoryQuery.mockReturnValue({
      data: [
        run({
          s3PoolSize: 10,
          satellitePoolSize: 4,
          poolAdded: { s3: 3, satellite: 4 },
          poolRemoved: { s3: 2, satellite: 1 },
        }),
      ],
      isFetching: false,
    });
    renderTable();
    expect(screen.getByText('2026-09-15')).toBeDefined();
    expect(screen.getByText('+3')).toBeDefined();
    expect(screen.getByText('+4')).toBeDefined();
    expect(screen.getByText('−2')).toBeDefined();
    expect(screen.getByText('−1')).toBeDefined();
    expect(screen.getByText(/池 = 两条回测腿/)).toBeDefined();
  });

  it('filters legacy funnel-only runs out', () => {
    useFunnelHistoryQuery.mockReturnValue({
      data: [run({ funnel: { tvHit: 10 } })],
      isFetching: false,
    });
    renderTable();
    expect(screen.getByText(/暂无池变动记录/)).toBeDefined();
  });
});
