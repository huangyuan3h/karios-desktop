import { QueryClient } from '@tanstack/react-query';
import { describe, expect, it, vi } from 'vitest';

import type { WatchlistMarketSnapshot } from '@/lib/watchlist-market';

import {
  buildDashboardSummaryPath,
  buildWatchlistRiskRowsFromSnapshot,
  dashboardSummaryQueryKey,
  fetchWatchlistRiskRows,
} from './dashboard';
import { watchlistMarketKey } from './watchlist';

vi.mock('@/lib/watchlist-storage', () => ({
  loadWatchlist: vi.fn(() => [{ symbol: 'CN:600519', name: 'Moutai' }]),
}));

describe('dashboardSummaryQueryKey', () => {
  it('distinguishes macro full vs lite', () => {
    expect(dashboardSummaryQueryKey(true)).toEqual(['dashboard', 'summary', 'full']);
    expect(dashboardSummaryQueryKey(false)).toEqual(['dashboard', 'summary', 'lite']);
  });
});

describe('buildDashboardSummaryPath', () => {
  it('omits macro when includeMacro is false', () => {
    expect(buildDashboardSummaryPath(false)).toBe('/dashboard/summary?include_macro=false');
  });

  it('uses full path when includeMacro is true', () => {
    expect(buildDashboardSummaryPath(true)).toBe('/dashboard/summary');
  });
});

describe('buildWatchlistRiskRowsFromSnapshot', () => {
  it('keeps rows with alerts and sorts block severity first', () => {
    const snapshot: WatchlistMarketSnapshot = {
      trend: {
        'CN:600519': {
          symbol: 'CN:600519',
          name: 'Moutai',
          riskAlerts: [{ code: 'intraday_surge', severity: 'block', message: 'surge' }],
        },
        'CN:000001': {
          symbol: 'CN:000001',
          name: 'Ping An',
          riskAlerts: [{ code: 'gap_up', severity: 'warn', message: 'gap' }],
        },
        'CN:999999': {
          symbol: 'CN:999999',
          name: 'Quiet',
        },
      },
      quotes: {},
    };

    const rows = buildWatchlistRiskRowsFromSnapshot(
      [
        { symbol: 'CN:000001', name: 'Ping An' },
        { symbol: 'CN:600519', name: 'Moutai' },
        { symbol: 'CN:999999', name: 'Quiet' },
      ],
      snapshot,
    );

    expect(rows.map((r) => r.symbol)).toEqual(['CN:600519', 'CN:000001']);
    expect(rows[0].alerts.some((a) => a.severity === 'block')).toBe(true);
  });
});

describe('fetchWatchlistRiskRows', () => {
  it('uses watchlistMarketKey via queryClient.fetchQuery', async () => {
    const snapshot: WatchlistMarketSnapshot = {
      trend: {
        'CN:600519': {
          symbol: 'CN:600519',
          name: 'Moutai',
          riskAlerts: [{ code: 'test', severity: 'warn', message: 'x' }],
        },
      },
      quotes: {},
    };
    const fetchQuery = vi.fn().mockResolvedValue(snapshot);
    const queryClient = { fetchQuery } as unknown as QueryClient;

    const rows = await fetchWatchlistRiskRows(queryClient);

    expect(fetchQuery).toHaveBeenCalledTimes(1);
    expect(fetchQuery.mock.calls[0][0].queryKey).toEqual(watchlistMarketKey(['CN:600519']));
    expect(rows).toHaveLength(1);
    expect(rows[0].symbol).toBe('CN:600519');
  });
});
