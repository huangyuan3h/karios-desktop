import { QueryClient } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

import {
  dashboardSummaryQueryKey,
  type DashboardSummary,
} from './dashboard';
import { fetchMacroSnapshotCached, macroSnapshotQueryKey } from './macro';

vi.mock('@/lib/endpoints', () => ({
  DATA_SYNC_BASE_URL: 'http://test-sync',
}));

describe('macroSnapshotQueryKey', () => {
  it('returns stable macro snapshot key', () => {
    expect(macroSnapshotQueryKey()).toEqual(['macro', 'snapshot']);
  });
});

describe('fetchMacroSnapshotCached', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('reads macroSnapshot from dashboard summary cache', async () => {
    const queryClient = new QueryClient();
    const macro = { cnIndexSignals: [{ tsCode: '000001.SH' }], macro: [] };
    queryClient.setQueryData<DashboardSummary>(dashboardSummaryQueryKey(true), {
      macroSnapshot: macro,
    });

    const result = await fetchMacroSnapshotCached(queryClient);
    expect(result).toEqual(macro);
  });

  it('falls back to macro endpoint when dashboard cache is empty', async () => {
    const queryClient = new QueryClient();
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ macro: [{ seriesId: 'DXY' }] }), { status: 200 }),
    );

    const result = await fetchMacroSnapshotCached(queryClient);

    expect(fetchMock).toHaveBeenCalledWith(`${DATA_SYNC_BASE_URL}/macro/snapshot`, {
      cache: 'no-store',
      signal: expect.any(AbortSignal),
    });
    expect(result.macro?.[0]?.seriesId).toBe('DXY');
  });
});
