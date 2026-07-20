'use client';

import { useQuery } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import type {
  ExecutionChangeListResponse,
  ExecutionSnapshotListResponse,
} from '@karios/shared';
import { getShanghaiTodayIso } from '@/lib/market-hours';

export function executionChangesKey(tradeDate: string) {
  return ['execution', 'changes', tradeDate] as const;
}

export function executionSnapshotsKey(tradeDate: string) {
  return ['execution', 'snapshots', tradeDate] as const;
}

export function useExecutionChangesQuery(tradeDate?: string) {
  const td = tradeDate ?? getShanghaiTodayIso();
  return useQuery({
    queryKey: executionChangesKey(td),
    queryFn: () =>
      apiGetJson<ExecutionChangeListResponse>(
        `/execution/changes?trade_date=${encodeURIComponent(td)}&limit=100`,
      ),
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
  });
}

export function useExecutionSnapshotsQuery(tradeDate?: string, limit = 20) {
  const td = tradeDate ?? getShanghaiTodayIso();
  return useQuery({
    queryKey: executionSnapshotsKey(td),
    queryFn: () =>
      apiGetJson<ExecutionSnapshotListResponse>(
        `/execution/snapshots?trade_date=${encodeURIComponent(td)}&limit=${limit}`,
      ),
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
  });
}

export function useExecutionRecentSnapshotsQuery(limit = 30) {
  return useQuery({
    queryKey: ['execution', 'snapshots', 'recent', limit] as const,
    queryFn: () =>
      apiGetJson<ExecutionSnapshotListResponse>(`/execution/snapshots?limit=${limit}`),
    refetchInterval: 120_000,
    refetchIntervalInBackground: false,
  });
}
