'use client';

import { useWatchlistRiskQuery } from '@/lib/queries/dashboard';

export function useWatchlistRisk() {
  const watchlistRiskQuery = useWatchlistRiskQuery();
  const rows = watchlistRiskQuery.data ?? [];
  const busy = watchlistRiskQuery.isFetching;
  const updatedAt = watchlistRiskQuery.dataUpdatedAt
    ? new Date(watchlistRiskQuery.dataUpdatedAt).toISOString()
    : null;

  return {
    rows,
    busy,
    updatedAt,
    refetch: watchlistRiskQuery.refetch,
  };
}
