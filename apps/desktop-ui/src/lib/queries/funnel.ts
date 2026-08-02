'use client';

import { useQuery } from '@tanstack/react-query';

import { fetchFunnelHistory, type AutomationRun } from '@/lib/watchlist-automation';

export const funnelHistoryKey = (limit: number) =>
  ['watchlist', 'funnel-history', limit] as const;

/** TIP-002 N-day funnel history (one row per trade_date, newest first). */
export function useFunnelHistoryQuery(limit = 10) {
  return useQuery({
    queryKey: funnelHistoryKey(limit),
    queryFn: () => fetchFunnelHistory(limit),
    staleTime: 120_000,
    refetchInterval: 300_000,
    refetchIntervalInBackground: false,
  });
}

export type { AutomationRun };
