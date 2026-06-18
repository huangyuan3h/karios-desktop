'use client';

import { useQuery } from '@tanstack/react-query';

import { fetchAutomationPending, isAutomationPollWindow } from '@/lib/watchlist-automation';

export const AUTOMATION_POLL_MS = 60_000;

export function automationPendingQueryKey() {
  return ['watchlist', 'automation', 'pending'] as const;
}

export function automationPendingQueryOptions() {
  return {
    queryKey: automationPendingQueryKey(),
    queryFn: () => fetchAutomationPending(),
    staleTime: AUTOMATION_POLL_MS,
  };
}

export function useAutomationPendingQuery() {
  return useQuery({
    ...automationPendingQueryOptions(),
    enabled: isAutomationPollWindow(),
    refetchInterval: AUTOMATION_POLL_MS,
    refetchIntervalInBackground: false,
  });
}
