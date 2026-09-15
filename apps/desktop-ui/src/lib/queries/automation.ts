'use client';

import { useQuery } from '@tanstack/react-query';

import {
  fetchAutomationLatest,
  fetchAutomationPending,
  isAutomationPollWindow,
} from '@/lib/watchlist-automation';

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

export function automationLatestQueryKey() {
  return ['watchlist', 'automation', 'latest'] as const;
}

/**
 * OPT-209: the backend auto-applies pool runs, so the client watches the latest
 * *applied* run and refreshes the registry when a new one lands.
 */
export function useAutomationLatestQuery() {
  return useQuery({
    queryKey: automationLatestQueryKey(),
    queryFn: () => fetchAutomationLatest(),
    staleTime: AUTOMATION_POLL_MS,
    enabled: isAutomationPollWindow(),
    refetchInterval: AUTOMATION_POLL_MS,
    refetchIntervalInBackground: false,
  });
}
