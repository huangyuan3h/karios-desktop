'use client';

import { useQuery } from '@tanstack/react-query';

import {
  DASHBOARD_SENTIMENT_INCLUDES,
  dashboardSummaryQueryKey,
  fetchDashboardSummaryPartial,
  type DashboardSummary,
} from './dashboard';
import { dashboardRefetchIntervalMs } from './intervals';

export function dashboardSentimentQueryKey() {
  return dashboardSummaryQueryKey(DASHBOARD_SENTIMENT_INCLUDES);
}

export async function fetchDashboardSentiment(): Promise<DashboardSummary> {
  return fetchDashboardSummaryPartial(DASHBOARD_SENTIMENT_INCLUDES);
}

export function dashboardSentimentQueryOptions() {
  return {
    queryKey: dashboardSentimentQueryKey(),
    queryFn: fetchDashboardSentiment,
  };
}

export function useDashboardSentimentQuery() {
  return useQuery({
    ...dashboardSentimentQueryOptions(),
    refetchInterval: dashboardRefetchIntervalMs,
    refetchIntervalInBackground: false,
  });
}
