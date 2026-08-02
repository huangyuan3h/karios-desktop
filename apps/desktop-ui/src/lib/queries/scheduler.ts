'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';
import {
  SchedulerJobsResponseSchema,
  type SchedulerJobsResponse,
} from '@karios/shared';

import { apiGetJson, apiPostJson } from '@/lib/api/client';

const SCHEDULER_GET_OPTS = { timeoutMs: 30_000 } as const;
const SCHEDULER_POST_OPTS = { timeoutMs: 300_000 } as const;

export const SCHEDULER_POLL_MS = 60_000;

export function schedulerJobsQueryKey() {
  return ['scheduler', 'jobs'] as const;
}

export function schedulerJobsQueryOptions() {
  return {
    queryKey: schedulerJobsQueryKey(),
    queryFn: fetchSchedulerJobs,
    staleTime: SCHEDULER_POLL_MS,
    refetchInterval: SCHEDULER_POLL_MS,
    refetchIntervalInBackground: false,
  };
}

export function useSchedulerJobsQuery() {
  return useQuery(schedulerJobsQueryOptions());
}

export async function fetchSchedulerJobs(): Promise<SchedulerJobsResponse> {
  const raw = await apiGetJson<unknown>('/sync/jobs', SCHEDULER_GET_OPTS);
  return SchedulerJobsResponseSchema.parse(raw);
}

export async function invalidateSchedulerJobs(queryClient: QueryClient): Promise<void> {
  await queryClient.invalidateQueries({ queryKey: schedulerJobsQueryKey() });
}

export type SchedulerActionResult = {
  ok?: boolean;
  skipped?: boolean;
  message?: string;
  error?: string;
  updated?: number;
  updatedDailyRows?: number;
  updatedAdjFactorRows?: number;
  tradeDates?: string[];
  trendCount?: number;
  ingestStats?: { stored?: number; filteredOut?: number; fetched?: number };
  partial?: boolean;
  [key: string]: unknown;
};

/** Trigger a scheduler job's manual endpoint. Body is determined by endpoint. */
export async function triggerSchedulerAction(
  endpoint: string,
  method: 'POST' | 'GET' = 'POST',
  body?: unknown,
): Promise<SchedulerActionResult> {
  if (method === 'GET') {
    return apiGetJson<SchedulerActionResult>(endpoint, SCHEDULER_GET_OPTS);
  }
  return apiPostJson<SchedulerActionResult>(endpoint, body, SCHEDULER_POST_OPTS);
}
