'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';

import { apiGetJson, apiPostJson } from '@/lib/api/client';

import { SCREENER_STALE_MS } from './intervals';

export type ResearchReport = {
  id: number;
  infoCode: string;
  stockCode: string;
  stockName: string;
  title: string;
  orgName: string;
  rating: string | null;
  targetPrice: number | null;
  epsThisYear: number | null;
  peThisYear: number | null;
  industryName: string | null;
  market: string;
  publishDate: string;
  encodeUrl: string | null;
  source: string;
  alphaScore: number | null;
  createdAt: string;
};

export type ResearchReportsResponse = {
  ok: boolean;
  reports: ResearchReport[];
};

export type ResearchStats = {
  total: number;
  last24h: number;
  last7d: number;
  stocks7d: number;
};

export type ResearchStatsResponse = {
  ok: boolean;
  stats: ResearchStats;
};

export function researchReportsQueryKey(days: number, limit = 50) {
  return ['research', 'reports', days, limit] as const;
}

export function researchStatsQueryKey() {
  return ['research', 'stats'] as const;
}

export async function fetchResearchReports(
  days = 7,
  limit = 50,
): Promise<ResearchReportsResponse> {
  return apiGetJson<ResearchReportsResponse>(
    `/api/research/reports?limit=${limit}&days=${days}`,
  );
}

export async function fetchResearchStats(): Promise<ResearchStatsResponse> {
  return apiGetJson<ResearchStatsResponse>('/api/research/stats');
}

export async function triggerResearchSync(days = 3): Promise<{ ok: boolean }> {
  return apiPostJson<{ ok: boolean }>(`/api/research/sync?days=${days}`);
}

export function researchReportsQueryOptions(days = 7, limit = 50) {
  return {
    queryKey: researchReportsQueryKey(days, limit),
    queryFn: () => fetchResearchReports(days, limit),
    staleTime: SCREENER_STALE_MS,
  };
}

export function researchStatsQueryOptions() {
  return {
    queryKey: researchStatsQueryKey(),
    queryFn: fetchResearchStats,
    staleTime: SCREENER_STALE_MS,
  };
}

export function useResearchReportsQuery(days = 7, limit = 50) {
  return useQuery(researchReportsQueryOptions(days, limit));
}

export function useResearchStatsQuery() {
  return useQuery(researchStatsQueryOptions());
}

export async function invalidateResearchQueries(
  queryClient: QueryClient,
): Promise<void> {
  await queryClient.invalidateQueries({ queryKey: ['research'] });
}
