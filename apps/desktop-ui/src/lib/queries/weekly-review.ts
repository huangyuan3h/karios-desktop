'use client';

import { useQuery } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';

export type WeeklyReviewResponse = {
  ok: boolean;
  week: { start: string; end: string };
  decisionVolume: { total: number; bySource: Record<string, number> };
  paper: {
    closed: number;
    wins: number;
    winRate: number | null;
    avgNetPnlPct: number | null;
    byReason: Record<string, { count: number; avgNet: number | null; winRate: number | null }>;
  };
  exitAttribution: {
    withForward: number;
    earlyRate: number | null;
    wellRate: number | null;
    avgFwdPct: number | null;
  };
  funnel: { runs: number; screenerAdded: number };
  registry: { total: number; held: number };
  markdown: string;
};

export function useWeeklyReviewQuery(enabled = true) {
  return useQuery({
    queryKey: ['weekly-review'],
    queryFn: () => apiGetJson<WeeklyReviewResponse>('/api/backtest/weekly-review'),
    staleTime: 5 * 60_000,
    enabled,
  });
}
