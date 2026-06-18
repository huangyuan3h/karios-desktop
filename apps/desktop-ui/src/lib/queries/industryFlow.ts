'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';

import { apiGetJson, apiPostJson } from '@/lib/api/client';
import { isShanghaiSyncWindow } from '@/lib/market-hours';

import { SCREENER_STALE_MS } from './intervals';

export type IndustryFundFlowPoint = {
  date: string;
  netInflow: number;
};

export type IndustryFundFlowRow = {
  industryCode: string;
  industryName: string;
  netInflow: number;
  sum10d: number;
  series10d: IndustryFundFlowPoint[];
};

export type IndustryFundFlowResp = {
  asOfDate: string;
  days: number;
  topN: number;
  dates: string[];
  top: IndustryFundFlowRow[];
};

export type MainlineFlowFlags = {
  sum20d: number;
  sum5d: number;
  rank20d: number;
  rank5d: number;
  positiveDays10d: number;
  midAccumulation: boolean;
  shortIntensity: boolean;
  consistency: boolean;
};

export type MainlineBreadthFlags = {
  limitUpCount: number;
  limitUpRank: number;
  limitUpQualified: boolean;
  dragonCount: number;
  dragonQualified: boolean;
  surgeRatio: number;
  surgeQualified: boolean;
};

export type MainlineTrendFlags = {
  indexAboveMa20: boolean;
  ma20Up: boolean;
  rps: number;
  rpsQualified: boolean;
};

export type MainlineScoreRow = {
  industryName: string;
  flowScore: number;
  breadthScore: number;
  trendScore: number;
  totalScore: number;
  isMainline: boolean;
  flags: {
    flow: MainlineFlowFlags;
    breadth: MainlineBreadthFlags;
    trend: MainlineTrendFlags;
  };
};

export type MainlineResp = {
  asOfDate: string;
  dates: string[];
  allScores: MainlineScoreRow[];
  currentMainline: MainlineScoreRow[];
  warning?: string;
};

/** Full universe for per-day ranking widgets on Industry Flow page. */
export const INDUSTRY_FLOW_UNIVERSE_TOP_N = 200;
export const INDUSTRY_FLOW_DAYS = 10;

export function industryFundFlowQueryKey(days: number, topN: number, asOfDate?: string) {
  const trimmed = asOfDate?.trim();
  if (trimmed) return ['industry', 'fundFlow', days, topN, trimmed] as const;
  return ['industry', 'fundFlow', days, topN] as const;
}

export function industryMainlineQueryKey() {
  return ['industry', 'mainline'] as const;
}

export async function fetchIndustryFundFlow(
  days: number = INDUSTRY_FLOW_DAYS,
  topN: number = INDUSTRY_FLOW_UNIVERSE_TOP_N,
  asOfDate?: string,
): Promise<IndustryFundFlowResp> {
  const params = new URLSearchParams({
    days: String(days),
    topN: String(topN),
  });
  const trimmedAsOf = asOfDate?.trim();
  if (trimmedAsOf) params.set('asOfDate', trimmedAsOf);
  return apiGetJson<IndustryFundFlowResp>(`/market/cn/industry-fund-flow?${params.toString()}`);
}

export async function fetchIndustryMainline(): Promise<MainlineResp> {
  return apiGetJson<MainlineResp>('/market/cn/industry-mainline');
}

export async function fetchIndustryFlowBundle(
  days: number = INDUSTRY_FLOW_DAYS,
  topN: number = INDUSTRY_FLOW_UNIVERSE_TOP_N,
): Promise<{ fundFlow: IndustryFundFlowResp; mainline: MainlineResp }> {
  const [fundFlow, mainline] = await Promise.all([
    fetchIndustryFundFlow(days, topN),
    fetchIndustryMainline(),
  ]);
  return { fundFlow, mainline };
}

export function industryFundFlowQueryOptions(
  days: number = INDUSTRY_FLOW_DAYS,
  topN: number = INDUSTRY_FLOW_UNIVERSE_TOP_N,
  asOfDate?: string,
) {
  return {
    queryKey: industryFundFlowQueryKey(days, topN, asOfDate),
    queryFn: () => fetchIndustryFundFlow(days, topN, asOfDate),
    staleTime: SCREENER_STALE_MS,
  };
}

export function industryMainlineQueryOptions() {
  return {
    queryKey: industryMainlineQueryKey(),
    queryFn: fetchIndustryMainline,
    staleTime: SCREENER_STALE_MS,
  };
}

export function useIndustryFundFlowQuery(
  days: number = INDUSTRY_FLOW_DAYS,
  topN: number = INDUSTRY_FLOW_UNIVERSE_TOP_N,
) {
  return useQuery(industryFundFlowQueryOptions(days, topN));
}

export function useIndustryMainlineQuery() {
  return useQuery(industryMainlineQueryOptions());
}

export async function syncIndustryFundFlow(options: {
  days?: number;
  topN?: number;
  force?: boolean;
}): Promise<Record<string, unknown>> {
  return apiPostJson<Record<string, unknown>>('/market/cn/industry-fund-flow/sync', {
    days: options.days ?? INDUSTRY_FLOW_DAYS,
    topN: options.topN ?? 10,
    force: Boolean(options.force),
  });
}

export async function syncIndustryMainline(force = false): Promise<Record<string, unknown>> {
  return apiPostJson<Record<string, unknown>>('/market/cn/industry-mainline/sync', {
    force,
  });
}

export async function invalidateIndustryFlowQueries(queryClient: QueryClient): Promise<void> {
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: ['industry'] }),
    queryClient.invalidateQueries({ queryKey: ['dashboard', 'summary'] }),
  ]);
}

export async function runIndustryFlowSync(
  queryClient: QueryClient,
  options: { force?: boolean } = {},
): Promise<Record<string, unknown>> {
  const fundResult = await syncIndustryFundFlow({ force: options.force });
  await syncIndustryMainline(Boolean(options.force));
  await invalidateIndustryFlowQueries(queryClient);
  return fundResult;
}
