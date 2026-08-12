'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';

import {
  DASHBOARD_NEWS_INCLUDES,
  dashboardSummaryQueryKey,
  fetchDashboardSummaryPartial,
  type DashboardSummary,
} from './dashboard';
import { dashboardRefetchIntervalMs, SCREENER_STALE_MS } from './intervals';

export type NewsSource = {
  id: string;
  name: string;
  url: string;
  enabled: boolean;
  lastFetch: string | null;
  createdAt: string;
  tier?: string;
  category?: string | null;
};

export type DashboardNewsItem = {
  id: string;
  sourceId: string;
  title: string;
  link: string;
  summary: string | null;
  publishedAt: string | null;
  fetchedAt: string;
  isRead: boolean;
  isImportant: boolean;
  // Track 2: LLM enrichment fields
  tickers?: string[] | null;
  sectors?: string[] | null;
  eventType?: string | null;
  importance?: number | null;
  relevanceScore?: number | null;
  aiSummary?: string | null;
  actionability?: 'actionable' | 'informational' | 'historical' | null;
  enrichmentStatus?: string | null;
  enrichedAt?: string | null;
  enrichmentModel?: string | null;
};

export type NewsItem = DashboardNewsItem;

export type NewsItemsResponse = {
  total: number;
  items: NewsItem[];
};

export type NewsSourcesResponse = {
  sources: NewsSource[];
};

export function dashboardNewsQueryKey() {
  return dashboardSummaryQueryKey(DASHBOARD_NEWS_INCLUDES);
}

export async function fetchDashboardNews(): Promise<DashboardSummary> {
  return fetchDashboardSummaryPartial(DASHBOARD_NEWS_INCLUDES);
}

export function dashboardNewsQueryOptions() {
  return {
    queryKey: dashboardNewsQueryKey(),
    queryFn: fetchDashboardNews,
  };
}

export function useDashboardNewsQuery() {
  return useQuery({
    ...dashboardNewsQueryOptions(),
    refetchInterval: dashboardRefetchIntervalMs,
    refetchIntervalInBackground: false,
  });
}

export function newsItemsQueryKey(hours: number, limit = 100) {
  return ['news', 'items', hours, limit] as const;
}

export function newsSourcesQueryKey() {
  return ['news', 'sources'] as const;
}

export async function fetchNewsItems(hours: number, limit = 100): Promise<NewsItemsResponse> {
  return apiGetJson<NewsItemsResponse>(
    `/api/news/items?limit=${limit}&hours=${hours}`,
  );
}

export async function fetchNewsSources(): Promise<NewsSourcesResponse> {
  return apiGetJson<NewsSourcesResponse>('/api/news/sources');
}

export function newsItemsQueryOptions(hours: number, limit = 100) {
  return {
    queryKey: newsItemsQueryKey(hours, limit),
    queryFn: () => fetchNewsItems(hours, limit),
    staleTime: SCREENER_STALE_MS,
  };
}

export function newsSourcesQueryOptions() {
  return {
    queryKey: newsSourcesQueryKey(),
    queryFn: fetchNewsSources,
    staleTime: SCREENER_STALE_MS,
  };
}

export function useNewsItemsQuery(hours: number, limit = 100) {
  return useQuery(newsItemsQueryOptions(hours, limit));
}

export function useNewsSourcesQuery() {
  return useQuery(newsSourcesQueryOptions());
}

export async function invalidateNewsPageQueries(queryClient: QueryClient): Promise<void> {
  await queryClient.invalidateQueries({ queryKey: ['news'] });
}

// --- Morning Brief (Track 3) ---

export type BriefCategory = 'watchlist' | 'risk' | 'macro' | 'sector';

export type BriefItem = {
  id: string;
  title: string;
  sourceId: string | null;
  publishedAt: string | null;
  tickers: string[];
  sectors: string[];
  eventType: string | null;
  importance: number | null;
  relevanceScore: number | null;
  aiSummary: string | null;
  actionability: 'actionable' | 'informational' | 'historical' | null;
  link: string | null;
  score: number;
  category: BriefCategory;
};

export type MorningBrief = {
  id: string;
  briefDate: string;
  briefType: string;
  items: BriefItem[];
  macroOverview: string | null;
  modelVersion: string | null;
  sourceItemIds: string[] | null;
  markdown: string | null;
  createdAt: string;
};

export function morningBriefQueryKey() {
  return ['news', 'brief', 'latest'] as const;
}

export async function fetchMorningBrief(): Promise<{ brief: MorningBrief | null }> {
  // 2026-08-12: the trading-session briefs (trading-open/midday/action) live in
  // the same table — the news brief card must NOT pick them up (different item
  // schema), so filter to the news briefs explicitly.
  return apiGetJson<{ brief: MorningBrief | null }>(
    '/api/news/brief/latest?brief_type=morning',
  );
}

export function morningBriefQueryOptions() {
  return {
    queryKey: morningBriefQueryKey(),
    queryFn: fetchMorningBrief,
    staleTime: 60_000,
  };
}

export function useMorningBriefQuery() {
  return useQuery(morningBriefQueryOptions());
}

/** Trading-session brief types (10:00 open / 12:00 midday / 14:30 action). */
export const TRADING_BRIEF_TYPES = ['open', 'midday', 'action'] as const;

export async function fetchTradingBrief(
  briefType: string,
): Promise<{ brief: MorningBrief | null }> {
  return apiGetJson<{ brief: MorningBrief | null }>(
    `/api/news/brief/latest?brief_type=trading-${briefType}`,
  );
}

export function useTradingBriefQuery(briefType: string) {
  return useQuery({
    queryKey: ['news', 'brief', 'trading', briefType],
    queryFn: () => fetchTradingBrief(briefType),
    staleTime: 60_000,
  });
}
