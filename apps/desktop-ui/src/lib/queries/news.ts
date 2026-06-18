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
