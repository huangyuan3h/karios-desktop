'use client';

import * as React from 'react';
import { useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import {
  getShanghaiTodayIso,
  isShanghaiSyncWindow,
  isShanghaiTradingTime,
} from '@/lib/market-hours';
import { buildWatchlistRowMetrics, type WatchlistRiskAlert } from '@/lib/watchlist-metrics';
import { loadWatchlist } from '@/lib/watchlist-storage';
import type { WatchlistMarketSnapshot } from '@/lib/watchlist-market';
import { watchlistMarketQueryOptions } from '@/lib/queries/watchlist';

import { dashboardRefetchIntervalMs } from './intervals';

export type DashboardSummary = Record<string, unknown>;

export type DashboardSummaryIncludes = {
  includeMacro?: boolean;
  includeSentiment?: boolean;
  includeNews?: boolean;
  includeIndustry?: boolean;
  includeScreeners?: boolean;
};

export const DASHBOARD_LITE_INCLUDES: DashboardSummaryIncludes = {
  includeMacro: false,
  includeSentiment: false,
  includeNews: false,
  includeIndustry: true,
  includeScreeners: true,
};

export const DASHBOARD_SENTIMENT_INCLUDES: DashboardSummaryIncludes = {
  includeMacro: true,
  includeSentiment: true,
  includeNews: false,
  includeIndustry: false,
  includeScreeners: false,
};

export const DASHBOARD_NEWS_INCLUDES: DashboardSummaryIncludes = {
  includeMacro: false,
  includeSentiment: false,
  includeNews: true,
  includeIndustry: false,
  includeScreeners: false,
};

const DASHBOARD_SUMMARY_CACHE_KEY = 'karios.dashboard.summary.v1';

type DashboardSummaryCache = {
  summary?: DashboardSummary;
  cachedAt?: string;
};

export type WatchlistRiskRow = {
  symbol: string;
  name: string;
  intradayChgPct: number | null;
  volumeRatio: number | null;
  gapUp: boolean | null;
  alerts: WatchlistRiskAlert[];
};

function normalizeIncludes(includes: DashboardSummaryIncludes): DashboardSummaryIncludes {
  return {
    includeMacro: includes.includeMacro ?? true,
    includeSentiment: includes.includeSentiment ?? true,
    includeNews: includes.includeNews ?? true,
    includeIndustry: includes.includeIndustry ?? true,
    includeScreeners: includes.includeScreeners ?? true,
  };
}

function resolveFullIncludes(includeMacro?: boolean): DashboardSummaryIncludes {
  return {
    includeMacro: includeMacro ?? isShanghaiSyncWindow(),
    includeSentiment: true,
    includeNews: true,
    includeIndustry: true,
    includeScreeners: true,
  };
}

function includesMatch(
  a: DashboardSummaryIncludes,
  b: DashboardSummaryIncludes,
): boolean {
  const na = normalizeIncludes(a);
  const nb = normalizeIncludes(b);
  return (
    na.includeMacro === nb.includeMacro &&
    na.includeSentiment === nb.includeSentiment &&
    na.includeNews === nb.includeNews &&
    na.includeIndustry === nb.includeIndustry &&
    na.includeScreeners === nb.includeScreeners
  );
}

function includesCacheKey(includes: DashboardSummaryIncludes): string {
  const n = normalizeIncludes(includes);
  if (includesMatch(n, DASHBOARD_LITE_INCLUDES)) return 'lite';
  if (includesMatch(n, DASHBOARD_SENTIMENT_INCLUDES)) return 'sentiment';
  if (includesMatch(n, DASHBOARD_NEWS_INCLUDES)) return 'news';
  if (includesMatch(n, resolveFullIncludes(true))) return 'full';
  if (includesMatch(n, resolveFullIncludes(false))) return 'no-macro';
  const coreLite: DashboardSummaryIncludes = {
    includeMacro: true,
    includeSentiment: false,
    includeNews: false,
    includeIndustry: true,
    includeScreeners: true,
  };
  if (includesMatch(n, normalizeIncludes(coreLite))) return 'core-lite';
  return [
    n.includeMacro ? 'm1' : 'm0',
    n.includeSentiment ? 's1' : 's0',
    n.includeNews ? 'n1' : 'n0',
    n.includeIndustry ? 'i1' : 'i0',
    n.includeScreeners ? 'sc1' : 'sc0',
  ].join('');
}

export function buildWatchlistRiskRowsFromSnapshot(
  items: Array<{ symbol: string; name?: string | null }>,
  snapshot: WatchlistMarketSnapshot,
): WatchlistRiskRow[] {
  const tradingTime = isShanghaiTradingTime();
  const todaySh = getShanghaiTodayIso();
  const out: WatchlistRiskRow[] = [];

  for (const it of items) {
    const t = snapshot.trend[it.symbol];
    const rowMetrics = buildWatchlistRowMetrics({
      symbol: it.symbol,
      trend: t,
      quote: snapshot.quotes[it.symbol],
      tradingTime,
      todaySh,
    });
    if (!rowMetrics.alerts.length) continue;
    out.push({
      symbol: it.symbol,
      name: it.name ?? t?.name ?? '—',
      intradayChgPct: rowMetrics.intradayChgPct,
      volumeRatio: rowMetrics.volumeRatio,
      gapUp: rowMetrics.gapUp,
      alerts: rowMetrics.alerts,
    });
  }

  out.sort((a, b) => {
    const ab = a.alerts.some((x) => x.severity === 'block');
    const bb = b.alerts.some((x) => x.severity === 'block');
    if (ab !== bb) return ab ? -1 : 1;
    const ia = a.intradayChgPct ?? -Infinity;
    const ib = b.intradayChgPct ?? -Infinity;
    return ib - ia;
  });
  return out;
}

export function loadDashboardSummaryCache(): DashboardSummary | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.localStorage.getItem(DASHBOARD_SUMMARY_CACHE_KEY);
    if (!raw) return null;
    const obj = JSON.parse(raw) as DashboardSummaryCache;
    const summary = obj?.summary;
    return summary && typeof summary === 'object' ? summary : null;
  } catch {
    return null;
  }
}

function isPopulatedRecord(value: unknown): boolean {
  return (
    value != null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.keys(value as object).length > 0
  );
}

/** Merge split dashboard queries without empty partial sections overwriting real data. */
export function mergeDashboardSummaryParts(
  lite?: DashboardSummary,
  sentiment?: DashboardSummary,
  news?: DashboardSummary,
): DashboardSummary | null {
  if (!lite && !sentiment && !news) return null;

  const merged: DashboardSummary = {};
  const asOfDate = lite?.asOfDate ?? sentiment?.asOfDate ?? news?.asOfDate;
  if (asOfDate != null) merged.asOfDate = asOfDate;

  if (lite) {
    if (isPopulatedRecord(lite.industryFundFlow)) merged.industryFundFlow = lite.industryFundFlow;
    if (Array.isArray(lite.screeners)) merged.screeners = lite.screeners;
  }

  if (sentiment) {
    if (isPopulatedRecord(sentiment.marketSentiment)) {
      merged.marketSentiment = sentiment.marketSentiment;
    }
    if (sentiment.macroSnapshot != null) merged.macroSnapshot = sentiment.macroSnapshot;
  }

  const envZh = lite?.marketEnvironmentZh ?? sentiment?.marketEnvironmentZh;
  if (typeof envZh === 'string' && envZh.trim()) merged.marketEnvironmentZh = envZh;

  if (news?.news != null) merged.news = news.news;

  const meta = lite?.meta ?? sentiment?.meta ?? news?.meta;
  if (meta != null) merged.meta = meta;

  return merged;
}

export function seedDashboardSummaryCaches(
  queryClient: QueryClient,
  cached: DashboardSummary,
): void {
  queryClient.setQueryData(dashboardLiteQueryKey(), (prev) =>
    prev !== undefined
      ? prev
      : {
          asOfDate: cached.asOfDate,
          industryFundFlow: cached.industryFundFlow,
          screeners: cached.screeners,
          marketEnvironmentZh: cached.marketEnvironmentZh,
          meta: cached.meta,
        },
  );
  queryClient.setQueryData(dashboardSummaryQueryKey(DASHBOARD_SENTIMENT_INCLUDES), (prev) =>
    prev !== undefined
      ? prev
      : {
          asOfDate: cached.asOfDate,
          marketSentiment: cached.marketSentiment,
          macroSnapshot: cached.macroSnapshot,
          marketEnvironmentZh: cached.marketEnvironmentZh,
          meta: cached.meta,
        },
  );
  queryClient.setQueryData(dashboardSummaryQueryKey(DASHBOARD_NEWS_INCLUDES), (prev) =>
    prev !== undefined ? prev : { news: cached.news },
  );
}

export function saveDashboardSummaryCache(summary: DashboardSummary): void {
  if (typeof window === 'undefined') return;
  try {
    const payload: DashboardSummaryCache = {
      summary,
      cachedAt: new Date().toISOString(),
    };
    window.localStorage.setItem(DASHBOARD_SUMMARY_CACHE_KEY, JSON.stringify(payload));
  } catch {
    // ignore
  }
}

export function dashboardSummaryQueryKey(
  includeOrOptions: boolean | DashboardSummaryIncludes = true,
) {
  const includes =
    typeof includeOrOptions === 'boolean'
      ? resolveFullIncludes(includeOrOptions)
      : normalizeIncludes(includeOrOptions);
  return ['dashboard', 'summary', includesCacheKey(includes)] as const;
}

export function dashboardLiteQueryKey() {
  return dashboardSummaryQueryKey(DASHBOARD_LITE_INCLUDES);
}

export function buildDashboardSummaryPath(
  includeOrOptions: boolean | DashboardSummaryIncludes = true,
): string {
  const includes =
    typeof includeOrOptions === 'boolean'
      ? resolveFullIncludes(includeOrOptions)
      : normalizeIncludes(includeOrOptions);
  const params = new URLSearchParams();
  if (!includes.includeMacro) params.set('include_macro', 'false');
  if (!includes.includeSentiment) params.set('include_sentiment', 'false');
  if (!includes.includeNews) params.set('include_news', 'false');
  if (!includes.includeIndustry) params.set('include_industry', 'false');
  if (!includes.includeScreeners) params.set('include_screeners', 'false');
  const qs = params.toString();
  return qs ? `/dashboard/summary?${qs}` : '/dashboard/summary';
}

export async function fetchDashboardSummaryPartial(
  includes: DashboardSummaryIncludes,
): Promise<DashboardSummary> {
  return apiGetJson<DashboardSummary>(buildDashboardSummaryPath(includes));
}

export async function fetchDashboardLiteSummary(): Promise<DashboardSummary> {
  return fetchDashboardSummaryPartial(DASHBOARD_LITE_INCLUDES);
}

export async function fetchDashboardSummary(includeMacro?: boolean): Promise<DashboardSummary> {
  return fetchDashboardSummaryPartial(resolveFullIncludes(includeMacro));
}

export async function fetchDashboardSummaryCached(
  queryClient: QueryClient,
  includeMacro?: boolean,
): Promise<DashboardSummary> {
  const macro = includeMacro ?? isShanghaiSyncWindow();
  return queryClient.fetchQuery({
    queryKey: dashboardSummaryQueryKey(macro),
    queryFn: () => fetchDashboardSummary(macro),
  });
}

export function useDashboardSummaryQuery() {
  const queryClient = useQueryClient();

  // Seed local cache after mount only — reading localStorage during render causes SSR hydration mismatches.
  React.useEffect(() => {
    const cached = loadDashboardSummaryCache();
    if (!cached) return;
    seedDashboardSummaryCaches(queryClient, cached);
  }, [queryClient]);

  return useQuery({
    queryKey: dashboardLiteQueryKey(),
    queryFn: () => fetchDashboardLiteSummary(),
    refetchInterval: dashboardRefetchIntervalMs,
    refetchIntervalInBackground: false,
  });
}

export async function fetchWatchlistRiskRows(
  queryClient: QueryClient,
): Promise<WatchlistRiskRow[]> {
  const itemsRaw = loadWatchlist();
  const items = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => ({ ...x, symbol: String(x.symbol).trim().toUpperCase() }))
    .filter((x) => typeof x.positionPct === 'number' && Number.isFinite(x.positionPct) && x.positionPct > 0);
  if (!items.length) return [];

  const symbols = items.map((x) => x.symbol);
  const snapshot = await queryClient.fetchQuery(watchlistMarketQueryOptions(symbols));
  return buildWatchlistRiskRowsFromSnapshot(items, snapshot);
}

export function watchlistRiskQueryKey() {
  return ['dashboard', 'watchlistRisk'] as const;
}

export function useWatchlistRiskQuery() {
  const queryClient = useQueryClient();
  return useQuery({
    queryKey: watchlistRiskQueryKey(),
    queryFn: () => fetchWatchlistRiskRows(queryClient),
    refetchInterval: dashboardRefetchIntervalMs,
    refetchIntervalInBackground: false,
    throwOnError: false,
  });
}
