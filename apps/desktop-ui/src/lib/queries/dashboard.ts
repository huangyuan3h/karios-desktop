'use client';

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

const DASHBOARD_SUMMARY_CACHE_KEY = 'karios.dashboard.summary.v1';

type DashboardSummaryCache = {
  summary?: DashboardSummary;
  cachedAt?: string;
};

export type WatchlistRiskRow = {
  symbol: string;
  name: string;
  intradayChgPct: number | null;
  gapUp: boolean | null;
  alerts: WatchlistRiskAlert[];
};

export function buildWatchlistRiskRowsFromSnapshot(
  items: Array<{ symbol: string; name?: string }>,
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

export function dashboardSummaryQueryKey(includeMacro: boolean) {
  return ['dashboard', 'summary', includeMacro ? 'full' : 'lite'] as const;
}

export function buildDashboardSummaryPath(includeMacro: boolean): string {
  return includeMacro ? '/dashboard/summary' : '/dashboard/summary?include_macro=false';
}

export async function fetchDashboardSummary(includeMacro?: boolean): Promise<DashboardSummary> {
  const macro = includeMacro ?? isShanghaiSyncWindow();
  return apiGetJson<DashboardSummary>(buildDashboardSummaryPath(macro));
}

export function useDashboardSummaryQuery() {
  const includeMacro = isShanghaiSyncWindow();
  return useQuery({
    queryKey: dashboardSummaryQueryKey(includeMacro),
    queryFn: () => fetchDashboardSummary(includeMacro),
    refetchInterval: dashboardRefetchIntervalMs,
    refetchIntervalInBackground: false,
    placeholderData: () => loadDashboardSummaryCache() ?? undefined,
  });
}

export async function fetchWatchlistRiskRows(
  queryClient: QueryClient,
): Promise<WatchlistRiskRow[]> {
  const itemsRaw = loadWatchlist();
  const items = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => ({ ...x, symbol: String(x.symbol).trim().toUpperCase() }));
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
