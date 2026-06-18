'use client';

import { useQuery } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import type { TrendOkResult } from '@/lib/api/types';
import { chunk } from '@/lib/chunk';
import {
  getShanghaiTodayIso,
  isShanghaiSyncWindow,
  isShanghaiTradingTime,
} from '@/lib/market-hours';
import {
  buildWatchlistRowMetrics,
  parseQuoteNumber,
  type WatchlistRiskAlert,
} from '@/lib/watchlist-metrics';
import { loadWatchlist } from '@/lib/watchlist-storage';
import { toTsCodeFromSymbol } from '@/lib/symbols';

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

type QuoteResp = {
  ok: boolean;
  error?: string;
  items: Array<{
    ts_code: string;
    price: string | null;
    pre_close: string | null;
    pct_chg: string | null;
    amount: string | null;
    volume: string | null;
    trade_time: string | null;
  }>;
};

function parseDashboardQuoteItem(it: QuoteResp['items'][number]): {
  price: number | null;
  tradeTime: string | null;
  amount: number | null;
  volume: number | null;
  preClose: number | null;
  pctChg: number | null;
} {
  const p = it.price != null ? Number(it.price) : NaN;
  const pre = it.pre_close != null ? Number(it.pre_close) : NaN;
  const pct = it.pct_chg != null ? Number(it.pct_chg) : NaN;
  return {
    price: Number.isFinite(p) ? p : null,
    tradeTime: typeof it.trade_time === 'string' ? it.trade_time : null,
    amount: parseQuoteNumber(it.amount),
    volume: parseQuoteNumber(it.volume),
    preClose: Number.isFinite(pre) ? pre : null,
    pctChg: Number.isFinite(pct) ? pct : null,
  };
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

export async function fetchWatchlistRiskRows(): Promise<WatchlistRiskRow[]> {
  const itemsRaw = loadWatchlist();
  const items = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => ({ ...x, symbol: String(x.symbol).trim().toUpperCase() }));
  if (!items.length) return [];

  const tradingTime = isShanghaiTradingTime();
  const quoteWindow = isShanghaiSyncWindow();
  const todaySh = getShanghaiTodayIso();
  const syms = items.map((x) => x.symbol);
  const byTsCode = new Map<string, string>();
  const tsCodes = syms
    .map((s) => {
      const t = toTsCodeFromSymbol(s);
      if (t) byTsCode.set(t, s);
      return t;
    })
    .filter(Boolean) as string[];

  const [trendResults, quoteResults] = await Promise.all([
    Promise.all(
      chunk(syms, 200).map(async (part) => {
        const sp = new URLSearchParams();
        sp.set('realtime', quoteWindow ? 'true' : 'false');
        for (const s of part) sp.append('symbols', s);
        return apiGetJson<TrendOkResult[]>(`/market/stocks/trendok?${sp.toString()}`);
      }),
    ),
    Promise.all(
      chunk(tsCodes, 50).map(async (part) => {
        return apiGetJson<QuoteResp>(`/quote?ts_codes=${encodeURIComponent(part.join(','))}`).catch(
          () => null,
        );
      }),
    ),
  ]);

  const trend: Record<string, TrendOkResult> = {};
  for (const trendRows of trendResults) {
    for (const r of Array.isArray(trendRows) ? trendRows : []) {
      if (r && r.symbol) trend[String(r.symbol).toUpperCase()] = r;
    }
  }

  const quotes: Record<
    string,
    {
      price: number | null;
      tradeTime: string | null;
      amount: number | null;
      volume: number | null;
      preClose: number | null;
      pctChg: number | null;
    }
  > = {};
  for (const r of quoteResults) {
    for (const it of r?.items ?? []) {
      const sym = byTsCode.get(it.ts_code);
      if (!sym) continue;
      quotes[sym] = parseDashboardQuoteItem(it);
    }
  }

  const out: WatchlistRiskRow[] = [];
  for (const it of items) {
    const t = trend[it.symbol];
    const rowMetrics = buildWatchlistRowMetrics({
      symbol: it.symbol,
      trend: t,
      quote: quotes[it.symbol],
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

export function watchlistRiskQueryKey() {
  return ['dashboard', 'watchlistRisk'] as const;
}

export function useWatchlistRiskQuery() {
  return useQuery({
    queryKey: watchlistRiskQueryKey(),
    queryFn: fetchWatchlistRiskRows,
    refetchInterval: dashboardRefetchIntervalMs,
    refetchIntervalInBackground: false,
    throwOnError: false,
  });
}
