'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';

import { SCREENER_STALE_MS } from './intervals';

export type BacktestRunListItem = {
  id: string;
  strategy_name: string;
  start_date: string;
  end_date: string;
  status: string;
  created_at: string;
  summary: Record<string, number> | null;
  error_message: string | null;
};

export type DailyLogEntry = {
  date: string;
  selected: Array<{ ts_code: string; score: number; avg_price: number }>;
  orders: Array<{
    ts_code: string;
    action: string;
    reason?: string | null;
    status?: string | null;
    exec_qty?: number | null;
    exec_price?: number | null;
  }>;
  positions?: Array<{ ts_code: string; qty: number }>;
  strategy_stats?: {
    date?: string;
    regime?: string;
    bars?: number;
    breakout_ok?: number;
    pullback_ok?: number;
    sell_ok?: number;
    buy_signal?: number;
  } | null;
  cash_before: number;
  cash: number;
  equity: number;
};

export type BacktestRunRecord = {
  id: string;
  strategy_name: string;
  start_date: string;
  end_date: string;
  status: string;
  created_at: string;
  params: unknown;
  summary: Record<string, number> | null;
  equity_curve: Array<{ date: string; equity: number }> | null;
  drawdown_curve: Array<{ date: string; drawdown: number }> | null;
  positions_curve: Array<{ date: string; invested_ratio: number }> | null;
  daily_log: Array<DailyLogEntry> | null;
  error_message: string | null;
};

export type BacktestResultResponse = {
  run: BacktestRunRecord;
  trades: Array<Record<string, unknown>>;
};

export type IndexDailyPoint = {
  date: string;
  close: number;
};

export function backtestRunsQueryKey(limit = 50) {
  return ['backtest', 'runs', limit] as const;
}

export function backtestResultQueryKey(runId: string) {
  return ['backtest', 'result', runId] as const;
}

export function backtestIndexQueryKey(startDate: string, endDate: string) {
  return ['backtest', 'index', '000001.SH', startDate, endDate] as const;
}

export async function fetchBacktestRuns(limit = 50): Promise<BacktestRunListItem[]> {
  const resp = await apiGetJson<{ items: BacktestRunListItem[] }>(
    `/backtest/runs?limit=${limit}`,
  );
  return resp.items ?? [];
}

export async function fetchBacktestResult(runId: string): Promise<BacktestResultResponse> {
  return apiGetJson<BacktestResultResponse>(`/backtest/result/${encodeURIComponent(runId)}`);
}

export async function fetchBacktestIndexSeries(
  startDate: string,
  endDate: string,
): Promise<IndexDailyPoint[]> {
  const items = await apiGetJson<Array<Record<string, unknown>>>(
    `/index-daily?ts_code=000001.SH&start_date=${startDate}&end_date=${endDate}&limit=10000`,
  );
  return items
    .map((x) => ({
      date: String(x.trade_date || ''),
      close: Number(x.close || 0),
    }))
    .filter((x) => x.date && Number.isFinite(x.close) && x.close > 0);
}

export function backtestRunsQueryOptions(limit = 50) {
  return {
    queryKey: backtestRunsQueryKey(limit),
    queryFn: () => fetchBacktestRuns(limit),
    staleTime: SCREENER_STALE_MS,
  };
}

export function backtestResultQueryOptions(runId: string) {
  return {
    queryKey: backtestResultQueryKey(runId),
    queryFn: () => fetchBacktestResult(runId),
    staleTime: SCREENER_STALE_MS,
    enabled: Boolean(runId?.trim()),
  };
}

export function backtestIndexQueryOptions(startDate: string, endDate: string) {
  return {
    queryKey: backtestIndexQueryKey(startDate, endDate),
    queryFn: () => fetchBacktestIndexSeries(startDate, endDate),
    staleTime: SCREENER_STALE_MS,
    enabled: Boolean(startDate?.trim() && endDate?.trim()),
  };
}

export function useBacktestRunsQuery(limit = 50) {
  return useQuery(backtestRunsQueryOptions(limit));
}

export function useBacktestResultQuery(runId: string) {
  return useQuery(backtestResultQueryOptions(runId));
}

export function useBacktestIndexQuery(startDate: string, endDate: string) {
  return useQuery(backtestIndexQueryOptions(startDate, endDate));
}

export async function invalidateBacktestQueries(queryClient: QueryClient): Promise<void> {
  await queryClient.invalidateQueries({ queryKey: ['backtest'] });
}
