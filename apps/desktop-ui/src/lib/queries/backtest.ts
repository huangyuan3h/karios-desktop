'use client';

import { useQuery } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';

export type BacktestSummary = {
  config: {
    start_date: string;
    end_date: string;
    score_threshold: number;
    max_hold_days: number;
    stop_loss_pct: number;
    target_pnl_pct: number;
    score_floor: number;
    market: string;
    gates: string;
    trailing_stop_pct?: number;
    position_pct?: number;
    max_positions?: number;
    rs_rank_min?: number;
    diverging_scale?: number;
  };
  calendar_days: number;
  trades: number;
  closed: number;
  open_at_end: number;
  wins: number;
  losses: number;
  win_rate: number | null;
  avg_net_pnl_pct: number | null;
  avg_gross_pnl_pct: number | null;
  avg_costs_pct: number | null;
  max_drawdown_pct: number;
  total_net_pnl_pct: number;
  annual_net_pnl_pct: number;
  avg_win_pct: number | null;
  avg_loss_pct: number | null;
  sharpe: number | null;
  excess_vs_best_benchmark_pct: number;
  best_benchmark: string;
  by_score_bucket: Record<string, { trades: number; wins: number; winRate: number | null; avgNet: number | null }>;
  gated_blocks: Record<string, number>;
};

export type SensitivityResult = BacktestSummary;

export type BenchmarkItem = {
  ts_code: string;
  name: string;
  start_date: string;
  end_date: string;
  total_return_pct: number;
  annual_pct: number;
};

export type SensitivityResponse = {
  ok: boolean;
  configs: number;
  benchmarks: BenchmarkItem[];
  results: SensitivityResult[];
};

export type BacktestRunResponse = {
  ok: boolean;
  summary: BacktestSummary;
  benchmarks: BenchmarkItem[];
};

export type ExitAttributionResponse = {
  ok: boolean;
  days: number;
  closedCount: number;
  withForwardCount: number;
  excluded: number;
  insufficient: boolean;
  hint: string | null;
  overall: {
    count: number;
    avgFwdPct: number | null;
    earlyCount: number;
    wellCount: number;
    neutralCount: number;
    earlyRate: number | null;
    wellRate: number | null;
  };
  byReason: Record<
    string,
    {
      label: string;
      count: number;
      withForward?: number;
      avgFwdPct?: number | null;
      earlyCount?: number;
      wellCount?: number;
      neutralCount?: number;
      earlyRate?: number | null;
      wellRate?: number | null;
    }
  >;
  exposure: {
    maxSimultaneous: number;
    singleStockWeightFloorPct: number | null;
    note: string;
  };
};

export type BacktestParams = {
  start: string;
  end: string;
  scoreThreshold: number;
  maxHoldDays: number;
  stopLossPct: number;
  gates: string;
  trailingStopPct: number;
  positionPct: number;
  maxPositions: number;
  rsRankMin: number;
  divergingScale: number;
  targetPnlPct: number;
  scoreFloor: number;
  panicCooldownDays: number;
  slippagePct: number;
  excludeBoards: string;
};

export const GATE_LEVELS = [
  { value: 'full', label: '全套（红绿灯+资金流+mainline）' },
  { value: 'regime', label: '仅红绿灯 regime' },
  { value: 'none', label: '只看分数（v0）' },
] as const;

function backtestRunPath(p: BacktestParams): string {
  const q = new URLSearchParams({
    start: p.start,
    end: p.end,
    score_threshold: String(p.scoreThreshold),
    max_hold_days: String(p.maxHoldDays),
    stop_loss_pct: String(p.stopLossPct),
    gates: p.gates,
    trailing_stop_pct: String(p.trailingStopPct),
    position_pct: String(p.positionPct),
    max_positions: String(p.maxPositions),
    rs_rank_min: String(p.rsRankMin),
    diverging_scale: String(p.divergingScale),
    target_pnl_pct: String(p.targetPnlPct),
    score_floor: String(p.scoreFloor),
    panic_cooldown_days: String(p.panicCooldownDays),
    slippage_pct: String(p.slippagePct),
    exclude_boards: p.excludeBoards,
  });
  return `/api/backtest/run?${q.toString()}`;
}

export function useBacktestRunQuery(p: BacktestParams, attempt = 0) {
  return useQuery({
    queryKey: ['backtest', 'run', p, attempt],
    queryFn: () => apiGetJson<BacktestRunResponse>(backtestRunPath(p)),
    staleTime: 0,
  });
}

export function useSensitivityQuery(start: string, end: string, enabled: boolean) {
  const q = new URLSearchParams({ start, end });
  return useQuery({
    queryKey: ['backtest', 'sensitivity', start, end],
    queryFn: () => apiGetJson<SensitivityResponse>(`/api/backtest/sensitivity?${q.toString()}`),
    staleTime: 60_000,
    enabled,
  });
}

export function useExitAttributionQuery(days = 5, enabled = true) {
  return useQuery({
    queryKey: ['backtest', 'exit-attribution', days],
    queryFn: () => apiGetJson<ExitAttributionResponse>(`/api/backtest/exit-attribution?days=${days}`),
    staleTime: 30_000,
    enabled,
  });
}

export type CorrelationStatusResponse = {
  ok: boolean;
  capPct: number;
  clusters: Record<
    string,
    {
      label: string;
      exposurePct: number;
      symbols: string[];
      industries: string[];
    }
  >;
  overLimit: string[];
  blockedSymbols: string[];
  topPairs: Array<[string, string, number]>;
  empiricalNote: string | null;
};

export function useCorrelationStatusQuery(includeMatrix = true, enabled = true) {
  const q = new URLSearchParams({ include_matrix: String(includeMatrix) });
  return useQuery({
    queryKey: ['backtest', 'correlation-status', includeMatrix],
    queryFn: () =>
      apiGetJson<CorrelationStatusResponse>(`/api/backtest/correlation-status?${q.toString()}`),
    staleTime: 60_000,
    enabled,
  });
}

/** Cluster exposure % for one symbol from a correlation-status payload. */
export function clusterExposureForSymbol(
  status: CorrelationStatusResponse | undefined,
  symbol: string,
): number | null {
  if (!status) return null;
  const s = symbol.trim().toUpperCase();
  for (const c of Object.values(status.clusters)) {
    if (c.symbols.includes(s)) return c.exposurePct;
  }
  return null;
}
