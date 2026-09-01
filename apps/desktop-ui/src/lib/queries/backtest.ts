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

export type ReconItem = {
  reconDate: string;
  market: string;
  window: string;
  expected: number;
  actual: number;
  aligned: number;
  missing: number;
  extra: number;
  alignedReturnDiffPct?: number | null;
  btReturnMedianPct?: number | null;
  paperReturnMedianPct?: number | null;
  detail?: Array<Record<string, unknown>> | null;
};

export type ReconResponse = { ok: boolean; items: ReconItem[] };

/**
 * Latest backtest-vs-paper reconciliation snapshots (2026-08-11): what the
 * S-3 backtest says we SHOULD hold vs what the paper book actually holds,
 * per market. The Monday cron fills this weekly.
 *
 * NOTE: pass limit >= 2 to cover both markets — the snapshot stores one row
 * per market, and limit=1 silently drops the second market.
 */
export function useBacktestReconQuery(limit = 2, enabled = true) {
  return useQuery({
    queryKey: ['backtest', 'recon', limit],
    queryFn: () =>
      apiGetJson<ReconResponse>(`/api/backtest/recon/latest?limit=${limit}`),
    staleTime: 60_000,
    enabled,
  });
}

export type BacktestOverviewWindow = {
  totalNetPnlPct?: number | null;
  winRate?: number | null;
  sharpe?: number | null;
  trades?: number | null;
  maxDrawdownPct?: number | null;
};

export type BacktestOverviewBaseline = {
  generatedAt?: string | null;
  tag?: string | null;
  windows: Record<string, BacktestOverviewWindow>;
};

export type RollingOosMarket = {
  closed?: number | null;
  winRate?: number | null;
  avgNetPnlPct?: number | null;
  totalNetPnlPct?: number | null;
  maxDrawdownPct?: number | null;
  sharpe?: number | null;
};

export type BacktestOverview = {
  ok: boolean;
  cnBaseline?: BacktestOverviewBaseline | null;
  hkBaseline?: BacktestOverviewBaseline | null;
  rollingOos?: {
    windowStart?: string | null;
    windowEnd?: string | null;
    warning?: boolean | null;
    warnings?: string[] | null;
    markets?: Record<string, RollingOosMarket> | null;
  } | null;
  longWindowCN?: {
    window?: string | null;
    totalNetPnlPct?: number | null;
    maxDrawdownPct?: number | null;
    sharpe?: number | null;
    trades?: number | null;
    byYear?: Record<string, number> | null;
  } | null;
};

/** S-3 conclusion board: frozen baselines + rolling OOS + long window. */
export function useBacktestOverviewQuery(enabled = true) {
  return useQuery({
    queryKey: ['backtest', 'overview'],
    queryFn: () => apiGetJson<BacktestOverview>('/api/backtest/overview'),
    staleTime: 5 * 60_000,
    enabled,
  });
}

export type SleeveNavWindow = {
  window?: string | null;
  totalBasePct?: number | null;
  totalSleevePct?: number | null;
  deltaPct?: number | null;
  maxDdBasePct?: number | null;
  maxDdSleevePct?: number | null;
  holdDays?: number | null;
  idleDays?: number | null;
  avgIdlePct?: number | null;
};

export type SleeveNavReport = {
  ok: boolean;
  report?: {
    generatedAt?: string | null;
    results?: Record<string, SleeveNavWindow> | null;
  } | null;
};

/** T6 third-asset sleeve NAV report (scripts/sleeve_nav_sim.py). */
export function useSleeveNavQuery(enabled = true) {
  return useQuery({
    queryKey: ['backtest', 'sleeve-nav'],
    queryFn: () => apiGetJson<SleeveNavReport>('/api/backtest/sleeve-nav'),
    staleTime: 10 * 60_000,
    enabled,
  });
}

export type CoreAuditOp = {
  date?: string | null;
  side?: string | null;
  price?: number | null;
  positionPct?: number | null;
  verdict?: 'ok' | 'warn' | 'violation' | null;
  rule?: string | null;
  detail?: string | null;
};

export type CoreAuditHolding = {
  symbol?: string | null;
  name?: string | null;
  positionPct?: number | null;
  costPrice?: number | null;
  lastClose?: number | null;
  pnlPct?: number | null;
  stopLossLine?: number | null;
  trailingLine?: number | null;
  maxHoldDate?: string | null;
  pyramidTriggerLine?: number | null;
  pyramidAdded?: boolean | null;
  ops?: CoreAuditOp[] | null;
};

export type CoreAudit = {
  ok: boolean;
  day?: string | null;
  gate?: {
    regime?: string | null;
    panicActive?: boolean | null;
    gateOpen?: boolean | null;
  } | null;
  holdings?: CoreAuditHolding[] | null;
  violations?: Array<CoreAuditOp & { symbol?: string | null; severity?: string | null }> | null;
  counts?: { ok?: number | null; warn?: number | null; violation?: number | null } | null;
};

/** Core-holding operation audit: did manual trades follow the rules? */
export function useCoreAuditQuery(enabled = true) {
  return useQuery({
    queryKey: ['backtest', 'core-audit'],
    queryFn: () => apiGetJson<CoreAudit>('/api/backtest/core-audit'),
    staleTime: 5 * 60_000,
    enabled,
  });
}

export type TimelineRow = {
  date: string;
  deployedPct: number;
  idlePct: number;
  positions: number;
  cnPositions: number;
  hkPositions: number;
  stockMarket: string;
  stockSymbols: string[];
  stockMom: number | null;
  pick: string | null;
  pickTs: string | null;
  navBase: number;
  navSleeve: number | null;
  navSingle: number;
  navMulti: number;
  navBaseReturnPct: number;
  navSingleReturnPct: number;
  navMultiReturnPct: number;
  /** twin_star (双子星) satellite leg */
  satNav?: number | null;
  satNavReturnPct?: number | null;
  satPositions?: number | null;
};

export type TimelineResponse = {
  ok: boolean;
  start: string;
  end: string;
  strategy?: string;
  mode?: string;
  rows: TimelineRow[];
};

export type TimelineStrategy = 'pick_strong' | 'twin_star';

export const TIMELINE_STRATEGY_LABEL: Record<TimelineStrategy, string> = {
  pick_strong: '单轨择强',
  twin_star: '机会双子星',
};

export function useTimelineQuery(
  start: string,
  end: string,
  strategy: TimelineStrategy = 'pick_strong',
  enabled = true,
) {
  const q = new URLSearchParams({ start, end, strategy });
  return useQuery({
    queryKey: ['backtest', 'timeline', start, end, strategy],
    queryFn: () => apiGetJson<TimelineResponse>(`/api/backtest/timeline?${q.toString()}`, { timeoutMs: 90_000 }),
    staleTime: 5 * 60_000,
    enabled,
  });
}

export type TwinStarSatCandidate = {
  ts: string;
  amp: number | null;
  gapPct: number | null;
  close: number | null;
};

export type TwinStarSatHolding = {
  ts: string;
  entryDate?: string | null;
  entryPrice?: number | null;
  close?: number | null;
  heldDays?: number | null;
  daysLeft?: number | null;
  exitDue?: string | null;
  pnlPct?: number | null;
};

export type TwinStarAction = {
  ok: boolean;
  core: {
    pick?: string | null;
    symbol?: string | null;
    label?: string | null;
    action?: string | null;
    message?: string | null;
    active?: boolean | null;
  };
  sat: {
    asOf?: string | null;
    gateOpen?: boolean | null;
    breadth?: number | null;
    gapCount?: number | null;
    candidates?: TwinStarSatCandidate[] | null;
    note?: string | null;
    approx?: boolean | null;
    coreTargetPct?: number | null;
    satTargetPct?: number | null;
    book?: {
      asOf?: string | null;
      holdings?: TwinStarSatHolding[] | null;
      exitsDue?: TwinStarSatHolding[] | null;
      body?: number | null;
    } | null;
  };
};

/** 双子星 (Twin-Star) 今日操作信号: 核心择强目标 + S-gap 卫星闸/候选. */
export function useTwinStarActionQuery(enabled = true) {
  return useQuery({
    queryKey: ['backtest', 'twin-star', 'action'],
    queryFn: () => apiGetJson<TwinStarAction>('/api/backtest/twin-star/action', { timeoutMs: 60_000 }),
    staleTime: 10 * 60_000,
    refetchInterval: 30 * 60_000,
    enabled,
  });
}

export type PickAttrStat = {
  days: number;
  pctDays: number;
  contribAddPct: number;
  contribGeoPct: number;
};

export type ReturnAttributionResponse = {
  ok: boolean;
  start: string;
  end: string;
  note?: string;
  pickStrong?: {
    byPick: Record<string, PickAttrStat>;
    totalDays: number;
    totalAddPct: number;
    totalGeoPct: number;
    timelineFusedPct?: number;
    byMonth: Array<{ month: string; byPick: Record<string, number>; totalAddPct: number }>;
    topDays: Array<{ date: string; pick: string; dayRetPct: number }>;
    stockBreakdown: {
      stockDays: number;
      bySymbol: Record<string, { days: number; contribAddPct: number }>;
    } | null;
  };
  userTrades?: {
    closedCount: number;
    bySymbol: Record<string, { count: number; sumPnlPct: number; bucket: string }>;
    byBucket: Record<string, { count: number; sumPnlPct: number }>;
    insufficient: boolean;
    note?: string;
    error?: string;
  };
};

export function useReturnAttributionQuery(start: string, end: string, enabled = true) {
  const q = new URLSearchParams({ start, end, books: 'pick_strong,user' });
  return useQuery({
    queryKey: ['backtest', 'return-attribution', start, end],
    queryFn: () =>
      apiGetJson<ReturnAttributionResponse>(`/api/backtest/return-attribution?${q.toString()}`, {
        timeoutMs: 120_000,
      }),
    staleTime: 5 * 60_000,
    enabled,
  });
}

export type PaperVsBacktestTwin = {
  entryDate?: string | null;
  closeDate?: string | null;
  entryPrice?: number | null;
  closePrice?: number | null;
  pnlPct?: number | null;
  holdingDays?: number | null;
  closeReason?: string | null;
};

export type PaperVsBacktestRow = {
  symbol: string;
  market: string;
  entryDate?: string | null;
  closeDate?: string | null;
  paper: {
    entryPrice?: number | null;
    closePrice?: number | null;
    pnlPct?: number | null;
    holdingDays?: number | null;
    closeReason?: string | null;
  };
  backtest?: PaperVsBacktestTwin | null;
  diff?: {
    entryPriceDiffPct?: number | null;
    pnlDiffPct?: number | null;
  } | null;
  note?: string | null;
};

export type PaperVsBacktestReport = {
  generatedAt?: string | null;
  sampleCount?: number | null;
  verdict?: string | null;
  rows: PaperVsBacktestRow[];
  summary?: {
    paper?: { closed?: number | null; winRate?: number | null; avgPnlPct?: number | null };
    backtestMatched?: { closed?: number | null; winRate?: number | null; avgPnlPct?: number | null };
  } | null;
};

export type PaperVsBacktestResponse = { ok: boolean; report: PaperVsBacktestReport };

/**
 * C4 paper-vs-backtest report (2026-08-12): every closed S-3 paper trade
 * reconciled against the backtest engine's twin trade. Generated by
 * scripts/paper_vs_backtest_report.py; the verdict flags <20 samples as
 * "not yet conclusive".
 */
export function usePaperVsBacktestQuery(enabled = true) {
  return useQuery({
    queryKey: ['backtest', 'paper-vs-backtest'],
    queryFn: () => apiGetJson<PaperVsBacktestResponse>('/api/backtest/paper-vs-backtest'),
    staleTime: 10 * 60_000,
    enabled,
  });
}
