'use client';

import { useQuery } from '@tanstack/react-query';

import { apiGetJson, apiPostJson } from '@/lib/api/client';

/** One agent overlay (P0: `hold_same_key_rebuy`). */
export type AgentOverlay = {
  id: string;
  type: 'hold_same_key_rebuy' | string;
  pickKey: string | null;
  symbol?: string | null;
  status: 'proposed' | 'accepted' | 'rejected' | 'cleared' | string;
  /** Status after applying `validUntil` (accepted + past date → expired). */
  effectiveStatus: 'proposed' | 'accepted' | 'rejected' | 'cleared' | 'expired' | string;
  validFrom?: string | null;
  validUntil?: string | null;
  note?: string | null;
  source?: string | null;
  createdAt?: string | null;
  updatedAt?: string | null;
};

export type AgentOverlayGroups = {
  active: AgentOverlay[];
  proposed: AgentOverlay[];
  expired: AgentOverlay[];
  all: AgentOverlay[];
};

/** Server-side same-key rebuy whip state (portfolio-health + contract). */
export type AgentWhipState = {
  sameKeyRebuy?: boolean;
  pickKey?: string | null;
  annotation?: string | null;
  /** Annotation when no accepted overlay; `建议暂缓 · 需确认` when one is active. */
  copy?: string | null;
  lastExit?: {
    key?: string | null;
    date?: string | null;
    kind?: string | null;
    symbol?: string | null;
  } | null;
  overlay?: AgentOverlay | null;
  /** Keys stopped out within the memory window (canonical symbols). */
  keys?: string[];
  memoryDays?: number;
};

export type DecisionContract = {
  ok: boolean;
  contractId: string;
  asOfDate: string;
  generatedAt: string;
  strategy: {
    mode: string;
    timelineStrategy: string;
    label: string;
    timelineLabel: string;
    mappingNote: string;
    sleeveRelevantToMode: boolean;
  };
  sleeve: {
    action: string;
    label?: string | null;
    message?: string | null;
    pickKey?: string | null;
    pick?: Record<string, unknown> | null;
    etfPick?: Record<string, unknown> | null;
    trailExit: boolean;
    pendingOpenSell: boolean;
    sellToRepo: boolean;
    symbol?: string | null;
    signalDate?: string | null;
    executeHint?: string | null;
    intradayWarn?: Record<string, unknown> | null;
    holding: boolean;
    hystBlocked: boolean;
    idlePct?: number | null;
    parkPct?: number | null;
  };
  health: {
    tradeDate?: string | null;
    regime?: string | null;
    sentiment?: string | null;
    panicActive: boolean;
    circuitBlocked: boolean;
    gateOpen: boolean;
    holdingsCount: number;
    cnHoldingsCount?: number;
    hkHoldingsCount?: number;
    parkingHoldingsCount?: number;
    exits: string[];
    candidateCount: number;
    scoreFresh?: boolean | null;
  };
  nextLeg: {
    key?: string | null;
    symbol?: string | null;
    name?: string | null;
    mom60?: number | null;
    close?: number | null;
    ma200?: number | null;
    aboveMa200?: boolean | null;
    reason?: string;
    candidates?: Array<{
      key: string;
      mom60?: number | null;
      aboveMa200: boolean;
      eligible: boolean;
    }>;
  };
  whip: AgentWhipState;
  overlays: AgentOverlayGroups;
  authority: string[];
};

export type OverlayMutationBody = {
  type?: 'hold_same_key_rebuy';
  pickKey: string;
  symbol?: string | null;
  status?: 'proposed' | 'accepted' | 'cleared';
  validUntil?: string | null;
  validDays?: number;
  note?: string | null;
  source?: string;
};

export type OverlayMutationResponse = {
  ok: boolean;
  overlay: AgentOverlay;
  overlays: AgentOverlayGroups;
};

export type DecisionLogBody = {
  tradeDate?: string | null;
  judgment: string;
  contractSnapshotId?: string | null;
  adoptedOverlay?: boolean | null;
  overlayId?: string | null;
  symbol?: string | null;
  source?: string;
  confidence?: number | null;
  meta?: Record<string, unknown> | null;
};

/** One-shot contract for the agent (curl / 乐乐). */
export function useDecisionContractQuery(tradeDate?: string, enabled = true) {
  const q = tradeDate ? `?tradeDate=${encodeURIComponent(tradeDate)}` : '';
  return useQuery({
    queryKey: ['agent', 'decision-contract', tradeDate ?? 'today'],
    queryFn: () =>
      apiGetJson<DecisionContract>(`/v1/agent/decision-contract${q}`, {
        timeoutMs: 120_000,
      }),
    staleTime: 5 * 60_000,
    enabled,
  });
}

export function useAgentOverlaysQuery(day?: string, enabled = true) {
  const q = day ? `?day=${encodeURIComponent(day)}` : '';
  return useQuery({
    queryKey: ['agent', 'overlays', day ?? 'today'],
    queryFn: () =>
      apiGetJson<{ ok: boolean; asOfDate: string; overlays: AgentOverlayGroups }>(
        `/v1/agent/overlays${q}`,
      ),
    staleTime: 60_000,
    enabled,
  });
}

/** Propose / confirm / clear `hold_same_key_rebuy` (annotation only). */
export async function mutateAgentOverlay(
  body: OverlayMutationBody,
): Promise<OverlayMutationResponse> {
  return apiPostJson<OverlayMutationResponse>('/v1/agent/overlays', {
    type: 'hold_same_key_rebuy',
    ...body,
  });
}

/** Write one minimal decision-log row (TIP-015 decision_actions). */
export async function writeAgentDecisionLog(
  body: DecisionLogBody,
): Promise<{ ok: boolean; item: Record<string, unknown> }> {
  return apiPostJson('/v1/agent/decision-log', body);
}

/* ------------------------------------------------------------------------- */
/* Factor system (advice only) — GET /v1/agent/factor-system                  */
/* ------------------------------------------------------------------------- */

export type FactorSystemStatus = 'cold' | 'watch' | 'revived' | string;
export type FactorSystemRole = 'reference' | 'satellite' | 'research' | string;
export type FactorSystemAction = 'wait_in_baseline' | 'advise_follow' | 'advise_exit' | string;

export type FactorSystemFreshness = {
  sourceGeneratedAt?: string | null;
  historyLatest?: string | null;
  tradeBlotterLastEntry?: string | null;
  /** As-of watermark of the live overlay (latest complete replay session). */
  liveAsOf?: string | null;
  /** `ok` | `degraded` (missing 14:30 bars / ETF not synced). */
  liveStatus?: string | null;
  liveUpdatedAt?: string | null;
  expectedSession?: string | null;
  ageDays?: number | null;
  stale?: boolean;
  reason?: string | null;
};

export type FactorSystemSummary = {
  nFactors: number;
  nCold: number;
  nWatch: number;
  nRevived: number;
};

/** One documented live gate (paper-20 / holdout / filtered valid). */
export type FactorSystemGate = {
  required?: number;
  completed?: number;
  pass?: boolean;
  asOfDate?: string | null;
  source?: string | null;
  stale?: boolean;
  currentPct?: number | null;
  recoverToPct?: number | null;
  thresholdPct?: number | null;
};

export type FactorSystemPreconditions = {
  paper20?: FactorSystemGate;
  holdoutRecovery?: FactorSystemGate;
  filteredValidPositive?: FactorSystemGate;
  allPass?: boolean;
};

export type FactorSystemDecay = {
  id: string;
  name?: string | null;
  from?: string | number | null;
  to?: string | number | null;
  fromPosition?: number | null;
  toPosition?: number | null;
  reason?: string | null;
};

export type FactorSystemRevived = {
  id: string;
  name?: string | null;
  status?: FactorSystemStatus;
  position?: number | null;
  percentile?: number | null;
};

export type FactorSystemPolicy = {
  action: FactorSystemAction;
  baseline: string;
  baselineLabel: string;
  /** Rule weight (0/10/20), independent of the live gates. */
  suggestedWeight: number;
  /** Live weight; stays 0 while any live gate is unmet. */
  suggestedLiveWeight: number;
  maxLiveWeight: number;
  liveBlocked: boolean;
  liveBlockedReason?: string | null;
  reason?: string | null;
  decayedFactors: FactorSystemDecay[];
  revivedFactors: FactorSystemRevived[];
  preconditions: FactorSystemPreconditions;
};

export type FactorSystemFocusItem = {
  id: string;
  name: string;
  family?: string | null;
  status: FactorSystemStatus;
  badge?: string | null;
  role: FactorSystemRole;
  position: number;
  percentile?: number | null;
  /** True only for a satellite factor that is revived; never an order. */
  actionable: boolean;
  note?: string | null;
};

/** Atomic brick / combination sleeve / reference light. */
export type FactorBrickKind = 'brick' | 'sleeve' | 'reference' | string;

/** One frozen window slice (no invented percentiles). */
export type FactorBrickWindow = {
  date?: string | null;
  net?: number | null;
  percentile?: number | null;
  winRate?: number | null;
  trades?: number | null;
  revived?: boolean | null;
};

/** Most recent rolling window: live overlay when present, else frozen holdout. */
export type FactorBrickRecentPerformance = {
  window: 'holdout' | 'live' | 'long' | 'valid' | string;
  asOf?: string | null;
  /** Last closed trade inside the window (the as-of may be the newer replay watermark). */
  lastTradeAsOf?: string | null;
  net?: number | null;
  percentile?: number | null;
  winRate?: number | null;
  trades?: number | null;
  unit?: string | null;
  missing?: boolean;
  /** `live` = frozen audit prefix + appended closed replay trades. */
  source?: 'live' | 'frozen' | string;
  basis?: string | null;
};

export type FactorBrickWindows = {
  OOS2?: FactorBrickWindow | null;
  train?: FactorBrickWindow | null;
  valid?: FactorBrickWindow | null;
  holdout?: FactorBrickWindow | null;
};

export type FactorBrick = {
  id: string;
  name: string;
  family?: string | null;
  source?: string | null;
  /** Display unit next to net (`点` / `%/笔` / `%`). */
  unit?: string | null;
  kind: FactorBrickKind;
  role: FactorSystemRole;
  status: FactorSystemStatus;
  badge?: string | null;
  position: number;
  confirmations: number;
  recentPerformance: FactorBrickRecentPerformance;
  windows: FactorBrickWindows;
  spark?: number[] | null;
  /** True only when a frozen window history entry says `revived: true`. */
  onceRevived: boolean;
  onceRevivedWindows: string[];
  note?: string | null;
};

export type FactorSystemTransition = {
  date?: string | null;
  kind?: string | null;
  eventType?: string | null;
  factorId?: string | null;
  name?: string | null;
  from?: string | null;
  to?: string | null;
};

export type FactorSystemPayload = {
  available: boolean;
  error?: string | null;
  generatedAt?: string | null;
  adviceOnly?: boolean;
  freshness: FactorSystemFreshness;
  summary: FactorSystemSummary;
  policy: FactorSystemPolicy;
  focus: FactorSystemFocusItem[];
  /** Full vault brick list (omitted from the compact judgment-brief payload). */
  bricks?: FactorBrick[];
  recentTransitions: FactorSystemTransition[];
};

/** Advice-only factor-system policy (read-only; never places orders). */
export function useFactorSystemQuery(enabled = true) {
  return useQuery({
    queryKey: ['agent', 'factor-system'],
    queryFn: () => apiGetJson<FactorSystemPayload>('/v1/agent/factor-system'),
    staleTime: 5 * 60_000,
    enabled,
  });
}
