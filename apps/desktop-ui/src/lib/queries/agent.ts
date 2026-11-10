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
    queryFn: () => apiGetJson<DecisionContract>(`/v1/agent/decision-contract${q}`, {
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
