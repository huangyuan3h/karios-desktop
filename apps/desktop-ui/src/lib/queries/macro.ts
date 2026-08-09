'use client';

import { useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';

import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

import { dashboardSummaryQueryKey, type DashboardSummary } from './dashboard';
import { MACRO_POLL_MS } from './intervals';

export type CnIndexSignal = {
  tsCode?: string;
  name?: string;
  featured?: boolean;
  signal?: string;
  positionRange?: string;
  close?: number | null;
  pctChg?: number | null;
  ma5?: number | null;
  ma20?: number | null;
  realtime?: boolean;
  tradeTime?: string | null;
  source?: string | null;
};

export type MacroItem = {
  seriesId?: string;
  name?: string;
  category?: string;
  why?: string;
  asOfDate?: string | null;
  close?: number | null;
  pctChg?: number | null;
  ma5?: number | null;
  ma20?: number | null;
  source?: string | null;
  underlyingTsCode?: string | null;
  realtime?: boolean;
  tradeTime?: string | null;
  quotePrice?: number | null;
  quotePctChg?: number | null;
  signal?: string;
  signalLabel?: string;
  unit?: string;
  warning?: string | null;
};

export type EtfFlowSignal = {
  asOfDate?: string;
  verdict?: 'confirm' | 'neutral' | 'contradict' | string;
  broadDirection?: 'buy' | 'outflow' | 'mixed' | 'neutral' | string;
  sectorDirection?: 'buy' | 'outflow' | 'mixed' | 'neutral' | string;
  confirmCount?: number;
  contradictCount?: number;
  intradaySafe?: boolean;
  shareLag?: boolean;
  incomplete?: boolean;
};

export type MacroSnapshot = {
  cnIndexSignals?: CnIndexSignal[];
  macro?: MacroItem[];
  etfFundFlow?: unknown;
  etfFlowSignal?: EtfFlowSignal;
  warning?: string;
};

const FETCH_TIMEOUT_MS = 30_000;

export function macroSnapshotQueryKey() {
  return ['macro', 'snapshot'] as const;
}

function macroFromDashboardSummary(summary: DashboardSummary | undefined): MacroSnapshot | null {
  const snap = summary?.macroSnapshot;
  if (!snap || typeof snap !== 'object') return null;
  return snap as MacroSnapshot;
}

export async function fetchMacroSnapshot(): Promise<MacroSnapshot> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(`${DATA_SYNC_BASE_URL}/macro/snapshot`, {
      cache: 'no-store',
      signal: ctrl.signal,
    });
    const txt = await res.text().catch(() => '');
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
    return (txt ? (JSON.parse(txt) as MacroSnapshot) : {}) as MacroSnapshot;
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') {
      throw new Error(`Request timed out after ${FETCH_TIMEOUT_MS / 1000}s (check data-sync-service)`);
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

export async function fetchMacroSnapshotCached(
  queryClient: QueryClient,
): Promise<MacroSnapshot> {
  const candidates = [
    queryClient.getQueryData<DashboardSummary>(dashboardSummaryQueryKey(true)),
    queryClient.getQueryData<DashboardSummary>(dashboardSummaryQueryKey(false)),
  ];
  for (const cached of candidates) {
    const fromSummary = macroFromDashboardSummary(cached);
    if (fromSummary) return fromSummary;
  }
  return fetchMacroSnapshot();
}

export function useMacroSnapshotQuery() {
  const queryClient = useQueryClient();
  return useQuery({
    queryKey: macroSnapshotQueryKey(),
    queryFn: () => fetchMacroSnapshotCached(queryClient),
    refetchInterval: MACRO_POLL_MS,
    refetchIntervalInBackground: false,
  });
}

export type MarketRegimeResponse = {
  ok: boolean;
  regime: string;
  asOfDate: string | null;
};

export function useMarketRegimeQuery(enabled = true) {
  return useQuery({
    queryKey: ['market', 'regime'],
    queryFn: async () => {
      const res = await fetch('/market/regime', { cache: 'no-store' });
      if (!res.ok) throw new Error(`regime ${res.status}`);
      return (await res.json()) as MarketRegimeResponse;
    },
    enabled,
    staleTime: 5 * 60 * 1000,
    refetchInterval: enabled ? 5 * 60 * 1000 : false,
  });
}
