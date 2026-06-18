'use client';

import { useQuery } from '@tanstack/react-query';

import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

import { MACRO_POLL_MS } from './intervals';

export type CnIndexSignal = {
  tsCode?: string;
  name?: string;
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
};

export type MacroSnapshot = {
  cnIndexSignals?: CnIndexSignal[];
  macro?: MacroItem[];
  warning?: string;
};

const FETCH_TIMEOUT_MS = 30_000;

export function macroSnapshotQueryKey() {
  return ['macro', 'snapshot'] as const;
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

export function useMacroSnapshotQuery() {
  return useQuery({
    queryKey: macroSnapshotQueryKey(),
    queryFn: fetchMacroSnapshot,
    refetchInterval: MACRO_POLL_MS,
    refetchIntervalInBackground: false,
  });
}
