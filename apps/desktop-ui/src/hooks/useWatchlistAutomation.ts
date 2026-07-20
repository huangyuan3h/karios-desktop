'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';

import { useAutomationPendingQuery } from '@/lib/queries/automation';
import { watchlistRiskQueryKey } from '@/lib/queries/dashboard';
import { watchlistMarketKey } from '@/lib/queries/watchlist';
import {
  applyAutomationRun,
  isAutomationPollWindow,
} from '@/lib/watchlist-automation';
import { loadWatchlist } from '@/lib/watchlist-storage';

const ACK_STORAGE_KEY = 'karios.watchlist.automation.ackedRunId';

function getAckedRunId(): string | null {
  if (typeof window === 'undefined') return null;
  try {
    return window.localStorage.getItem(ACK_STORAGE_KEY);
  } catch {
    return null;
  }
}

function setAckedRunId(runId: string): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(ACK_STORAGE_KEY, runId);
  } catch {
    // ignore
  }
}

function watchlistSymbolsFromStorage(): string[] {
  const items = loadWatchlist();
  return (Array.isArray(items) ? items : [])
    .map((x) => String(x?.symbol ?? '').trim().toUpperCase())
    .filter(Boolean);
}

export function useWatchlistAutomation(): void {
  const queryClient = useQueryClient();
  const applyingRef = React.useRef(false);
  const { data: pending } = useAutomationPendingQuery();

  React.useEffect(() => {
    if (!isAutomationPollWindow()) return;
    if (!pending || pending.skipped) return;
    if (getAckedRunId() === pending.runId) return;
    if (applyingRef.current) return;

    let cancelled = false;
    applyingRef.current = true;
    void (async () => {
      try {
        await applyAutomationRun(pending, { silent: true });
        if (cancelled) return;
        setAckedRunId(pending.runId);
        const symbols = watchlistSymbolsFromStorage();
        if (symbols.length) {
          void queryClient.invalidateQueries({ queryKey: watchlistMarketKey(symbols) });
        }
        void queryClient.invalidateQueries({ queryKey: watchlistRiskQueryKey() });
      } catch {
        // silent scheduled apply
      } finally {
        applyingRef.current = false;
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [pending, queryClient]);
}
