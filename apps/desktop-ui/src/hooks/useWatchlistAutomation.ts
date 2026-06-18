'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';

import { watchlistRiskQueryKey } from '@/lib/queries/dashboard';
import { watchlistMarketKey } from '@/lib/queries/watchlist';
import {
  applyAutomationRun,
  fetchAutomationPending,
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

  React.useEffect(() => {
    let cancelled = false;

    async function tick() {
      if (cancelled || applyingRef.current) return;
      if (!isAutomationPollWindow()) return;
      try {
        const pending = await fetchAutomationPending();
        if (!pending || pending.skipped) return;
        if (getAckedRunId() === pending.runId) return;
        applyingRef.current = true;
        await applyAutomationRun(pending, { silent: true });
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
    }

    void tick();
    const id = window.setInterval(() => void tick(), 60_000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [queryClient]);
}
