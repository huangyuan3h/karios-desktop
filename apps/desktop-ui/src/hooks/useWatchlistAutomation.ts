'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';

import { useAutomationLatestQuery } from '@/lib/queries/automation';
import { watchlistRiskQueryKey } from '@/lib/queries/dashboard';
import { watchlistMarketKey } from '@/lib/queries/watchlist';
import { hydrateWatchlist, loadWatchlist } from '@/lib/watchlist-storage';

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
    .map((x) =>
      String(x?.symbol ?? '')
        .trim()
        .toUpperCase(),
    )
    .filter(Boolean);
}

/**
 * OPT-209: the backend applies each pool run to the registry itself (S-3 +
 * 星舰 add / invalidation GC). The client only re-hydrates the registry and
 * refreshes market data when a new applied run appears — no client-side apply,
 * no ack step.
 */
export function useWatchlistAutomation(): void {
  const queryClient = useQueryClient();
  const refreshingRef = React.useRef(false);
  const { data: latest } = useAutomationLatestQuery();

  React.useEffect(() => {
    if (!latest?.runId || latest.skipped) return;
    if (getAckedRunId() === latest.runId) return;
    if (refreshingRef.current) return;

    refreshingRef.current = true;
    void (async () => {
      try {
        setAckedRunId(latest.runId);
        await hydrateWatchlist();
        const symbols = watchlistSymbolsFromStorage();
        if (symbols.length) {
          void queryClient.invalidateQueries({ queryKey: watchlistMarketKey(symbols) });
        }
        void queryClient.invalidateQueries({ queryKey: watchlistRiskQueryKey() });
      } catch {
        // silent background refresh
      } finally {
        refreshingRef.current = false;
      }
    })();
  }, [latest, queryClient]);
}
