'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';

import {
  refetchWatchlistMarket,
  useWatchlistMarketQuery,
} from '@/lib/queries/watchlist';
import type { WatchlistItem } from '@/lib/watchlist-storage';

export function useWatchlistTrend(
  symbols: string[],
  items: WatchlistItem[],
  persist: (next: WatchlistItem[]) => void,
) {
  const queryClient = useQueryClient();
  const [syncMsg, setSyncMsg] = React.useState<string | null>(null);
  const marketQuery = useWatchlistMarketQuery(symbols);
  const trend = marketQuery.data?.trend ?? {};
  const quotes = marketQuery.data?.quotes ?? {};
  const trendBusy = marketQuery.isFetching;
  const trendUpdatedAt = marketQuery.dataUpdatedAt
    ? new Date(marketQuery.dataUpdatedAt).toISOString()
    : null;

  React.useEffect(() => {
    if (!marketQuery.data) return;
    const nextQuotes = marketQuery.data.quotes;
    const next = marketQuery.data.trend;
    const nextItems = items.map((it) => {
      if (!(it.positionPct && it.positionPct > 0)) return it;
      if (!it.costPrice) return it;
      const q = nextQuotes[it.symbol];
      const price =
        typeof q?.price === 'number' && Number.isFinite(q.price)
          ? q.price
          : (() => {
              const close = next[it.symbol]?.values?.close;
              return typeof close === 'number' && Number.isFinite(close) ? close : null;
            })();
      if (price == null) return it;
      const maxPrice = typeof it.maxPrice === 'number' ? it.maxPrice : 0;
      if (price > maxPrice) return { ...it, maxPrice: price };
      if (!it.maxPrice) return { ...it, maxPrice: price };
      return it;
    });
    if (nextItems.some((x, i) => x.maxPrice !== items[i]?.maxPrice)) {
      persist(nextItems);
    }
  }, [marketQuery.data, items, persist]);

  // When the symbol set grows (new stocks added), force one market refresh so
  // new HK/ETF symbols pull K-lines from yfinance / tushare. We deliberately
  // only trigger when the set *expands* to avoid forcing on every render.
  const prevSymbolsRef = React.useRef<Set<string>>(new Set());
  React.useEffect(() => {
    const next = new Set(symbols);
    const prev = prevSymbolsRef.current;
    const grew = symbols.length > prev.size || [...next].some((s) => !prev.has(s));
    prevSymbolsRef.current = next;
    if (!grew || !symbols.length) return;
    if (marketQuery.data) {
      void refetchWatchlistMarket(queryClient, symbols, { forceMarket: true }).catch(() => {
        // best-effort; trend data will catch up on the next poll
      });
    }
  }, [symbols, marketQuery.data, queryClient]);

  async function onManualRefreshTrend() {
    if (!symbols.length) return;
    setSyncMsg('Syncing K-lines for new symbols (yfinance / tushare)…');
    try {
      const snapshot = await refetchWatchlistMarket(queryClient, symbols, { forceMarket: true });
      if (snapshot.barSync && snapshot.barSync.failures > 0) {
        setSyncMsg(
          `K-line sync failed for ${snapshot.barSync.failures}/${snapshot.barSync.total} symbols; data may be missing — check tushare rate limit or run POST /sync/hk-daily.`,
        );
      } else if (snapshot.barSync) {
        setSyncMsg(null);
      }
    } catch (e) {
      console.warn('Watchlist trendok load failed:', e);
      setSyncMsg('K-line sync failed; check server logs.');
    }
  }

  return {
    trend,
    quotes,
    trendBusy,
    trendUpdatedAt,
    syncMsg,
    setSyncMsg,
    onManualRefreshTrend,
    queryClient,
    marketQuery,
  };
}

export type UseWatchlistTrendReturn = ReturnType<typeof useWatchlistTrend>;
