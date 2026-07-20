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

  async function onManualRefreshTrend() {
    if (!symbols.length) return;
    setSyncMsg(null);
    try {
      const snapshot = await refetchWatchlistMarket(queryClient, symbols, { forceMarket: true });
      if (snapshot.barSync && snapshot.barSync.failures > 0) {
        setSyncMsg(
          `Network sync failed for ${snapshot.barSync.failures}/${snapshot.barSync.total} symbols; using cached data.`,
        );
      }
    } catch (e) {
      console.warn('Watchlist trendok load failed:', e);
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
