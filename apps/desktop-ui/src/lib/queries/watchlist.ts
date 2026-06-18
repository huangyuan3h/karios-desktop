'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';

import { isShanghaiQuoteWindow } from '@/lib/market-hours';
import {
  fetchWatchlistMarketSnapshot,
  type WatchlistMarketSnapshot,
} from '@/lib/watchlist-market';

import { WATCHLIST_POLL_MS } from './intervals';

export function watchlistMarketKey(symbols: string[]) {
  const sorted = [...symbols]
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean)
    .sort();
  return ['watchlist', 'market', sorted.join(',')] as const;
}

export function watchlistMarketQueryOptions(symbols: string[]) {
  return {
    queryKey: watchlistMarketKey(symbols),
    queryFn: () =>
      fetchWatchlistMarketSnapshot(symbols, {
        forceMarket: false,
        realtime: isShanghaiQuoteWindow(),
      }),
  };
}

export function useWatchlistMarketQuery(symbols: string[]) {
  const options = watchlistMarketQueryOptions(symbols);
  return useQuery({
    ...options,
    enabled: symbols.length > 0,
    refetchInterval: symbols.length > 0 ? WATCHLIST_POLL_MS : false,
    refetchIntervalInBackground: false,
  });
}

export async function refetchWatchlistMarket(
  queryClient: QueryClient,
  symbols: string[],
  options: { forceMarket?: boolean } = {},
): Promise<WatchlistMarketSnapshot> {
  const key = watchlistMarketKey(symbols);
  const forceMarket = Boolean(options.forceMarket);
  const snapshot = await queryClient.fetchQuery({
    queryKey: key,
    queryFn: () =>
      fetchWatchlistMarketSnapshot(symbols, {
        forceMarket,
        realtime: isShanghaiQuoteWindow(),
      }),
  });
  queryClient.setQueryData(key, snapshot);
  return snapshot;
}
