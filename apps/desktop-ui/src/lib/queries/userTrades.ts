'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';

import { apiDeleteJson, apiGetJson, apiPostJson } from '@/lib/api/client';
import type {
  UserTrade,
  UserTradeRequest,
  UserTradesStats,
} from '@karios/shared';

export function userTradesListQueryKey(limit = 50) {
  return ['userTrades', 'list', limit] as const;
}

export function userTradesStatsQueryKey() {
  return ['userTrades', 'stats'] as const;
}

export async function fetchUserTrades(limit = 50): Promise<UserTrade[]> {
  const res = await apiGetJson<{ ok: boolean; trades: UserTrade[] }>(
    `/trades?limit=${limit}`,
  );
  return res.trades ?? [];
}

export async function fetchUserTradesStats(): Promise<UserTradesStats> {
  const res = await apiGetJson<{ ok: boolean; stats: UserTradesStats }>(
    '/trades/stats',
  );
  return res.stats;
}

export function userTradesListQueryOptions(limit = 50) {
  return {
    queryKey: userTradesListQueryKey(limit),
    queryFn: () => fetchUserTrades(limit),
    staleTime: 15_000,
  };
}

export function userTradesStatsQueryOptions() {
  return {
    queryKey: userTradesStatsQueryKey(),
    queryFn: fetchUserTradesStats,
    staleTime: 15_000,
  };
}

export function useUserTradesListQuery(limit = 50) {
  return useQuery(userTradesListQueryOptions(limit));
}

export function useUserTradesStatsQuery() {
  return useQuery(userTradesStatsQueryOptions());
}

export async function recordUserTrade(
  req: UserTradeRequest,
): Promise<UserTrade> {
  const res = await apiPostJson<{ ok: boolean; trade: UserTrade }>('/trades', req);
  return res.trade;
}

export async function deleteUserTrade(tradeId: string): Promise<void> {
  await apiDeleteJson<{ ok: boolean }>(`/trades/${tradeId}`);
}

export async function invalidateUserTradesQueries(
  queryClient: QueryClient,
): Promise<void> {
  await queryClient.invalidateQueries({ queryKey: ['userTrades'] });
}
