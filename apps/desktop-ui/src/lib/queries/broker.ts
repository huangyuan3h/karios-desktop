'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';

import { SCREENER_STALE_MS } from './intervals';

export type BrokerAccountState = {
  accountId: string;
  broker: string;
  updatedAt: string;
  overview: Record<string, unknown>;
  positions: Array<Record<string, unknown>>;
  conditionalOrders: Array<Record<string, unknown>>;
  trades: Array<Record<string, unknown>>;
  counts: Record<string, number>;
};

export type BrokerAccount = {
  id: string;
  broker: string;
  title: string;
  accountMasked: string | null;
  updatedAt: string;
};

export function brokerAccountsQueryKey(broker = 'pingan') {
  return ['broker', 'accounts', broker] as const;
}

export function brokerAccountStateQueryKey(broker: string, accountId: string) {
  return ['broker', 'state', broker, accountId] as const;
}

export async function fetchBrokerAccounts(broker = 'pingan'): Promise<BrokerAccount[]> {
  return apiGetJson<BrokerAccount[]>(`/broker/accounts?broker=${encodeURIComponent(broker)}`);
}

export async function fetchBrokerAccountState(
  broker: string,
  accountId: string,
): Promise<BrokerAccountState> {
  return apiGetJson<BrokerAccountState>(
    `/broker/${encodeURIComponent(broker)}/accounts/${encodeURIComponent(accountId)}/state`,
  );
}

export function brokerAccountsQueryOptions(broker = 'pingan') {
  return {
    queryKey: brokerAccountsQueryKey(broker),
    queryFn: () => fetchBrokerAccounts(broker),
    staleTime: SCREENER_STALE_MS,
  };
}

export function brokerAccountStateQueryOptions(broker: string, accountId: string) {
  return {
    queryKey: brokerAccountStateQueryKey(broker, accountId),
    queryFn: () => fetchBrokerAccountState(broker, accountId),
    staleTime: SCREENER_STALE_MS,
    enabled: Boolean(accountId?.trim()),
  };
}

export function useBrokerAccountsQuery(broker = 'pingan') {
  return useQuery(brokerAccountsQueryOptions(broker));
}

export function useBrokerAccountStateQuery(broker: string, accountId: string) {
  return useQuery(brokerAccountStateQueryOptions(broker, accountId));
}

export async function invalidateBrokerQueries(queryClient: QueryClient): Promise<void> {
  await queryClient.invalidateQueries({ queryKey: ['broker'] });
}
