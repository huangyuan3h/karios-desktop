'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';
import {
  TvScreenerListResponseSchema,
  TvSnapshotDetailSchema,
  type TvScreener,
  type TvSnapshotDetail,
} from '@karios/shared';

import { apiGetJson } from '@/lib/api/client';

import { SCREENER_STALE_MS } from './intervals';

export function screenerListQueryKey() {
  return ['screener', 'list', 'enabled'] as const;
}

export function screenerSnapshotsQueryKey(screenerIds: string[]) {
  const sorted = [...screenerIds]
    .map((id) => id.trim())
    .filter(Boolean)
    .sort();
  return ['screener', 'snapshots', sorted.join(',')] as const;
}

export async function fetchEnabledScreeners(): Promise<TvScreener[]> {
  const resp = TvScreenerListResponseSchema.parse(
    await apiGetJson<unknown>('/integrations/tradingview/screeners'),
  );
  return resp.items.filter((item) => item.enabled);
}

export async function fetchScreenerSnapshotsMap(
  screenerIds: string[],
): Promise<Record<string, TvSnapshotDetail | null>> {
  const ids = [...screenerIds].map((id) => id.trim()).filter(Boolean);
  if (!ids.length) return {};

  const params = new URLSearchParams();
  for (const id of ids) params.append('ids', id);
  const resp = await apiGetJson<{ items?: Record<string, unknown> }>(
    `/integrations/tradingview/screeners/snapshots/latest?${params.toString()}`,
  );
  const items = resp.items ?? {};
  const out: Record<string, TvSnapshotDetail | null> = {};
  for (const id of ids) {
    const raw = items[id];
    out[id] = raw ? TvSnapshotDetailSchema.parse(raw) : null;
  }
  return out;
}

export function screenerListQueryOptions() {
  return {
    queryKey: screenerListQueryKey(),
    queryFn: fetchEnabledScreeners,
    staleTime: SCREENER_STALE_MS,
  };
}

export function screenerSnapshotsQueryOptions(screenerIds: string[]) {
  return {
    queryKey: screenerSnapshotsQueryKey(screenerIds),
    queryFn: () => fetchScreenerSnapshotsMap(screenerIds),
    staleTime: SCREENER_STALE_MS,
  };
}

export function useScreenerListQuery() {
  return useQuery(screenerListQueryOptions());
}

export function useScreenerSnapshotsQuery(screenerIds: string[]) {
  return useQuery({
    ...screenerSnapshotsQueryOptions(screenerIds),
    enabled: screenerIds.length > 0,
  });
}

export async function invalidateScreenerQueries(queryClient: QueryClient): Promise<void> {
  await queryClient.invalidateQueries({ queryKey: ['screener'] });
}
