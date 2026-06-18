'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';
import {
  TvScreenerListResponseSchema,
  TvSnapshotDetailSchema,
  TvSnapshotListResponseSchema,
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

export async function fetchLatestSnapshotDetail(
  screenerId: string,
): Promise<TvSnapshotDetail | null> {
  const list = TvSnapshotListResponseSchema.parse(
    await apiGetJson<unknown>(
      `/integrations/tradingview/screeners/${encodeURIComponent(screenerId)}/snapshots?limit=1`,
    ),
  );
  const latest = list.items[0];
  if (!latest) return null;
  return TvSnapshotDetailSchema.parse(
    await apiGetJson<unknown>(
      `/integrations/tradingview/snapshots/${encodeURIComponent(latest.id)}`,
    ),
  );
}

export async function fetchScreenerSnapshotsMap(
  screenerIds: string[],
): Promise<Record<string, TvSnapshotDetail | null>> {
  const entries = await Promise.all(
    screenerIds.map(async (id) => [id, await fetchLatestSnapshotDetail(id)] as const),
  );
  return Object.fromEntries(entries);
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
