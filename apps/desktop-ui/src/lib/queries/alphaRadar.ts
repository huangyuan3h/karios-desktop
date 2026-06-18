'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';

import { apiDeleteJson, apiGetJson, apiPostJson } from '@/lib/api/client';
import {
  DEFAULT_CATALYST_MAX_AGE_DAYS,
  type CatalystStock,
} from '@/lib/alpha-radar-catalyst';

import { SCREENER_STALE_MS } from './intervals';

const ALPHA_GET_OPTS = { timeoutMs: 60_000 } as const;
const ALPHA_POST_OPTS = { timeoutMs: 300_000 } as const;
const ALPHA_DELETE_OPTS = { timeoutMs: 60_000 } as const;

export type ViewTab = 'trends' | 'catalyst' | 'rss';
export type TrendsScope = 'batch' | 'all';
export type DriverFilter = 'all' | 'Global_Tech' | 'Domestic_Policy' | 'Cycle_Reversal';

export type CnSymbol = {
  symbol: string;
  name: string;
  confidence: number;
  rationale: string;
};

export type AlphaTrend = {
  id: string;
  documentId: string;
  trendName: string;
  macroTheme?: string | null;
  catalystGrade?: string | null;
  driverType?: string | null;
  eventFocus?: string | null;
  logicSummary?: string | null;
  catalyst: string | null;
  globalTarget: string | null;
  urgencyLevel: string;
  keywordsForMapping: string[];
  cnSymbols: CnSymbol[];
  mappingConfidence: number | null;
  riskStatus: string;
  createdAt: string;
  documentTitle?: string;
  documentUrl?: string;
  documentCategory?: string;
  documentPublishedAt?: string | null;
  documentFetchedAt?: string | null;
  documentSummary?: string | null;
};

export type PipelineStatus = {
  lastRunAt?: string | null;
  lastIngestAt?: string | null;
  lastProcessAt?: string | null;
  lastBatchStartedAt?: string | null;
  lastTrendCount?: number;
  currentTrendCount?: number;
  accumulatedTrendCount?: number;
  rawBacklogCount?: number;
  lastIngestStats?: {
    fetched?: number;
    filteredOut?: number;
    stored?: number;
    new?: number;
    requeued?: number;
    unchanged?: number;
  } | null;
  withinCooldown?: boolean;
  cooldownHours?: number;
};

export type RssDocument = {
  id: string;
  sourceId: string;
  title: string;
  url: string;
  category: string;
  summary: string | null;
  publishedAt: string | null;
  fetchedAt: string;
  processingStatus: string;
};

export type AlphaSource = {
  id: string;
  name: string;
};

export function alphaRadarQueryKey() {
  return ['alphaRadar'] as const;
}

export function alphaRadarStatusQueryKey() {
  return ['alphaRadar', 'status'] as const;
}

export function alphaRadarTrendsQueryKey(scope: TrendsScope) {
  return ['alphaRadar', 'trends', scope] as const;
}

export function alphaRadarCatalystQueryKey(maxAgeDays = DEFAULT_CATALYST_MAX_AGE_DAYS) {
  return ['alphaRadar', 'catalyst', maxAgeDays] as const;
}

export function alphaRadarRssQueryKey() {
  return ['alphaRadar', 'rss'] as const;
}

export async function fetchAlphaRadarStatus(): Promise<PipelineStatus> {
  return apiGetJson<{ ok?: boolean } & PipelineStatus>('/api/alpha-radar/status', ALPHA_GET_OPTS);
}

export async function fetchAlphaRadarTrends(scope: TrendsScope): Promise<{
  total: number;
  items: AlphaTrend[];
}> {
  const path =
    scope === 'batch'
      ? '/api/alpha-radar/trends?limit=50&latest_batch=true'
      : '/api/alpha-radar/trends?limit=100&latest_batch=false';
  return apiGetJson<{ total: number; items: AlphaTrend[] }>(path, ALPHA_GET_OPTS);
}

export async function fetchAlphaRadarCatalyst(
  maxAgeDays = DEFAULT_CATALYST_MAX_AGE_DAYS,
): Promise<{ total: number; maxAgeDays: number; items: CatalystStock[] }> {
  return apiGetJson<{ total: number; maxAgeDays: number; items: CatalystStock[] }>(
    `/api/alpha-radar/catalyst-stocks?limit=50&maxAgeDays=${maxAgeDays}`,
    ALPHA_GET_OPTS,
  );
}

export async function fetchAlphaRadarRss(): Promise<{
  documents: RssDocument[];
  total: number;
  sourceNames: Record<string, string>;
}> {
  const [docResp, srcResp] = await Promise.all([
    apiGetJson<{ total: number; items: RssDocument[] }>(
      '/api/alpha-radar/documents?limit=100',
      ALPHA_GET_OPTS,
    ),
    apiGetJson<{ sources: AlphaSource[] }>('/api/alpha-radar/sources', ALPHA_GET_OPTS),
  ]);
  const sourceNames: Record<string, string> = {};
  for (const s of srcResp.sources || []) {
    sourceNames[s.id] = s.name;
  }
  return {
    documents: docResp.items || [],
    total: docResp.total ?? docResp.items?.length ?? 0,
    sourceNames,
  };
}

export function alphaRadarStatusQueryOptions() {
  return {
    queryKey: alphaRadarStatusQueryKey(),
    queryFn: fetchAlphaRadarStatus,
    staleTime: SCREENER_STALE_MS,
  };
}

export function alphaRadarTrendsQueryOptions(scope: TrendsScope) {
  return {
    queryKey: alphaRadarTrendsQueryKey(scope),
    queryFn: () => fetchAlphaRadarTrends(scope),
    staleTime: SCREENER_STALE_MS,
  };
}

export function alphaRadarCatalystQueryOptions(
  maxAgeDays = DEFAULT_CATALYST_MAX_AGE_DAYS,
) {
  return {
    queryKey: alphaRadarCatalystQueryKey(maxAgeDays),
    queryFn: () => fetchAlphaRadarCatalyst(maxAgeDays),
    staleTime: SCREENER_STALE_MS,
  };
}

export function alphaRadarRssQueryOptions() {
  return {
    queryKey: alphaRadarRssQueryKey(),
    queryFn: fetchAlphaRadarRss,
    staleTime: SCREENER_STALE_MS,
  };
}

export function useAlphaRadarStatusQuery() {
  return useQuery(alphaRadarStatusQueryOptions());
}

export function useAlphaRadarTrendsQuery(scope: TrendsScope) {
  return useQuery(alphaRadarTrendsQueryOptions(scope));
}

export function useAlphaRadarCatalystQuery(
  maxAgeDays = DEFAULT_CATALYST_MAX_AGE_DAYS,
) {
  return useQuery(alphaRadarCatalystQueryOptions(maxAgeDays));
}

export function useAlphaRadarRssQuery(options?: { enabled?: boolean }) {
  return useQuery({
    ...alphaRadarRssQueryOptions(),
    enabled: options?.enabled ?? true,
  });
}

export async function invalidateAlphaRadarQueries(queryClient: QueryClient): Promise<void> {
  await queryClient.invalidateQueries({ queryKey: alphaRadarQueryKey() });
}

export async function runAlphaRadarPipeline(force = false): Promise<{
  skipped?: boolean;
  ok?: boolean;
  trendCount?: number;
  processedHeadlines?: number;
  ingestStats?: { fetched?: number; filteredOut?: number; stored?: number };
  keptPreviousTrends?: boolean;
  lastRunAt?: string;
  errors?: Array<{ error?: string }>;
}> {
  return apiPostJson('/api/alpha-radar/run-pipeline', { force }, ALPHA_POST_OPTS);
}

export async function deleteAlphaRadarTrend(trendId: string): Promise<{ ok?: boolean; error?: string }> {
  return apiDeleteJson(
    `/api/alpha-radar/trends/${encodeURIComponent(trendId)}`,
    ALPHA_DELETE_OPTS,
  );
}

export async function remapAlphaRadarTrend(trendId: string): Promise<{
  ok?: boolean;
  cnSymbols?: CnSymbol[];
  error?: string;
}> {
  return apiPostJson(
    `/api/alpha-radar/trends/${encodeURIComponent(trendId)}/remap`,
    undefined,
    ALPHA_POST_OPTS,
  );
}
