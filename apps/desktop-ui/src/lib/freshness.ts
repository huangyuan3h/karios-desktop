import { apiGetJson } from '@/lib/api/client';

/** Per-source data freshness from /api/health/datasources (TIP-013). */
export type DataSourceFreshness = {
  source: string;
  label: string;
  lastSyncedAt: string | null;
  ageMinutes: number | null;
  thresholdMinutes: number;
  stale: boolean;
};

export type DataSourcesHealth = {
  ok: boolean;
  generatedAt: string;
  sources: DataSourceFreshness[];
};

export async function fetchDataSourcesHealth(): Promise<DataSourcesHealth> {
  return apiGetJson<DataSourcesHealth>('/api/health/datasources');
}

function formatAge(ageMinutes: number | null): string {
  if (ageMinutes == null) return 'unknown';
  if (ageMinutes < 60) return `${ageMinutes}m`;
  return `${(ageMinutes / 60).toFixed(1)}h`;
}

/**
 * Markdown block listing per-source freshness, with stale sources flagged.
 * Used in the Copy All header so the downstream agent can judge recency
 * itself instead of trusting a silent snapshot.
 */
export function buildDataFreshnessMarkdown(sources: DataSourceFreshness[]): string {
  const lines = ['## Data freshness', '', '- note: per-source last successful sync (agent: treat stale sources with caution)'];
  if (!sources.length) {
    lines.push('- unavailable');
    return lines.join('\n');
  }
  for (const s of sources) {
    const when = s.lastSyncedAt ?? 'never';
    lines.push(
      `- ${s.label}: ${when} (${formatAge(s.ageMinutes)} ago)${s.stale ? ' — ⚠ STALE' : ''}`,
    );
  }
  const staleCount = sources.filter((s) => s.stale).length;
  if (staleCount > 0) {
    lines.unshift(`⚠ WARNING: ${staleCount} data source(s) stale at copy time.`);
  }
  return lines.join('\n');
}
