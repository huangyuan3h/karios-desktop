import { AI_BASE_URL, DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import type { JobFailuresResponse, SyncJobFailure } from '@/lib/queries/syncFailures';

export interface DataSourceStatus {
  source: string;
  label: string;
  lastSyncedAt: string | null;
  ageMinutes: number | null;
  thresholdMinutes: number;
  stale: boolean;
}

export interface DatasourcesResponse {
  ok: boolean;
  generatedAt: string;
  sources: DataSourceStatus[];
}

export interface SystemHealthReport {
  dataSyncOnline: boolean;
  aiOnline: boolean;
  datasources: DataSourceStatus[];
  failures: SyncJobFailure[];
  errorCount: number;
  warnCount: number;
}

async function getJson<T>(url: string): Promise<T | null> {
  try {
    const res = await fetch(url, { cache: 'no-store', signal: AbortSignal.timeout(10_000) });
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

/** Probe both services + data freshness + job failures (fail-open per probe). */
export async function fetchSystemHealth(
  dataSyncBase: string = DATA_SYNC_BASE_URL,
  aiBase: string = AI_BASE_URL,
): Promise<SystemHealthReport> {
  const [ds, ai, failures] = await Promise.all([
    getJson<DatasourcesResponse>(`${dataSyncBase}/api/health/datasources`),
    getJson<{ ok: boolean }>(`${aiBase}/healthz`),
    getJson<JobFailuresResponse>(`${dataSyncBase}/api/health/job-failures?hours=48`),
  ]);
  const report: SystemHealthReport = {
    dataSyncOnline: ds !== null,
    aiOnline: ai?.ok === true,
    datasources: ds?.sources ?? [],
    failures: failures?.failures ?? [],
    errorCount: 0,
    warnCount: 0,
  };
  if (!report.dataSyncOnline) report.errorCount += 1;
  if (!report.aiOnline) report.errorCount += 1;
  report.warnCount += report.datasources.filter((s) => s.stale).length;
  report.warnCount += report.failures.length;
  return report;
}
