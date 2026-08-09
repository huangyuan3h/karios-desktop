import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

export interface SyncJobFailure {
  jobType: string;
  syncedAt: string | null;
  lastTsCode?: string | null;
  errorMessage?: string | null;
  failures24h?: number;
}

export interface JobFailuresResponse {
  ok: boolean;
  hours: number;
  count: number;
  failures: SyncJobFailure[];
}

/** Recent sync-job failures (GET /api/health/job-failures). */
export async function fetchJobFailures(
  baseUrl: string = DATA_SYNC_BASE_URL,
  hours = 48,
  signal?: AbortSignal,
): Promise<JobFailuresResponse | null> {
  try {
    const res = await fetch(`${baseUrl}/api/health/job-failures?hours=${hours}`, {
      cache: 'no-store',
      signal: signal ?? AbortSignal.timeout(15_000),
    });
    if (!res.ok) return null;
    return (await res.json()) as JobFailuresResponse;
  } catch {
    return null;
  }
}
